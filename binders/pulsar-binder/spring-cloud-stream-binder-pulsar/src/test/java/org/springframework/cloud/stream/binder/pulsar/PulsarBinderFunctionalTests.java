/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.pulsar;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.support.json.JacksonJsonUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Lari Hotari
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
	properties = {
		"spring.cloud.stream.bindings.eventConsumerBatchProcessingWithHeaders-in-0.destination=" + PulsarBinderFunctionalTests.PULSAR_TOPIC,
		"spring.cloud.stream.pulsar.bindings.eventConsumerBatchProcessingWithHeaders-in-0.consumer.idleBetweenPolls = 1",
		"spring.cloud.stream.pulsar.bindings.eventConsumerBatchProcessingWithHeaders-in-0.consumer.listenerMode = batch",
		"spring.cloud.stream.pulsar.bindings.eventConsumerBatchProcessingWithHeaders-in-0.consumer.checkpointMode = manual",
		"spring.cloud.stream.pulsar.binder.headers = event.eventType",
		"spring.cloud.stream.pulsar.binder.autoAddShards = true" })
@DirtiesContext
public class PulsarBinderFunctionalTests implements PulsarContainerTest {

	static final String PULSAR_TOPIC = "test_topic";

	private static PulsarClient PULSAR_CLIENT;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private CountDownLatch messageBarrier;

	@Autowired
	private AtomicReference<Message<List<?>>> messageHolder;

	@BeforeAll
	static void setup() throws PulsarClientException {
		PULSAR_CLIENT = PulsarContainerTest.pulsarClient();
	}

	@Test
	void testPulsarBinder() throws JsonProcessingException, InterruptedException, PulsarClientException {

		try(Producer<byte[]> producer = PULSAR_CLIENT.newProducer().topic(PULSAR_TOPIC).create()) {
			for (int i = 0; i < 10; i++) {
				Message<String> eventMessages = MessageBuilder.withPayload("Message" + i)
					.setHeader("event.eventType", "createEvent").build();
				byte[] jsonInput = objectMapper.writeValueAsBytes(eventMessages);
				producer.newMessage().key("1").value(jsonInput).send();
			}
		}

		assertThat(this.messageBarrier.await(10, TimeUnit.SECONDS)).isTrue();

		Message<List<?>> message = this.messageHolder.get();

		List<?> payload = message.getPayload();
		assertThat(payload).hasSize(10);

		Object item = payload.get(0);

		assertThat(item).isInstanceOf(GenericMessage.class);

		Message<?> messageFromBatch = (Message<?>) item;

		assertThat(messageFromBatch.getPayload()).isEqualTo("Message0");
		assertThat(messageFromBatch.getHeaders())
			.containsEntry("event.eventType", "createEvent");
	}

	@Configuration
	@EnableAutoConfiguration
	static class TestConfiguration {

		@Bean(destroyMethod = "")
		public PulsarClient pulsarClient() {
			return PULSAR_CLIENT;
		}

		@Bean
		public ObjectMapper objectMapper() {
			return JacksonJsonUtils.messagingAwareMapper();
		}

		@Bean
		public AtomicReference<Message<List<?>>> messageHolder() {
			return new AtomicReference<>();
		}

		@Bean
		public CountDownLatch messageBarrier() {
			return new CountDownLatch(1);
		}

		@Bean
		public Consumer<Message<List<?>>> eventConsumerBatchProcessingWithHeaders() {
			return eventMessages -> {
				messageHolder().set(eventMessages);
				messageBarrier().countDown();
			};
		}
	}

}
