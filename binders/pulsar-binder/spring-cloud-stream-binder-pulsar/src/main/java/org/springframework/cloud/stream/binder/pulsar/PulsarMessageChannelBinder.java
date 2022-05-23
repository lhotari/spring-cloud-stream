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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarConsumerProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarExtendedBindingProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarProducerProperties;
import org.springframework.cloud.stream.binder.pulsar.provisioning.PulsarTopicProvisioner;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.Lifecycle;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.core.Pausable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 *
 * The Spring Cloud Stream Binder implementation for Apache Pulsar.
 *
 * @author Lari Hotari
 *
 */
public class PulsarMessageChannelBinder extends
		AbstractMessageChannelBinder<ExtendedConsumerProperties<PulsarConsumerProperties>, ExtendedProducerProperties<PulsarProducerProperties>, PulsarTopicProvisioner>
		implements
		ExtendedPropertiesBinder<MessageChannel, PulsarConsumerProperties, PulsarProducerProperties> {

	private final PulsarBinderConfigurationProperties configurationProperties;
	private final PulsarClient pulsarClient;

	private PulsarExtendedBindingProperties extendedBindingProperties = new PulsarExtendedBindingProperties();

	public PulsarMessageChannelBinder(
			PulsarBinderConfigurationProperties configurationProperties,
			PulsarTopicProvisioner provisioningProvider, PulsarClient pulsarClient) {

		super(headersToMap(configurationProperties), provisioningProvider);
		this.configurationProperties = configurationProperties;
		this.pulsarClient = pulsarClient;
	}

	public void setExtendedBindingProperties(
			PulsarExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	@Override
	public PulsarConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public PulsarProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	public String getDefaultsPrefix() {
		return this.extendedBindingProperties.getDefaultsPrefix();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return this.extendedBindingProperties.getExtendedPropertiesEntryClass();
	}

	private static String[] headersToMap(
			PulsarBinderConfigurationProperties configurationProperties) {
		Assert.notNull(configurationProperties,
				"'configurationProperties' must not be null");
		if (ObjectUtils.isEmpty(configurationProperties.getHeaders())) {
			return BinderHeaders.STANDARD_HEADERS;
		}
		else {
			String[] combinedHeadersToMap = Arrays.copyOfRange(
					BinderHeaders.STANDARD_HEADERS, 0,
					BinderHeaders.STANDARD_HEADERS.length
							+ configurationProperties.getHeaders().length);
			System.arraycopy(configurationProperties.getHeaders(), 0,
					combinedHeadersToMap, BinderHeaders.STANDARD_HEADERS.length,
					configurationProperties.getHeaders().length);
			return combinedHeadersToMap;
		}
	}

	@Override
	protected MessageHandler createProducerMessageHandler(ProducerDestination destination,
			ExtendedProducerProperties<PulsarProducerProperties> producerProperties,
			MessageChannel errorChannel) throws Exception {

		class PulsarMessageHandler implements MessageHandler, Lifecycle {
			private final Producer<byte[]> pulsarProducer;
			private volatile boolean running;

			public PulsarMessageHandler() {
				try {
					pulsarProducer = pulsarClient.newProducer()
							.topic(destination.getName()).create();
				}
				catch (PulsarClientException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				try {
					pulsarProducer.newMessage()
							// support just byte[] for now
							.value((byte[]) message.getPayload())
							// map headers to Map<String, String>
							.properties(message.getHeaders().entrySet().stream()
									.map(entry -> Map.entry(entry.getKey(),
											entry.getValue() != null
													? String.valueOf(entry.getValue())
													: null))
									.collect(Collectors.toUnmodifiableMap(
											Map.Entry::getKey, Map.Entry::getValue)))
							.send();
				}
				catch (PulsarClientException e) {
					throw new MessageDeliveryException(message, e);
				}
			}

			@Override public void start() {
				running = true;
			}

			@Override public void stop() {
				running = false;
				try {
					pulsarProducer.close();
				}
				catch (PulsarClientException e) {
					throw new RuntimeException(e);
				}
			}

			@Override public boolean isRunning() {
				return running;
			}
		}

		return new PulsarMessageHandler();
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination destination,
			String group,
			ExtendedConsumerProperties<PulsarConsumerProperties> properties) {

		class PulsarMessageProducer implements MessageProducer, Pausable {
			private final Consumer<byte[]> pulsarConsumer;
			private MessageChannel outputChannel;

			private volatile boolean running;

			PulsarMessageProducer() {
				try {
					pulsarConsumer = pulsarClient.newConsumer()
							.topic(destination.getName())
							.subscriptionType(SubscriptionType.Shared)
							.subscriptionName(
									group == null || group.isBlank() ? "anonymous"
											: group)
							.messageListener(this::consumeMessage).startPaused(true)
							.subscribe();
				}
				catch (PulsarClientException e) {
					throw new RuntimeException(e);
				}
			}

			private void consumeMessage(Consumer<byte[]> consumer,
					org.apache.pulsar.client.api.Message<byte[]> message) {
				GenericMessage<byte[]> msg = new GenericMessage<>(message.getValue(),
						new HashMap<>(message.getProperties()));
				outputChannel.send(msg);
			}

			@Override
			public void setOutputChannel(MessageChannel outputChannel) {
				this.outputChannel = outputChannel;
			}

			@Override
			public MessageChannel getOutputChannel() {
				return outputChannel;
			}

			@Override
			public void pause() {
				pulsarConsumer.pause();
			}

			@Override
			public void resume() {
				pulsarConsumer.resume();
			}

			@Override
			public void start() {
				running = true;
				pulsarConsumer.resume();
			}

			@Override
			public void stop() {
				try {
					running = false;
					pulsarConsumer.close();
				}
				catch (PulsarClientException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public boolean isRunning() {
				return running;
			}
		}

		return new PulsarMessageProducer();
	}
}
