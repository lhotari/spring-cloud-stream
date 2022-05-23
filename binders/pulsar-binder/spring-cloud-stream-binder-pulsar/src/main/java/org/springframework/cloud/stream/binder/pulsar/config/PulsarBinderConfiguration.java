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

package org.springframework.cloud.stream.binder.pulsar.config;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.pulsar.PulsarMessageChannelBinder;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.pulsar.properties.PulsarExtendedBindingProperties;
import org.springframework.cloud.stream.binder.pulsar.provisioning.PulsarTopicProvisioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * The auto-configuration for Apache Pulsar components and Spring Cloud Stream Pulsar Binder.
 *
 * @author Lari Hotari
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnMissingBean(Binder.class)
@EnableConfigurationProperties({ PulsarBinderConfigurationProperties.class, PulsarExtendedBindingProperties.class,
	PulsarClientConfig.class})
public class PulsarBinderConfiguration {

	private final PulsarBinderConfigurationProperties configurationProperties;

	public PulsarBinderConfiguration(PulsarBinderConfigurationProperties configurationProperties) {

		this.configurationProperties = configurationProperties;
	}

	@Bean
	public PulsarTopicProvisioner pulsarTopicProvisioner() {
		return new PulsarTopicProvisioner(configurationProperties);
	}

	@Lazy
	@Bean
	@ConditionalOnMissingBean
	PulsarClient pulsarClient(PulsarClientConfig pulsarClientConfig) throws PulsarClientException {
		return new ClientBuilderImpl(pulsarClientConfig).build();
	}

	@Bean
	public PulsarMessageChannelBinder pulsarMessageChannelBinder(
			PulsarClient pulsarClient,
			PulsarTopicProvisioner provisioningProvider,
			PulsarExtendedBindingProperties pulsarExtendedBindingProperties) {

		PulsarMessageChannelBinder pulsarMessageChannelBinder =
				new PulsarMessageChannelBinder(this.configurationProperties, provisioningProvider, pulsarClient);
		return pulsarMessageChannelBinder;
	}
}
