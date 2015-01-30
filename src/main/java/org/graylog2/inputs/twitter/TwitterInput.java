package org.graylog2.inputs.twitter;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;

import javax.inject.Inject;

public class TwitterInput extends MessageInput {
    private static final String NAME = "Twitter";

    @AssistedInject
    public TwitterInput(MetricRegistry metricRegistry,
                        @Assisted Configuration configuration,
                        TwitterTransport.Factory transportFactory,
                        TwitterCodec.Factory codecFactory,
                        LocalMetricRegistry localRegistry,
                        Config config,
                        Descriptor descriptor,
                        ServerStatus serverStatus) {
        super(metricRegistry, configuration, transportFactory.create(configuration),
                localRegistry, codecFactory.create(configuration), config, descriptor, serverStatus);
    }

    @FactoryClass
    public interface Factory extends MessageInput.Factory<TwitterInput> {
        @Override
        TwitterInput create(Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }

    public static class Descriptor extends MessageInput.Descriptor {
        @Inject
        public Descriptor() {
            super(NAME, false, "");
        }
    }

    @ConfigClass
    public static class Config extends MessageInput.Config {
        @Inject
        public Config(TwitterTransport.Factory transport, TwitterCodec.Factory codec) {
            super(transport.getConfig(), codec.getConfig());
        }
    }
}