package org.graylog2.inputs.twitter;

import org.graylog2.plugin.PluginModule;

public class TwitterInputModule extends PluginModule {
    @Override
    protected void configure() {
        addTransport("twitter", TwitterTransport.class);
        addCodec("twitter", TwitterCodec.class);
        addMessageInput(TwitterInput.class);
    }
}
