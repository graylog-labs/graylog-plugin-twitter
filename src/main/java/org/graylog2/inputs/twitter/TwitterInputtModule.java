package org.graylog2.inputs.twitter;

import org.graylog2.plugin.PluginModule;

public class TwitterInputtModule extends PluginModule {
    @Override
    protected void configure() {
        registerPlugin(TwitterInputMetadata.class);
        addMessageInput(TwitterInput.class);
    }
}
