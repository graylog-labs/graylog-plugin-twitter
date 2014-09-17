package org.graylog2.inputs.twitter;

import org.graylog2.plugin.Plugin;
import org.graylog2.plugin.PluginModule;

import java.util.Collection;
import java.util.Collections;

public class TwitterInputPlugin implements Plugin {
    @Override
    public Collection<PluginModule> modules() {
        return Collections.<PluginModule>singleton(new TwitterInputtModule());
    }
}
