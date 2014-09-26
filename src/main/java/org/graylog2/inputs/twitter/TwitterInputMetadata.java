package org.graylog2.inputs.twitter;

import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.Version;

import java.net.URI;

public class TwitterInputMetadata implements PluginMetaData {
    @Override
    public String getUniqueId() {
        return TwitterInput.class.getCanonicalName();
    }

    @Override
    public String getName() {
        return "Twitter Input Plugin";
    }

    @Override
    public String getAuthor() {
        return "TORCH GmbH";
    }

    @Override
    public URI getURL() {
        return URI.create("http://www.torch.sh");
    }

    @Override
    public Version getVersion() {
        return new Version(0, 90, 0);
    }

    @Override
    public String getDescription() {
        return "Ingest tweets from the Twitter Firehose filtered by keywords.";
    }

    @Override
    public Version getRequiredVersion() {
        return new Version(0, 90, 0);
    }
}
