/**
 * Copyright 2013-2014 TORCH GmbH, 2015 Graylog, Inc.
 *
 * This file is part of Graylog.
 *
 * Graylog is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.graylog2.inputs.twitter;

import com.codahale.metrics.MetricSet;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.graylog2.plugin.LocalMetricRegistry;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.inputs.transports.Transport;
import org.graylog2.plugin.journal.RawMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TwitterTransport implements Transport {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterTransport.class);

    private static final String CK_KEYWORDS = "keywords";
    private static final String CK_OAUTH_CONSUMER_KEY = "oauth_consumer_key";
    private static final String CK_OAUTH_CONSUMER_SECRET = "oauth_consumer_secret";
    private static final String CK_OAUTH_ACCESS_TOKEN = "oauth_access_token";
    private static final String CK_OAUTH_ACCESS_TOKEN_SECRET = "oauth_access_token_secret";

    private final Configuration configuration;
    private final LocalMetricRegistry localRegistry;

    private TwitterStream twitterStream;

    @AssistedInject
    public TwitterTransport(@Assisted Configuration configuration,
                            LocalMetricRegistry localRegistry) {
        this.configuration = configuration;
        this.localRegistry = localRegistry;
    }


    @Override
    public void setMessageAggregator(CodecAggregator aggregator) {
        // NOP
    }

    @Override
    public void launch(final MessageInput input) throws MisfireException {
        final ConfigurationBuilder cb = new ConfigurationBuilder()
                .setOAuthConsumerKey(configuration.getString(CK_OAUTH_CONSUMER_KEY))
                .setOAuthConsumerSecret(configuration.getString(CK_OAUTH_CONSUMER_SECRET))
                .setOAuthAccessToken(configuration.getString(CK_OAUTH_ACCESS_TOKEN))
                .setOAuthAccessTokenSecret(configuration.getString(CK_OAUTH_ACCESS_TOKEN_SECRET))
                .setJSONStoreEnabled(true);

        final StatusListener listener = new StatusListener() {
            public void onStatus(final Status status) {
                try {
                    input.processRawMessage(createMessageFromStatus(status));
                } catch (IOException e) {
                    LOG.debug("Error while processing tweet status", e);
                }
            }

            private RawMessage createMessageFromStatus(final Status status) throws IOException {
                return new RawMessage(TwitterObjectFactory.getRawJSON(status).getBytes(StandardCharsets.UTF_8));
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onException(final Exception ex) {
                LOG.error("Error while reading Twitter stream", ex);
            }

            @Override
            public void onScrubGeo(long lon, long lat) {
            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {
                LOG.info("Stall warning: {} ({}% full)", stallWarning.getMessage(), stallWarning.getPercentFull());
            }
        };


        final String[] track = Iterables.toArray(Splitter.on(',').omitEmptyStrings().trimResults()
                .split(configuration.getString(CK_KEYWORDS)), String.class);
        final FilterQuery filterQuery = new FilterQuery();
        filterQuery.track(track);

        if (twitterStream == null) {
            twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        }

        twitterStream.addListener(listener);
        twitterStream.filter(filterQuery);
    }

    @Override
    public void stop() {
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }

    @Override
    public MetricSet getMetricSet() {
        return localRegistry;
    }


    @FactoryClass
    public interface Factory extends Transport.Factory<TwitterTransport> {
        TwitterTransport create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config implements Transport.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            ConfigurationRequest c = new ConfigurationRequest();

            c.addField(new TextField(
                    CK_KEYWORDS,
                    "Keywords",
                    "",
                    "Keywords to track. Phrases of keywords, specified by a comma-separated list.",
                    ConfigurationField.Optional.NOT_OPTIONAL
            ));

            c.addField(new TextField(
                    CK_OAUTH_ACCESS_TOKEN,
                    "Access Token",
                    "",
                    "The OAuth Access Token for accessing the Twitter API.",
                    ConfigurationField.Optional.NOT_OPTIONAL
            ));
            c.addField(new TextField(
                    CK_OAUTH_ACCESS_TOKEN_SECRET,
                    "Access Token Secret",
                    "",
                    "The OAuth Access Token Secret for accessing the Twitter API.",
                    ConfigurationField.Optional.NOT_OPTIONAL,
                    TextField.Attribute.IS_PASSWORD
            ));
            c.addField(new TextField(
                    CK_OAUTH_CONSUMER_KEY,
                    "Consumer Key",
                    "",
                    "The OAuth Consumer Key for accessing the Twitter API.",
                    ConfigurationField.Optional.NOT_OPTIONAL
            ));
            c.addField(new TextField(
                    CK_OAUTH_CONSUMER_SECRET,
                    "Consumer Secret",
                    "",
                    "The OAuth Consumer Secret for accessing the Twitter API.",
                    ConfigurationField.Optional.NOT_OPTIONAL,
                    TextField.Attribute.IS_PASSWORD
            ));

            return c;
        }
    }
}
