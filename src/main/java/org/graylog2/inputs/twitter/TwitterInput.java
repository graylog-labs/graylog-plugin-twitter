/**
 * Copyright 2012 Lennart Koopmann <lennart@socketfeed.com>
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.graylog2.inputs.twitter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationException;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MisfireException;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;

public class TwitterInput extends MessageInput {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterInput.class);
    private static final String NAME = "Twitter input";

    private static final String CK_KEYWORDS = "keywords";
    private static final String CK_OAUTH_CONSUMER_KEY = "oauth_consumer_key";
    private static final String CK_OAUTH_CONSUMER_SECRET = "oauth_consumer_secret";
    private static final String CK_OAUTH_ACCESS_TOKEN = "oauth_access_token";
    private static final String CK_OAUTH_ACCESS_TOKEN_SECRET = "oauth_access_token_secret";

    private static final String[] MANDATORY_CONFIG_FIELDS = new String[]{
            CK_OAUTH_CONSUMER_KEY, CK_OAUTH_CONSUMER_SECRET, CK_OAUTH_ACCESS_TOKEN, CK_OAUTH_ACCESS_TOKEN_SECRET
    };

    static {
        System.setProperty("twitter4j.loggerFactory", "twitter4j.SLF4JLoggerFactory");
    }

    private TwitterStream twitterStream;

    public TwitterInput() {
    }

    @VisibleForTesting
    TwitterInput(final TwitterStream twitterStream) {
        this.twitterStream = twitterStream;
    }

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

    @Override
    public void checkConfiguration(Configuration configuration) throws ConfigurationException {
        for (String key : MANDATORY_CONFIG_FIELDS) {
            if (!configuration.stringIsSet(key)) {
                throw new ConfigurationException(key + " must not be empty.");
            }
        }
    }

    @Override
    public void launch(final Buffer processBuffer) throws MisfireException {
        final ConfigurationBuilder cb = new ConfigurationBuilder()
                .setOAuthConsumerKey(getConfiguration().getString(CK_OAUTH_CONSUMER_KEY))
                .setOAuthConsumerSecret(getConfiguration().getString(CK_OAUTH_CONSUMER_SECRET))
                .setOAuthAccessToken(getConfiguration().getString(CK_OAUTH_ACCESS_TOKEN))
                .setOAuthAccessTokenSecret(getConfiguration().getString(CK_OAUTH_ACCESS_TOKEN_SECRET));

        final StatusListener listener = new StatusListener() {
            public void onStatus(final Status status) {
                processBuffer.insertCached(createMessageFromStatus(status), TwitterInput.this);
            }

            private Message createMessageFromStatus(final Status status) {
                final Message message = new Message(status.getText(), "twitter.com", new DateTime(status.getCreatedAt()));

                message.addField("facility", "Tweets");
                message.addField("level", 6);
                message.addField("tweet_id", status.getId());
                message.addField("tweet_is_retweet", Boolean.toString(status.isRetweet()));
                message.addField("tweet_favorite_count", status.getFavoriteCount());
                message.addField("tweet_retweet_count", status.getRetweetCount());
                message.addField("tweet_language", status.getLang());


                final GeoLocation geoLocation = status.getGeoLocation();
                if (geoLocation != null) {
                    message.addField("tweet_geo_long", geoLocation.getLongitude());
                    message.addField("tweet_geo_lat", geoLocation.getLatitude());
                }

                final User user = status.getUser();
                if (user != null) {
                    message.addField("tweet_url", "https://twitter.com/" + user.getScreenName() + "/status/" + status.getId());
                    message.addField("user_id", user.getId());
                    message.addField("user_name", user.getScreenName());
                    message.addField("user_description", user.getDescription());
                    message.addField("user_timezone", user.getTimeZone());
                    message.addField("user_utc_offset", user.getUtcOffset());
                    message.addField("user_location", user.getLocation());
                    message.addField("user_language", user.getLang());
                    message.addField("user_url", user.getURL());
                    message.addField("user_followers", user.getFollowersCount());
                    message.addField("user_tweets", user.getStatusesCount());
                    message.addField("user_favorites", user.getFavouritesCount());
                }
                return message;
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
                .split(getConfiguration().getString(CK_KEYWORDS)), String.class);
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
    public boolean isExclusive() {
        return false;
    }

    @Override
    public String linkToDocs() {
        return null;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return Maps.transformEntries(getConfiguration().getSource(), new Maps.EntryTransformer<String, Object, Object>() {
            @Override
            public Object transformEntry(String key, Object value) {
                if (CK_OAUTH_ACCESS_TOKEN_SECRET.equals(key) || CK_OAUTH_CONSUMER_SECRET.equals(key)) {
                    return "****";
                }
                return value;
            }
        });
    }

    @Override
    public String getName() {
        return NAME;
    }
}