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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.assistedinject.Assisted;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.inputs.annotations.Codec;
import org.graylog2.plugin.inputs.annotations.ConfigClass;
import org.graylog2.plugin.inputs.annotations.FactoryClass;
import org.graylog2.plugin.inputs.codecs.AbstractCodec;
import org.graylog2.plugin.inputs.codecs.CodecAggregator;
import org.graylog2.plugin.journal.RawMessage;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import twitter4j.User;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Codec(name = "twitter", displayName = "Twitter")
public class TwitterCodec extends AbstractCodec {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterCodec.class);

    private final ObjectMapper objectMapper;

    @Inject
    public TwitterCodec(@Assisted Configuration configuration, ObjectMapper objectMapper) {
        super(configuration);
        this.objectMapper = objectMapper;
    }

    @Nullable
    @Override
    public Message decode(@Nonnull final RawMessage rawMessage) {
        final Status status;
        try {
            status = TwitterObjectFactory.createStatus(new String(rawMessage.getPayload(), StandardCharsets.UTF_8));
        } catch (TwitterException e) {
            LOG.warn("Error while decoding raw message", e);
            return null;
        }

        return createMessageFromStatus(status);
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

    @Nullable
    @Override
    public CodecAggregator getAggregator() {
        return null;
    }

    @FactoryClass
    public interface Factory extends AbstractCodec.Factory<TwitterCodec> {
        @Override
        TwitterCodec create(Configuration configuration);

        @Override
        Config getConfig();
    }

    @ConfigClass
    public static class Config extends AbstractCodec.Config {
        @Override
        public ConfigurationRequest getRequestedConfiguration() {
            return super.getRequestedConfiguration();
        }
    }
}