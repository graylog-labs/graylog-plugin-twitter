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
package org.graylog2.twitterinput.input;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.logmessage.LogMessage;

/**
 * @author Lennart Koopmann <lennart@socketfeed.com>
 */
public class TwitterStreamingConsumer {

    public final static String TARGET_BASE = "stream.twitter.com";
    public final static String TARGET = "/1/statuses/filter.json";

    private final String user;
    private final String password;
    
    // XXX CHECK PARAMS
    // XXX parse foo, bar, zomg to foo,bar,zomg
    
    public TwitterStreamingConsumer(String user, String password) {
        this.user = user;
        this.password = password;
    }
    
    public void consume(Buffer targetBuffer, String track) {
        while (true) {
            try {
                DefaultHttpClient client = new DefaultHttpClient();

                client.getCredentialsProvider().setCredentials(
                    new AuthScope(TARGET_BASE, 443),
                    new UsernamePasswordCredentials(user, password)
                );

                HttpPost post = new HttpPost("https://" + TARGET_BASE + TARGET);
                
                List<NameValuePair> params = new ArrayList<NameValuePair>();
                params.add(new BasicNameValuePair("track", track));
                post.setEntity(new UrlEncodedFormEntity(params));
                
                HttpResponse response = client.execute(post);
                BufferedReader reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

                while (true) {
                    String line = reader.readLine();

                    if (line == null) {
                        break;
                    }

                    if (line.length() > 0) {
                        targetBuffer.insertCached(
                            parseLogMessageFromTweet(parseTweetFromJson(line))
                        );
                    }

                }
            } catch (Exception e) {
                System.err.println("Error in TwitterStreamingConsumer. (Reconnecting in 5 seconds.) - " + e.getMessage());
            }
            
            try { Thread.sleep(5000); } catch (Exception e) { /* ZOMG I don't care */ }
        }
    }

    private Tweet parseTweetFromJson(String json) {
        return new Gson().fromJson(json, Tweet.class);
    }
    
    private LogMessage parseLogMessageFromTweet(Tweet tweet) {
        LogMessage lm = new LogMessage();
        
        lm.setHost("twitter.com");
        lm.setShortMessage(tweet.text);
        lm.setFacility("tweets");
        lm.setLevel(6); // INFO
        
        lm.addAdditionalData("tweet_id", tweet.id);
        
        if (tweet.user != null) {
            lm.setFullMessage("https://twitter.com/" + tweet.user.screen_name + "/status/" + tweet.id);
            lm.addAdditionalData("user_id", tweet.user.id);
            lm.addAdditionalData("user_name", tweet.user.screen_name);
            lm.addAdditionalData("user_timezone", tweet.user.time_zone);
            lm.addAdditionalData("user_location", tweet.user.location);
            lm.addAdditionalData("user_language", tweet.user.lang);
            lm.addAdditionalData("user_followers", tweet.user.followers_count);
            lm.addAdditionalData("user_tweets", tweet.user.statuses_count);
        }
        
        return lm;
    }

}
