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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.graylog2.plugin.GraylogServer;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MessageInputConfigurationException;

/**
 * @author Lennart Koopmann <lennart@socketfeed.com>
 */
public class TwitterInput implements MessageInput {

    private static final String NAME = "Twitter input";
    
    private Map<String, String> configuration;
    
    public static final Set<String> REQUIRED_FIELDS = new HashSet<String>();

    public void initialize(Map<String, String> config, GraylogServer graylogServer) throws MessageInputConfigurationException {
        TwitterStreamingConsumer tsc = new TwitterStreamingConsumer(config.get("username"), config.get("password"));
        tsc.consume(graylogServer.getProcessBuffer(), config.get("search_for"));
    }

    public Map<String, String> getRequestedConfiguration() {
        Map<String, String> config = new HashMap<String, String>();

        config.put("search_for", "Keywords to track. Phrases of keywords, specified by a comma-separated list.");
        config.put("username", "Twitter username");
        config.put("password", "Twitter password");
        
        return config;
    }

    public String getName() {
        return NAME;
    }
    
}
