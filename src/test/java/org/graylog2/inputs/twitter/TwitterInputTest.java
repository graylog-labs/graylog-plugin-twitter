package org.graylog2.inputs.twitter;

import com.google.common.collect.ImmutableMap;
import org.graylog2.plugin.buffers.Buffer;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationException;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.inputs.MisfireException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;

import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TwitterInputTest {
    private TwitterInput twitterInput;

    @Before
    public void setUp() {
        twitterInput = new TwitterInput();
    }

    @Test
    public void getRequestedConfigurationContainsAllConfigurationKeys() {
        final ConfigurationRequest configurationRequest = twitterInput.getRequestedConfiguration();

        assertThat(configurationRequest.asList().keySet(),
                hasItems("keywords", "oauth_consumer_key", "oauth_consumer_secret",
                        "oauth_access_token", "oauth_access_token_secret"));
    }

    @Test
    public void checkConfigurationSucceedsWithValidConfiguration() throws ConfigurationException {
        final Configuration configuration = new Configuration(ImmutableMap.<String, Object>of(
                "keywords", "TEST_keywords1,TEST_keywords2",
                "oauth_consumer_key", "TEST_oauth_consumer_key",
                "oauth_consumer_secret", "TEST_oauth_consumer_secret",
                "oauth_access_token", "TEST_oauth_access_token",
                "oauth_access_token_secret", "TEST_oauth_access_token_secret"
        ));
        twitterInput.checkConfiguration(configuration);
    }

    @Test
    public void checkConfigurationSucceedsIfKeywordsIsMissing() throws ConfigurationException {
        final Configuration configuration = new Configuration(ImmutableMap.<String, Object>of(
                "oauth_consumer_key", "TEST_oauth_consumer_key",
                "oauth_consumer_secret", "TEST_oauth_consumer_secret",
                "oauth_access_token", "TEST_oauth_access_token",
                "oauth_access_token_secret", "TEST_oauth_access_token_secret"
        ));
        twitterInput.checkConfiguration(configuration);
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfOAuthConsumerKeyIsMissing() throws ConfigurationException {
        final Configuration configuration = new Configuration(ImmutableMap.<String, Object>of(
                "keywords", "TEST_keywords1,TEST_keywords2",
                "oauth_consumer_secret", "TEST_oauth_consumer_secret",
                "oauth_access_token", "TEST_oauth_access_token",
                "oauth_access_token_secret", "TEST_oauth_access_token_secret"
        ));
        twitterInput.checkConfiguration(configuration);
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfOAuthConsumerSecretIsMissing() throws ConfigurationException {
        final Configuration configuration = new Configuration(ImmutableMap.<String, Object>of(
                "keywords", "TEST_keywords1,TEST_keywords2",
                "oauth_consumer_key", "TEST_oauth_consumer_key",
                "oauth_access_token", "TEST_oauth_access_token",
                "oauth_access_token_secret", "TEST_oauth_access_token_secret"
        ));
        twitterInput.checkConfiguration(configuration);
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfOAuthAccessTokenIsMissing() throws ConfigurationException {
        final Configuration configuration = new Configuration(ImmutableMap.<String, Object>of(
                "keywords", "TEST_keywords1,TEST_keywords2",
                "oauth_consumer_key", "TEST_oauth_consumer_key",
                "oauth_consumer_secret", "TEST_oauth_consumer_secret",
                "oauth_access_token_secret", "TEST_oauth_access_token_secret"
        ));
        twitterInput.checkConfiguration(configuration);
    }

    @Test(expected = ConfigurationException.class)
    public void checkConfigurationFailsIfOAuthAccessTokenSecretIsMissing() throws ConfigurationException {
        final Configuration configuration = new Configuration(ImmutableMap.<String, Object>of(
                "keywords", "TEST_keywords1,TEST_keywords2",
                "oauth_consumer_key", "TEST_oauth_consumer_key",
                "oauth_consumer_secret", "TEST_oauth_consumer_secret",
                "oauth_access_token", "TEST_oauth_access_token"
        ));
        twitterInput.checkConfiguration(configuration);
    }

    @Test
    public void testLaunch() throws MisfireException {
        final Buffer processBuffer = mock(Buffer.class);
        final Configuration configuration = new Configuration(ImmutableMap.<String, Object>of(
                "keywords", "TEST_keywords1,TEST_keywords2",
                "oauth_consumer_key", "TEST_oauth_consumer_key",
                "oauth_consumer_secret", "TEST_oauth_consumer_secret",
                "oauth_access_token", "TEST_oauth_access_token",
                "oauth_access_token_secret", "TEST_oauth_access_token_secret"
        ));
        final TwitterStream mockTwitterStream = mock(TwitterStream.class);
        final TwitterInput input = new TwitterInput(mockTwitterStream);
        input.initialize(configuration);
        input.setConfiguration(configuration);
        input.launch(processBuffer);

        ArgumentCaptor<FilterQuery> filterQueryArgument = ArgumentCaptor.forClass(FilterQuery.class);
        verify(mockTwitterStream).filter(filterQueryArgument.capture());
        assertThat(filterQueryArgument.getValue().toString(), containsString("track=[TEST_keywords1, TEST_keywords2]"));
    }

    @Test
    public void testStop() {
        final TwitterStream mockTwitterStream = mock(TwitterStream.class);
        final TwitterInput input = new TwitterInput(mockTwitterStream);
        input.stop();

        verify(mockTwitterStream, times(1)).cleanUp();
        verify(mockTwitterStream, times(1)).shutdown();
        verifyNoMoreInteractions(mockTwitterStream);
    }

    @Test
    public void testIsExclusive() {
        assertThat(twitterInput.isExclusive(), is(false));
    }

    @Test
    public void testGetAttributes() {
        final Configuration configuration = new Configuration(ImmutableMap.<String, Object>of(
                "keywords", "TEST_keywords1,TEST_keywords2",
                "oauth_consumer_key", "TEST_oauth_consumer_key",
                "oauth_consumer_secret", "TEST_oauth_consumer_secret",
                "oauth_access_token", "TEST_oauth_access_token",
                "oauth_access_token_secret", "TEST_oauth_access_token_secret"
        ));
        twitterInput.initialize(configuration);
        twitterInput.setConfiguration(configuration);
        final Map<String, Object> attributes = twitterInput.getAttributes();

        assertThat((String) attributes.get("oauth_consumer_secret"), equalTo("****"));
        assertThat((String) attributes.get("oauth_access_token_secret"), equalTo("****"));
    }

    @Test
    public void testGetName() throws Exception {
        assertThat(twitterInput.getName(), equalTo("Twitter input"));
    }
}
