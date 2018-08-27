package com.rahoogan.log4j;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.InvalidParameterException;
import java.util.Properties;
import java.text.MessageFormat;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rahoogan.http.HttpPostMethodThread;
import com.rahoogan.http.EventHubHttp;

/**
 * 
 */
public final class EventHubRestAppender extends AppenderSkeleton {
    public static final String PROPERTIES_FILE = "config.properties";
    public static final String PROP_EVH_NAMESPACE = "namespacename";
    public static final String PROP_EVH_NAME = "eventhubname";
    public static final String PROP_EVH_SAS_KEYNAME = "saskeyname";
    public static final String PROP_EVH_SAS_KEY = "saskey";
    public static final String PROP_RETRIES = "retries";

    private final Gson gson;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private URL evhUrl;
    private String evhNamespace;
    private String evhName;
    private String evhSasKeyname;
    private String evhSasKey;
    private int evhRetryAttempts;

    /**
     * 
     */
    public EventHubRestAppender() {
        // Read from properties file
        final Properties properties = new Properties();
        try(final InputStream stream = ClassLoader.getSystemClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            properties.load(stream);
            // Check all properties have been specified
            if (properties.getProperty(PROP_EVH_NAMESPACE) == null || 
                properties.getProperty(PROP_EVH_NAMESPACE) == "") {
                    throw new InvalidParameterException(MessageFormat.format("Missing value for key {0}!", PROP_EVH_NAMESPACE));
            }
            if (properties.getProperty(PROP_EVH_NAME) == null || 
                properties.getProperty(PROP_EVH_NAME) == "") {
                throw new InvalidParameterException(MessageFormat.format("Missing value for key {0}!", PROP_EVH_NAME));
            }
            if (properties.getProperty(PROP_EVH_SAS_KEYNAME) == null || 
                properties.getProperty(PROP_EVH_SAS_KEYNAME) == "") {
                throw new InvalidParameterException(MessageFormat.format("Missing value for key {0}!", PROP_EVH_SAS_KEYNAME));
            }
            if (properties.getProperty(PROP_EVH_SAS_KEY) == null || 
                properties.getProperty(PROP_EVH_SAS_KEY) == "") {
                throw new InvalidParameterException(MessageFormat.format("Missing value for key {0}!", PROP_EVH_SAS_KEY));
            }
            // Optional retry attempts value
            this.evhRetryAttempts = 0;
            if (properties.getProperty(PROP_RETRIES) != null &&
                properties.getProperty(PROP_RETRIES) != "") {
                    try {
                        this.evhRetryAttempts = Integer.parseInt(properties.getProperty(PROP_RETRIES));
                    } catch(NumberFormatException e) {
                        LogLog.debug("Could not set number of retries - Number format exception");
                    }
            }
        } catch (IOException e) {
            errorHandler.error(e.toString());
        } catch (InvalidParameterException e) {
            errorHandler.error(e.toString());
        }
        // Initialise EventHub Connection Properties
        this.evhNamespace = properties.getProperty(PROP_EVH_NAMESPACE);
        this.evhName = properties.getProperty(PROP_EVH_NAME);
        this.evhSasKeyname = properties.getProperty(PROP_EVH_SAS_KEYNAME);
        this.evhSasKey = properties.getProperty(PROP_EVH_SAS_KEY);

        gson = new GsonBuilder().create();
        URIBuilder ub = new URIBuilder();
        String host = new StringBuilder().append(this.evhNamespace).append(".servicebus.windows.net").toString();
        String path = new StringBuilder().append("/").append(this.evhName).append("/messages").toString();
        ub.setHost(host);
        ub.setPath(path);
        ub.addParameter("timeout", "60");
        ub.addParameter("api-version", "2014-01");
        ub.setScheme("https");

        try {
            this.evhUrl = ub.build().toURL();
        } catch (MalformedURLException e) {
            errorHandler.error(e.toString());
        } catch (URISyntaxException e) {
            errorHandler.error(e.toString());
        }
    }

    @Override
    public void activateOptions() {
        // Do Nothing
    }

    @Override
    protected void append(LoggingEvent event) {
        /**
         * POST https://your-namespace.servicebus.windows.net/your-event-hub/messages?timeout=60&api-version=2014-01 HTTP/1.1  
            Authorization: SharedAccessSignature sr=your-namespace.servicebus.windows.net&sig=your-sas-key&se=1403736877&skn=RootManageSharedAccessKey  
            Content-Type: application/atom+xml;type=entry;charset=utf-8  
            Host: your-namespace.servicebus.windows.net  

            { "DeviceId":"dev-01", "Temperature":"37.0" }  
         */

        final LogEvent log = new LogEvent(System.currentTimeMillis(), event.getRenderedMessage());
        String logString = gson.toJson(log);
        StringEntity data = null;
        try {
            data = new StringEntity(logString);
        } catch (UnsupportedEncodingException e) {
            errorHandler.error(e.toString());
        }

        HttpPost post = new HttpPost(evhUrl.toString());
        post.addHeader("Authorization",EventHubHttp.GetSASToken(this.evhNamespace, this.evhSasKeyname, this.evhSasKey));
        post.addHeader("Content-Type", "application/atom+xml;type=entry;charset=utf-8;");
        post.setEntity(data);

        HttpPostMethodThread hp = new HttpPostMethodThread(post, errorHandler);
        executorService.submit(hp);

        // future = CompletableFuture.runAsync(new HttpPostMethodThread(post, errorHandler));
    }

    @Override
    public void close() {
        executorService.shutdown();
    }

    @Override
    public boolean requiresLayout()
    {
        return false;
    }

    /**
     * Based on the Azure Advanced Send Options here: 
     * https://github.com/Azure/azure-event-hubs/blob/master/samples/Java/Basic/AdvancedSendOptions/src/main/java/com/microsoft/azure/eventhubs/samples/AdvancedSendOptions/AdvancedSendOptions.java
     */
    static final class LogEvent {
        LogEvent(final long seed, final String message) {
            this.id = "log-event" + seed;
            this.logMessage = message;
        }

        public String id;
        public String logMessage;
    }
}