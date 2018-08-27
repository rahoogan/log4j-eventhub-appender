package com.rahoogan.log4j;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidParameterException;
import java.util.Properties;
import java.text.MessageFormat;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;

import com.microsoft.azure.eventhubs.EventHubException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
 * EventHub Appender
 *
 */
public class EventHubAppender extends AppenderSkeleton
{
    public static final String PROPERTIES_FILE = "config.properties";
    public static final String PROP_EVH_NAMESPACE = "namespacename";
    public static final String PROP_EVH_NAME = "eventhubname";
    public static final String PROP_EVH_SAS_KEYNAME = "saskeyname";
    public static final String PROP_EVH_SAS_KEY = "saskey";
    public static final String PROP_RETRIES = "retries";

    private CompletableFuture<Void> completedFuture;
    private ExecutorService executorService = Executors.newFixedThreadPool(20);
    // private ExecutorService executorService = Executors.newFixedThreadPool((int)Math.round(Runtime.getRuntime().availableProcessors() * 0.75));
    private EventHubClient ehClient;
    private ConnectionStringBuilder connStr;

    private int evhRetryAttempts;
    private String evhNamespace;
    private String evhName;
    private String evhSasKeyname;
    private String evhSasKey;

    final Gson gson;

    public EventHubAppender() throws IOException, EventHubException{
        this.gson = new GsonBuilder().create();
                // Read from properties file
                final Properties properties = new Properties();
                try(final InputStream stream = ClassLoader.getSystemClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
                    properties.load(stream);
                    System.out.println(properties.getProperty(PROP_EVH_NAMESPACE));
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
        
                // Create connection string
                this.connStr = new ConnectionStringBuilder()
                                        .setNamespaceName(this.evhNamespace)
                                        .setEventHubName(this.evhName)
                                        .setSasKeyName(this.evhSasKeyname)
                                        .setSasKey(this.evhSasKey);
                // DEBUG MESSAGE
                System.out.println(connStr.toString());
                // final Gson gson = new GsonBuilder().create();
                // final LogEvent log = new LogEvent(System.currentTimeMillis(), "testy");
                // byte[] logBytes = gson.toJson(log).getBytes();
                // final EventData sendEvent = EventData.create(logBytes);
                EventHubClient.create(connStr.toString(), executorService)
                                .whenComplete((evh, throwable) -> {
                                    System.out.println("hurray!");
                                    this.ehClient = evh;
                                });
                // ehClient.send(sendEvent);
                // ehClient = EventHubClient.createSync(connStr.toString(), executorService);       
                System.out.println("test2");

    }

    @Override
    public void activateOptions() {

        System.out.println("options");
        // Initialise EventHub Client and Threadpool
        // initThreadPool();
        // initEhClient();
    }

    // private synchronized void initEhClient() {
    //     if (null == this.ehClient) {
    //         // Create an eventhub client synchronously
    //         try {
    //             this.ehClient = EventHubClient.createSync(connStr.toString(), executorService);
    //         } catch (EventHubException e) {
    //             errorHandler.error(e.toString());
    //         } catch (IOException e) {
    //             errorHandler.error(e.toString());
    //         }
    //     }
    // }

    // private void initThreadPool() {
    //     if (null == this.executorService) {
    //         this.executorService = Executors.newCachedThreadPool();
    //     }
    // }

    // @Override
    // public void doAppend(LoggingEvent event) {
    //     if(null == this.ehClient) {
    //         initEhClient();
    //     }
    //     if(null == this.executorService) {
    //         initThreadPool();
    //     }
    //     initEhClient();
    // }

    public void send(EventData ev) {
        // ExecutorService executorService = null;
        EventHubClient ehClient = null;
        try {
            try {
                EventHubClient.create(connStr.toString(), executorService)
                                .thenAccept(eh -> {
                                    try {
                                        try {
                                            eh.sendSync(ev);
                                        } catch (EventHubException e) {
                                            System.out.println(e.toString());
                                        } finally {
                                            eh.closeSync();
                                        }
                                    } catch (EventHubException e) {
                                        System.out.println(e.toString());
                                    }
                                })
                                .whenCompleteAsync((evh, throwable) -> {
                                    System.out.println("Yay!");
                                });
            } finally {
                if(ehClient != null) {
                    ehClient.closeSync();
                }                
            }
            
            // try {
            //     ehClient.sendSync(ev);
            // } finally {
            //     if(ehClient != null) {
            //         ehClient.closeSync();
            //     }                
            // }
        } catch (IOException e) {
            System.out.println(e.toString());
        } catch (EventHubException e) {
            System.out.println(e.toString());
        } finally {
            // if (executorService != null) {
            //     executorService.shutdown();
            // }
        }
        System.out.println("done");
    }

    // @Override
    // public void doAppend(LoggingEvent event) {
    //     System.out.println("hello");
    // }

    public void send2(EventData ed) {
        try {
        try {
            ehClient.sendSync(ed);
        } catch (EventHubException e) {
            System.out.println(e.toString());
        }finally {
            ehClient.closeSync();
        }
    } catch (EventHubException e) {
        System.out.println(e.toString());
    }
    }
    @Override
    protected void append(LoggingEvent event) {
        System.out.println("hello again");
        final Gson gson = new GsonBuilder().create();
        final LogEvent log = new LogEvent(System.currentTimeMillis(), event.getRenderedMessage());
        byte[] logBytes = gson.toJson(log).getBytes();
        final EventData sendEvent = EventData.create(logBytes);
        // send(sendEvent);
        LogLog.debug(event.getRenderedMessage());
        System.out.println(event.getRenderedMessage());
        // send2(sendEvent);
        
    }

    @Override
    public void close() {
        // this.ehClient.close();
        this.executorService.shutdown();

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
