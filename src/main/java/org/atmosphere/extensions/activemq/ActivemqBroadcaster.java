package org.atmosphere.extensions.activemq;

import org.apache.activemq.command.ActiveMQTopic;
import org.atmosphere.cpr.AtmosphereConfig;
import org.atmosphere.util.AbstractBroadcasterProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.util.Date;

/**
 * Created by IntelliJ IDEA.
 * User: przemyslaw.palak
 * Date: 20.03.13
 * Time: 15:47
 */
public class ActivemqBroadcaster extends AbstractBroadcasterProxy {
    private static final Logger logger = LoggerFactory.getLogger(ActivemqBroadcaster.class);
    public static final String BROADCASTER_CONTAINER_NAME = "activemqBrodcasterContainer";

    private Connection connection;
    private Session consumerSession;
    private Session publisherSession;
    private Topic topic;
    private MessageConsumer consumer;
    private MessageProducer publisher;
    private String defaultTopicName;

    ConnectionFactory connectionFactory;

    private long lastTouchTs;

    public long getLastTouchTs() {
        return lastTouchTs;
    }

    public void touch(){
        lastTouchTs = new Date().getTime();
    }

    private String getTopicName(){
        String id = getID();
        if (id.startsWith("/*")) {
            return  defaultTopicName;
        }
        return id;
    }

    public ActivemqBroadcaster(String id, AtmosphereConfig config) {
        super(id, null, config);
        ApplicationContext context = WebApplicationContextUtils.getWebApplicationContext(config.getServletContext());
        ActivemqBrodcasterContainer container = (ActivemqBrodcasterContainer) context.getBean(BROADCASTER_CONTAINER_NAME);
        connectionFactory = container.getConnectionFactory();
        defaultTopicName = container.getDefaultTopicName();
        container.register(this);
        topic = new ActiveMQTopic(getTopicName());
        setUp();
        logger.trace("New broadcaster is created witch id: "+id);
    }

    private void setUp() {
        try {
            connection = connectionFactory.createConnection();
            consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            publisherSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            publisher = publisherSession.createProducer(topic);
            connection.start();
        } catch (Exception e) {
            String msg = "Unable to configure JMSBroadcaster";
            logger.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incomingBroadcast() {
        // we setup the consumer in the setID method. No need to do it here too
    }

    @Override
    public void setID(String id) {
        super.setID(id);
        restartConsumer();
    }

    void restartConsumer() {
        try {
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }

            logger.info("Create JMS consumer: ");
            consumer = consumerSession.createConsumer(topic);
            consumer.setMessageListener(new MessageListener() {

                public void onMessage(Message msg) {
                    try {
                        TextMessage textMessage = (TextMessage) msg;
                        String message = textMessage.getText();

                        if (message != null && bc != null) {
                            broadcastReceivedMessage(message);
                        }
                    } catch (JMSException ex) {
                        logger.warn("Failed to broadcast message", ex);
                    }
                }
            });
        } catch (Throwable ex) {
            String msg = "Unable to initialize JMSBroadcaster";
            logger.error(msg, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void outgoingBroadcast(Object message) {
        try {

            if (publisherSession == null) {
                throw new IllegalStateException("JMS Session is null");
            }

            TextMessage textMessage = publisherSession.createTextMessage(message
                    .toString());
            //textMessage.setStringProperty("BroadcasterId", id);
            publisher.send(textMessage);
        } catch (JMSException ex) {
            logger.warn("Failed to send message over JMS", ex);
        }
    }

    /**
     * Close all related JMS factory, connection, etc.
     */
    @Override
    public synchronized void releaseExternalResources() {
        try {
            connection.close();
            consumerSession.close();
            publisherSession.close();
            consumer.close();
            publisher.close();
            logger.trace("Disposing broadcaster witch id: "+getID());
        } catch (Throwable ex) {
            logger.warn("releaseExternalResources", ex);
        }
    }


}
