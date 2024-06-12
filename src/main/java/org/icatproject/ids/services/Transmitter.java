package org.icatproject.ids.services;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.ejb.Singleton;
import jakarta.jms.JMSException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicConnection;
import jakarta.jms.TopicConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

@Singleton
public class Transmitter {

    private static Logger logger = LoggerFactory.getLogger(Transmitter.class);
    private final static Marker fatal = MarkerFactory.getMarker("FATAL");

    private Topic topic;

    private TopicConnection topicConnection;

    @PostConstruct
    private void init() {

        try {
            PropertyHandler propertyHandler = PropertyHandler.getInstance();
            InitialContext ic = new InitialContext();
            TopicConnectionFactory topicConnectionFactory = (TopicConnectionFactory) ic
                    .lookup(propertyHandler.getJmsTopicConnectionFactory());
            topicConnection = topicConnectionFactory.createTopicConnection();
            topic = (Topic) ic.lookup("jms/IDS/log");
            logger.info("Notification Transmitter created");
        } catch (JMSException | NamingException e) {
            logger.error(fatal, "Problem with JMS " + e);
            throw new IllegalStateException(e.getMessage());
        }

    }

    @PreDestroy()
    private void exit() {
        try {
            if (topicConnection != null) {
                topicConnection.close();
            }
            logger.info("Notification Transmitter closing down");
        } catch (JMSException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    public void processMessage(String operation, String ip, String body,
            long startMillis) {
        try (Session jmsSession = topicConnection.createSession(false,
                Session.AUTO_ACKNOWLEDGE)) {
            TextMessage jmsg = jmsSession.createTextMessage(body);
            jmsg.setStringProperty("operation", operation);
            jmsg.setStringProperty("ip", ip);
            jmsg.setLongProperty("millis",
                    System.currentTimeMillis() - startMillis);
            jmsg.setLongProperty("start", startMillis);
            MessageProducer jmsProducer = jmsSession.createProducer(topic);
            jmsProducer.send(jmsg);
            logger.debug("Sent jms message " + operation + " " + ip);
        } catch (JMSException e) {
            logger.error("Failed to send jms message " + operation + " " + ip);
        }
    }

}
