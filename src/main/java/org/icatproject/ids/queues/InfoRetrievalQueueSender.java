package org.icatproject.ids.queues;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import org.icatproject.ids.entity.DownloadRequestEntity;


/**
 * This class adds new requests to the InfoRetrievalQueue.
 */
@Stateless
public class InfoRetrievalQueueSender
{
    @Resource(mappedName = "jms/IDS/InfoRetrievalQueueFactory")
    private ConnectionFactory connectionFactory;
    
    @Resource(mappedName = "jms/IDS/InfoRetrievalQueue")
    private Queue queue;
    
    public void addInfoRetrievalRequest(DownloadRequestEntity downloadRequestEntity)
    {
        try {
            Connection connection = connectionFactory.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = session.createProducer(queue);

            ObjectMessage message = session.createObjectMessage();
            message.setObject(downloadRequestEntity.getId());
            messageProducer.send(message);
            messageProducer.close();
            connection.close();
        } catch (JMSException ex) {
            Logger.getLogger(InfoRetrievalQueueSender.class.getName()).log(Level.SEVERE, "Couldn't send message to InfoRetrievalQueue", ex);
        }        
    }
}

