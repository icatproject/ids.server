package org.icatproject.ids.queues;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.EJB;
import javax.ejb.MessageDriven;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

import org.icatproject.ids.entity.DownloadRequestEntity;
import org.icatproject.ids.util.DownloadRequestHelper;


/**
 * This is a Message Driven Bean that listens for the new download request on the
 * DataRetrievalQueue. Upon reading a new request will retrieve the requested files from storage and
 * package them up in a zip file.
 */
@MessageDriven(mappedName = "jms/IDS/DataRetrievalQueue", activationConfig = {
        @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge"),
        @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue")})
public class DataRetrievalQueueListener implements MessageListener {

    @EJB
    private DownloadRequestHelper downloadRequestHelper;

    private final static Logger logger = Logger.getLogger(DataRetrievalQueueListener.class.getName());
    
    @Override
    public void onMessage(Message message) {
        DownloadRequestEntity downloadRequestEntity = null;
        if (message instanceof ObjectMessage) {
            try {
                Long requestId = (Long) ((ObjectMessage) message).getObject();
                downloadRequestEntity = downloadRequestHelper.getDownloadRequestById(requestId);
                
                if (downloadRequestEntity != null) {
                    downloadRequestHelper.processDataRetrievalRequest(downloadRequestEntity);
                } else {         
                    logger.log(Level.SEVERE, "Could not find the download request Id");
                }
            } catch (JMSException e) {
                logger.log(Level.SEVERE, "Unable to proccess the download request", e);
            }
        }
    }
}
