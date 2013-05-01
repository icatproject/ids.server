package org.icatproject.ids.queues;

import java.net.MalformedURLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.EJB;
import javax.ejb.MessageDriven;
import javax.jms.*;

import org.icatproject.ids.entity.DownloadRequestEntity;
import org.icatproject.ids.util.DownloadRequestHelper;
import org.icatproject.ids.util.StatusInfo;


/**
 * This is a Message Driven Bean that listens for the new download request on the
 * InfoRetrievalQueue. Upon reading a new request its gets information about the datafiles and
 * datasets from ICAT.
 */
@MessageDriven(mappedName = "jms/IDS/InfoRetrievalQueue", activationConfig = {
        @ActivationConfigProperty(propertyName = "acknowledgeMode", propertyValue = "Auto-acknowledge"),
        @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue")})
public class InfoRetrievalQueueListener implements MessageListener {

    @EJB
    private DataRetrievalQueueSender dataRetrievalQueueSender;

    @EJB
    private DownloadRequestHelper downloadRequestHelper;
    
    private final static Logger logger = Logger.getLogger(InfoRetrievalQueueListener.class.getName());
    
    public InfoRetrievalQueueListener() {}

    @Override
    public void onMessage(Message message) {
        DownloadRequestEntity downloadRequestEntity = null;
        if (message instanceof ObjectMessage) {
            try {
                Long requestId = (Long) ((ObjectMessage) message).getObject();
                downloadRequestEntity = downloadRequestHelper.getDownloadRequestById(requestId);

                if (downloadRequestEntity != null) {
                    downloadRequestHelper.processInfoRetrievalRequest(downloadRequestEntity);

                    // if all information successfully retrieved from ICAT, add request to the
                    // data retrieval queue
                    if (downloadRequestEntity.getStatus().equals(StatusInfo.INFO_RETRIVED.name())) {
                        dataRetrievalQueueSender.addDataRetrievalRequest(downloadRequestEntity);
                    }
                } else {
                    logger.log(Level.SEVERE, "Couldn not find the download request Id");
                }
            } catch (JMSException e) {
                logger.log(Level.SEVERE, "Unable to proccess the download request", e);
            } catch (MalformedURLException e) {
                logger.log(Level.SEVERE, "Unable to connect to ICAT - Bad URL", e);
            }
        }
    }
}