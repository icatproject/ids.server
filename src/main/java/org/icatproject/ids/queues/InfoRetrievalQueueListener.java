package org.icatproject.ids.queues;

import java.net.MalformedURLException;

import javax.ejb.ActivationConfigProperty;
import javax.ejb.EJB;
import javax.ejb.MessageDriven;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;

import org.icatproject.ids.entity.DownloadRequestEntity;
import org.icatproject.ids.util.DownloadRequestHelper;
import org.icatproject.ids.util.StatusInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
    
    private final static Logger logger = LoggerFactory.getLogger(InfoRetrievalQueueListener.class);
    
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
                    // logger.severe("InfoRetrievalQueueListener status: " + downloadRequestEntity.getStatus()); // TODO remove
                    if (downloadRequestEntity.getStatus().equals(StatusInfo.INFO_RETRIVED.name())) {
                        dataRetrievalQueueSender.addDataRetrievalRequest(downloadRequestEntity);
                    }
                } else {
                    logger.error("Could not find the download request Id");
                }
            } catch (JMSException e) {
                logger.error("Unable to proccess the download request", e);
            } catch (MalformedURLException e) {
                logger.error("Unable to connect to ICAT - Bad URL", e);
            }
        }
    }
}