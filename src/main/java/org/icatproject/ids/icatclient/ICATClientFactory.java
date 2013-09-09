package org.icatproject.ids.icatclient;

import java.net.MalformedURLException;

import org.icatproject.ids.icatclient.exceptions.ICATClientException;
import org.icatproject.ids.util.PropertyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ICATClientFactory {

    private final static Logger logger = LoggerFactory.getLogger(ICATClientFactory.class);

    private static ICATClientFactory instance = new ICATClientFactory();
    private static String url;

    private ICATClientFactory() {
        url = PropertyHandler.getInstance().getIcatURL();
    }

    public static ICATClientFactory getInstance() {
        return instance;
    }

    public ICATClientBase createICATInterface() throws MalformedURLException, ICATClientException {
        ICATClientBase icat = null;

        // Using client for ICAT 4.2 get major and minor version number of requested ICAT
        ICATClient42 tempICATClient = new ICATClient42(url);
        String version = tempICATClient.getICATVersion().substring(0, 3);

        if (version.equals("4.2")) {
            icat = new ICATClient42(url);
        } else {
            logger.error("Unsupported ICAT version : '" + tempICATClient.getICATVersion() + "'");
        }
        
        return icat;
    }
}
