package org.icatproject.ids.icatclient;

import java.net.MalformedURLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.icatproject.ids.icatclient.ICATClient42;
import org.icatproject.ids.icatclient.ICATClientBase;
import org.icatproject.ids.icatclient.exceptions.ICATClientException;
import org.icatproject.ids.util.PropertyHandler;


public class ICATClientFactory {

    private final static Logger logger = Logger.getLogger(ICATClientFactory.class.getName());

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
            logger.log(Level.SEVERE,
                    "Unsupported ICAT version : '" + tempICATClient.getICATVersion() + "'");
        }
        
        return icat;
    }
}
