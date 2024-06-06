package org.icatproject.ids.services;

import jakarta.xml.ws.WebServiceException;
import java.net.MalformedURLException;
import java.net.URL;
import org.icatproject.ICAT;
import org.icatproject.ICATService;
import org.icatproject.IcatException;
import org.icatproject.IcatExceptionType;
import org.icatproject.IcatException_Exception;

/**
 * To find the ICAT SOAP web service irrespective of the container in use. It
 * does this by trial and error.
 */
public class ICATGetter {

    private static String[] suffices = new String[] {
        "ICATService/ICAT?wsdl",
        "icat/ICAT?wsdl",
    };

    /**
     * Provide access to an ICAT SOAP web service with the basic URL string
     * provided. This exists to hide the differences between containers.
     *
     * @param urlString the url of the machine to be contacted. If the url has a
     *                  non-empty file part it is used unchanged, otherwise suffices
     *                  are tried suitable for Glassfish and WildFly.
     * @return an ICAT
     * @throws IcatException_Exception if something is wrong
     */
    public static ICAT getService(String urlString)
        throws IcatException_Exception {
        if (urlString == null) {
            throwSessionException("Argument to constructor must not be null");
        }

        boolean emptyFile = false;
        try {
            emptyFile = new URL(urlString).getFile().isEmpty();
        } catch (MalformedURLException e) {
            throwSessionException("Invalid URL: " + urlString);
        }

        ICAT icatService;
        if (emptyFile) {
            for (String suffix : suffices) {
                String icatUrlWsdl = urlString + "/" + suffix;
                try {
                    icatService =
                        new ICATService(new URL(icatUrlWsdl)).getICATPort();
                    icatService.getApiVersion();
                    return icatService;
                } catch (MalformedURLException e) {
                    throwSessionException("Invalid URL");
                } catch (WebServiceException e) {
                    Throwable cause = e.getCause();
                    if (
                        cause != null && cause.getMessage().contains("security")
                    ) {
                        throwSessionException(cause.getMessage());
                    }
                } catch (Exception e) {
                    throwSessionException(e.getMessage());
                }
            }
        } else {
            try {
                icatService = new ICATService(new URL(urlString)).getICATPort();
                icatService.getApiVersion();
                return icatService;
            } catch (MalformedURLException e) {
                throwSessionException("Invalid URL: " + urlString);
            } catch (WebServiceException e) {
                Throwable cause = e.getCause();
                if (cause != null && cause.getMessage().contains("security")) {
                    throwSessionException(cause.getMessage());
                }
            } catch (Exception e) {
                throwSessionException(e.getMessage());
            }
        }
        throwSessionException("Unable to connect to: " + urlString);
        return null; // To please the compiler
    }

    public static String getCleanUrl(String urlString) {
        for (String suffix : suffices) {
            suffix = "/" + suffix;
            if (urlString.endsWith(suffix)) {
                return urlString.substring(
                    0,
                    urlString.length() - suffix.length()
                );
            }
        }
        if (urlString.endsWith("/")) {
            return urlString.substring(0, urlString.length() - 1);
        }
        return urlString;
    }

    private static void throwSessionException(String msg)
        throws IcatException_Exception {
        IcatException except = new IcatException();
        except.setMessage(msg);
        except.setType(IcatExceptionType.SESSION);
        throw new IcatException_Exception(msg, new IcatException());
    }
}
