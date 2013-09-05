package org.icatproject.ids.webservice.exceptions;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Capture any {@link org.icatproject.ids.webservice.exceptions.WebServiceException
 * WebServiceException} thrown from {@link org.icatproject.ids.webservice.WebService
 * WebService} and generate the appropriate response code and message.
 */
@Provider
public class WebServiceExceptionMapper implements ExceptionMapper<WebServiceException> {
	
	private final static Logger logger = LoggerFactory.getLogger(WebServiceExceptionMapper.class);

    @Override
    public Response toResponse(WebServiceException e) {
    	logger.info("mapping exception " + e + " " + e.getMessage());
        return Response.status(e.getResponse()).entity(e.getMessage() + "\n").build();
    }
}