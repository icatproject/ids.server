package org.icatproject.ids.webservice.exceptions;

import java.io.FileNotFoundException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class WebApplicationExceptionMapper implements ExceptionMapper<WebApplicationException> {
	
	private final static Logger logger = LoggerFactory.getLogger(WebServiceExceptionMapper.class);

    @Override
    public Response toResponse(WebApplicationException e) {
    	logger.info("mapping exception " + e + " " + e.getMessage());
    	int responseStatus = 500;
    	if (e.getCause() instanceof FileNotFoundException) {
    		responseStatus = WebServiceException.Response.NOT_FOUND.getResponseCode();
    	}
    	else if (e.getCause() instanceof IllegalArgumentException) {
    		responseStatus = WebServiceException.Response.BAD_REQUEST.getResponseCode();
    	}
        return Response.status(responseStatus).entity(e.getCause().getMessage() + "\n").build();
    }
}