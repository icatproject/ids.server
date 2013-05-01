package org.icatproject.ids.webservice.exceptions;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Capture any {@link org.icatproject.ids.webservice.exceptions.WebServiceException
 * WebServiceException} thrown from {@link org.icatproject.ids.webservice.WebService
 * WebService} and generate the appropriate response code and message.
 */
@Provider
public class WebServiceExceptionMapper implements ExceptionMapper<WebServiceException> {

    @Override
    public Response toResponse(WebServiceException e) {
        return Response.status(e.getResponse()).entity(e.getMessage() + "\n").build();
    }
}