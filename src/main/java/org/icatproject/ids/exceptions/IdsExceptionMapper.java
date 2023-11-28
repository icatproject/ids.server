package org.icatproject.ids.exceptions;

import java.io.ByteArrayOutputStream;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Capture any {@link org.icatproject.ids.exceptions.IdsException WebServiceException} thrown from
 * {@link org.icatproject.ids.IdsService WebService} and generate the appropriate response code and
 * message.
 */
@Provider
public class IdsExceptionMapper implements ExceptionMapper<IdsException> {

    @Override
    public Response toResponse(IdsException e) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonGenerator gen = Json.createGenerator(baos);
        gen.writeStartObject().write("code", e.getClass().getSimpleName())
                .write("message", e.getShortMessage());
        gen.writeEnd().close();
        return Response.status(e.getHttpStatusCode()).entity(baos.toString()).build();
    }
}