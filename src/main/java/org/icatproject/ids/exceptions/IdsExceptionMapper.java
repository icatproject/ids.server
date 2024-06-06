package org.icatproject.ids.exceptions;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import java.io.ByteArrayOutputStream;

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
        gen
            .writeStartObject()
            .write("code", e.getClass().getSimpleName())
            .write("message", e.getShortMessage());
        gen.writeEnd().close();
        return Response
            .status(e.getHttpStatusCode())
            .entity(baos.toString())
            .build();
    }
}
