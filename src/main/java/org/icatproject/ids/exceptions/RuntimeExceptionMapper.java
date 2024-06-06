package org.icatproject.ids.exceptions;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class RuntimeExceptionMapper implements ExceptionMapper<RuntimeException> {

    private final static Logger logger = LoggerFactory.getLogger(RuntimeExceptionMapper.class);

    @Override
    public Response toResponse(RuntimeException e) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        e.printStackTrace(new PrintStream(baos));
        logger.error("Processing: " + baos.toString());
        baos.reset();
        JsonGenerator gen = Json.createGenerator(baos);
        gen.writeStartObject().write("code", "InternalException")
                .write("message", e.getClass() + " " + e.getMessage()).writeEnd().close();
        return Response.status(HttpURLConnection.HTTP_INTERNAL_ERROR).entity(baos.toString())
                .build();
    }
}
