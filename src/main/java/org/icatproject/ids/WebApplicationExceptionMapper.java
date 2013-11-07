package org.icatproject.ids;

import java.net.HttpURLConnection;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.icatproject.ids.exceptions.IdsExceptionMapper;
import org.icatproject.ids.exceptions.InternalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Provider
public class WebApplicationExceptionMapper implements ExceptionMapper<WebApplicationException> {

	private final static Logger logger = LoggerFactory.getLogger(IdsExceptionMapper.class);

	@Override
	public Response toResponse(WebApplicationException e) {

		logger.info("Processing: " + e.getClass() + " " + e.getMessage());

		ObjectMapper om = new ObjectMapper();
		ObjectNode error = om.createObjectNode();
		error.put("code", InternalException.class.getSimpleName());
		error.put("message", e.getClass() + " " + e.getMessage());
		return Response.status(HttpURLConnection.HTTP_INTERNAL_ERROR).entity(error.asText())
				.build();

	}
}