package org.icatproject.ids.webservice;

import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.xml.datatype.DatatypeConfigurationException;

import org.icatproject.ids.webservice.exceptions.BadRequestException;
import org.icatproject.ids.webservice.exceptions.DataNotOnlineException;
import org.icatproject.ids.webservice.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.webservice.exceptions.InternalException;
import org.icatproject.ids.webservice.exceptions.NotFoundException;
import org.icatproject.ids.webservice.exceptions.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/")
@Stateless
public class IdsService {

	private final static Logger logger = LoggerFactory.getLogger(IdsService.class);

	@EJB
	private IdsBean idsBean;

	private Pattern rangeRe;

	@POST
	@Path("archive")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response archive(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds,
			@FormParam("datasetIds") String datasetIds, @FormParam("datafileIds") String datafileIds)
			throws BadRequestException, InsufficientPrivilegesException, NotImplementedException,
			InternalException, NotFoundException {

		return idsBean.archive(sessionId, investigationIds, datasetIds, datafileIds);
	}

	@DELETE
	@Path("delete")
	@Produces("text/plain")
	public Response delete(@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafileIds) throws NotImplementedException,
			BadRequestException, InsufficientPrivilegesException, NotFoundException,
			InternalException {

		return idsBean.delete(sessionId, investigationIds, datasetIds, datafileIds);
	}

	@GET
	@Path("getData")
	@Produces("application/octet-stream")
	public Response getData(@QueryParam("preparedId") String preparedId,
			@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafileIds,
			@QueryParam("compress") boolean compress, @QueryParam("zip") boolean zip,
			@QueryParam("outname") String outname, @HeaderParam("Range") String range)
			throws BadRequestException, NotFoundException, InternalException,
			InsufficientPrivilegesException, NotImplementedException, DataNotOnlineException {
		Response response = null;

		long offset = 0;
		if (range != null) {

			Matcher m = rangeRe.matcher(range);
			if (!m.matches()) {
				throw new BadRequestException("The range must match " + rangeRe.pattern());
			}
			offset = Long.parseLong(m.group(1));
			logger.debug("Range " + range + " -> offset " + offset);
		}

		if (preparedId != null) {
			response = idsBean.getData(preparedId, outname, offset);
		} else {
			response = idsBean.getData(sessionId, investigationIds, datasetIds, datafileIds,
					compress, zip, outname, offset);
		}
		return response;
	}

	@GET
	@Path("getStatus")
	@Produces("text/plain")
	public Response getStatus(@QueryParam("preparedId") String preparedId,
			@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafilesIds) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException,
			NotImplementedException {

		if (preparedId != null) {
			return idsBean.getStatus(preparedId);
		} else {
			return idsBean.getStatus(sessionId, investigationIds, datasetIds, datafilesIds);
		}

	}

	@PostConstruct
	private void init() throws DatatypeConfigurationException {
		logger.info("creating IdsService");
		rangeRe = Pattern.compile("bytes=(\\d+)-");
		logger.info("created IdsService");
	}

	@GET
	@Path("ping")
	@Produces("text/plain")
	public Response ping() {
		logger.debug("ping request received");
		return Response.ok("IdsOK").build();
	}

	@POST
	@Path("prepareData")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response prepareData(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds,
			@FormParam("datasetIds") String datasetIds,
			@FormParam("datafileIds") String datafileIds, @FormParam("compress") boolean compress,
			@FormParam("zip") boolean zip) throws NotImplementedException, BadRequestException,
			InsufficientPrivilegesException, NotFoundException, InternalException {

		return idsBean.prepareData(sessionId, investigationIds, datasetIds, datafileIds, compress,
				zip);

	}

	@PUT
	@Path("put")
	@Consumes("application/octet-stream")
	public Response put(InputStream body, @QueryParam("sessionId") String sessionId,
			@QueryParam("name") String name, @QueryParam("datafileFormatId") long datafileFormatId,
			@QueryParam("datasetId") long datasetId, @QueryParam("description") String description,
			@QueryParam("doi") String doi,
			@QueryParam("datafileCreateTime") Long datafileCreateTime,
			@QueryParam("datafileModTime") Long datafileModTime) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException,
			NotImplementedException, DataNotOnlineException {

		return idsBean.put(body, sessionId, name, datafileFormatId, datasetId, description, doi,
				datafileCreateTime, datafileModTime);

	}

	@POST
	@Path("restore")
	@Consumes("application/x-www-form-urlencoded")
	@Produces("text/plain")
	public Response restore(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds,
			@FormParam("datasetIds") String datasetIds, @FormParam("datafileIds") String datafileIds)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException {

		return idsBean.restore(sessionId, investigationIds, datasetIds, datafileIds);

	}

}