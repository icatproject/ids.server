package org.icatproject.ids;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.servlet.http.HttpServletRequest;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.fileupload.util.Streams;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
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
	@Path("getLink")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.TEXT_PLAIN)
	public String getLink(@FormParam("sessionId") String sessionId,
			@FormParam("datafileId") long datafileId, @FormParam("username") String username)
			throws BadRequestException, InsufficientPrivilegesException, NotImplementedException,
			InternalException, NotFoundException, DataNotOnlineException {
		return idsBean.getLink(sessionId, datafileId, username);
	}

	@POST
	@Path("archive")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	public void archive(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds,
			@FormParam("datasetIds") String datasetIds, @FormParam("datafileIds") String datafileIds)
			throws BadRequestException, InsufficientPrivilegesException, NotImplementedException,
			InternalException, NotFoundException {
		idsBean.archive(sessionId, investigationIds, datasetIds, datafileIds);
	}

	@DELETE
	@Path("delete")
	public void delete(@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafileIds) throws NotImplementedException,
			BadRequestException, InsufficientPrivilegesException, NotFoundException,
			InternalException, DataNotOnlineException {
		idsBean.delete(sessionId, investigationIds, datasetIds, datafileIds);
	}

	@GET
	@Path("isReadOnly")
	@Produces(MediaType.TEXT_PLAIN)
	public boolean isReadOnly() {
		return idsBean.isReadOnly();
	}

	@GET
	@Path("isTwoLevel")
	@Produces(MediaType.TEXT_PLAIN)
	public boolean isTwoLevel() {
		return idsBean.isTwoLevel();
	}

	@GET
	@Path("getApiVersion")
	@Produces(MediaType.TEXT_PLAIN)
	public String getApiVersion() {
		return Constants.API_VERSION;
	}

	@GET
	@Path("getSize")
	@Produces(MediaType.TEXT_PLAIN)
	public long getSize(@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafilesIds) throws BadRequestException,
			NotFoundException, InsufficientPrivilegesException, InternalException {
		return idsBean.getSize(sessionId, investigationIds, datasetIds, datafilesIds);
	}

	@GET
	@Path("getData")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
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
	@Produces(MediaType.TEXT_PLAIN)
	public String getStatus(@QueryParam("sessionId") String sessionId,
			@QueryParam("investigationIds") String investigationIds,
			@QueryParam("datasetIds") String datasetIds,
			@QueryParam("datafileIds") String datafilesIds) throws BadRequestException,
			NotFoundException, InsufficientPrivilegesException, InternalException {
		return idsBean.getStatus(sessionId, investigationIds, datasetIds, datafilesIds);
	}

	@GET
	@Path("isPrepared")
	@Produces(MediaType.TEXT_PLAIN)
	public boolean isPrepared(@QueryParam("preparedId") String preparedId)
			throws BadRequestException, NotFoundException, InternalException {
		return idsBean.isPrepared(preparedId);
	}

	@GET
	@Path("getServiceStatus")
	@Produces(MediaType.APPLICATION_JSON)
	public String getServiceStatus(@QueryParam("sessionId") String sessionId)
			throws InternalException, InsufficientPrivilegesException {
		return idsBean.getServiceStatus(sessionId);
	}

	@PostConstruct
	private void init() {
		logger.info("creating IdsService");
		rangeRe = Pattern.compile("bytes=(\\d+)-");
		logger.info("created IdsService");
	}

	@PreDestroy
	private void exit() {
		logger.info("destroyed IdsService");
	}

	@GET
	@Path("ping")
	@Produces(MediaType.TEXT_PLAIN)
	public String ping() {
		logger.debug("ping request received");
		return "IdsOK";
	}

	@POST
	@Path("prepareData")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces(MediaType.TEXT_PLAIN)
	public String prepareData(@FormParam("sessionId") String sessionId,
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
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	public Response put(InputStream body, @QueryParam("sessionId") String sessionId,
			@QueryParam("name") String name, @QueryParam("datafileFormatId") long datafileFormatId,
			@QueryParam("datasetId") long datasetId, @QueryParam("description") String description,
			@QueryParam("doi") String doi,
			@QueryParam("datafileCreateTime") Long datafileCreateTime,
			@QueryParam("datafileModTime") Long datafileModTime) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException,
			NotImplementedException, DataNotOnlineException {
		return idsBean.put(body, sessionId, name, datafileFormatId, datasetId, description, doi,
				datafileCreateTime, datafileModTime, false, false);
	}

	@POST
	@Path("put")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response putAsPost(@Context HttpServletRequest request) throws BadRequestException,
			NotFoundException, InternalException, InsufficientPrivilegesException,
			NotImplementedException, DataNotOnlineException {
		if (!ServletFileUpload.isMultipartContent(request)) {
			throw new BadRequestException("Multipart content expected");
		}
		try {
			ServletFileUpload upload = new ServletFileUpload();
			String sessionId = null;
			String name = null;
			long datafileFormatId = 0;
			long datasetId = 0;
			String description = null;
			String doi = null;
			Long datafileCreateTime = null;
			Long datafileModTime = null;
			Response result = null;
			boolean wrap = false;
			boolean padding = false;

			// Parse the request
			FileItemIterator iter = upload.getItemIterator(request);
			while (iter.hasNext()) {
				FileItemStream item = iter.next();
				String fieldName = item.getFieldName();
				InputStream stream = item.openStream();
				if (item.isFormField()) {
					String value = Streams.asString(stream);
					if (fieldName.equals("sessionId")) {
						sessionId = value;
					} else if (fieldName.equals("name")) {
						name = value;
					} else if (fieldName.equals("datafileFormatId")) {
						datafileFormatId = Long.parseLong(value);
					} else if (fieldName.equals("datasetId")) {
						datasetId = Long.parseLong(value);
					} else if (fieldName.equals("description")) {
						description = value;
					} else if (fieldName.equals("doi")) {
						doi = value;
					} else if (fieldName.equals("datafileCreateTime")) {
						datafileCreateTime = Long.parseLong(value);
					} else if (fieldName.equals("datafileModTime")) {
						datafileModTime = Long.parseLong(value);
					} else if (fieldName.equals("wrap")) {
						wrap = (value != null && value.toUpperCase().equals("TRUE"));
					} else if (fieldName.equals("padding")) {
						padding = (value != null && value.toUpperCase().equals("TRUE"));
					} else {
						throw new BadRequestException("Form field " + fieldName
								+ "is not recognised");
					}
				} else {
					if (name == null) {
						name = item.getName();
					}
					result = idsBean.put(stream, sessionId, name, datafileFormatId, datasetId,
							description, doi, datafileCreateTime, datafileModTime, wrap, padding);
				}
			}
			return result;
		} catch (IOException | FileUploadException e) {
			throw new InternalException(e.getClass() + " " + e.getMessage());
		}
	}

	@POST
	@Path("restore")
	@Consumes("application/x-www-form-urlencoded")
	public void restore(@FormParam("sessionId") String sessionId,
			@FormParam("investigationIds") String investigationIds,
			@FormParam("datasetIds") String datasetIds, @FormParam("datafileIds") String datafileIds)
			throws NotImplementedException, BadRequestException, InsufficientPrivilegesException,
			InternalException, NotFoundException {
		idsBean.restore(sessionId, investigationIds, datasetIds, datafileIds);
	}

}