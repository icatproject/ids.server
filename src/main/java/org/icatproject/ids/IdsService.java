package org.icatproject.ids;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.Part;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;

@Path("/")
@Stateless
public class IdsService {

    private final static Logger logger = LoggerFactory.getLogger(IdsService.class);

    @EJB
    private IdsBean idsBean;

    private Pattern rangeRe;

    /**
     * Archive data specified by the investigationIds, datasetIds and
     * datafileIds specified along with a sessionId. If two level storage is not
     * in use this has no effect.
     *
     * @title archive
     * @param sessionId        A sessionId returned by a call to the icat server.
     * @param investigationIds If present, a comma separated list of investigation id values
     * @param datasetIds       If present, a comma separated list of data set id values or
     *                         null
     * @param datafileIds      If present, a comma separated list of datafile id values.
     * @throws NotImplementedException
     * @throws BadRequestException
     * @throws InsufficientPrivilegesException
     * @throws InternalException
     * @throws NotFoundException
     * @statuscode 200 To indicate success
     */
    @POST
    @Path("archive")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public void archive(@Context HttpServletRequest request, @FormParam("sessionId") String sessionId,
                        @FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
                        @FormParam("datafileIds") String datafileIds)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException {
        idsBean.archive(sessionId, investigationIds, datasetIds, datafileIds, request.getRemoteAddr());
    }

    /**
     * Delete data specified by the investigationIds, datasetIds and datafileIds
     * specified along with a sessionId.
     *
     * @title delete
     * @param sessionId        A sessionId returned by a call to the icat server.
     * @param investigationIds If present, a comma separated list of investigation id values
     * @param datasetIds       If present, a comma separated list of data set id values or
     *                         null
     * @param datafileIds      If present, a comma separated list of datafile id values.
     * @throws NotImplementedException
     * @throws BadRequestException
     * @throws InsufficientPrivilegesException
     * @throws NotFoundException
     * @throws InternalException
     * @throws DataNotOnlineException
     * @statuscode 200 To indicate success
     */
    @DELETE
    @Path("delete")
    public void delete(@Context HttpServletRequest request, @QueryParam("sessionId") String sessionId,
                       @QueryParam("investigationIds") String investigationIds, @QueryParam("datasetIds") String datasetIds,
                       @QueryParam("datafileIds") String datafileIds) throws NotImplementedException, BadRequestException,
            InsufficientPrivilegesException, NotFoundException, InternalException, DataNotOnlineException {
        idsBean.delete(sessionId, investigationIds, datasetIds, datafileIds, request.getRemoteAddr());
    }

    @PreDestroy
    private void exit() {
        logger.info("destroyed IdsService");
    }

    /**
     * Return the version of the server
     *
     * @title Version
     * @return json string of the form: <samp>{"version":"4.4.0"}</samp>
     */
    @GET
    @Path("version")
    @Produces(MediaType.APPLICATION_JSON)
    public String getVersion() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonGenerator gen = Json.createGenerator(baos);
        gen.writeStartObject().write("version", Constants.API_VERSION).writeEnd();
        gen.close();
        return baos.toString();
    }

    /**
     * Return data files included in the preparedId returned by a call to
     * prepareData or by the investigationIds, datasetIds and datafileIds, any
     * of which may be omitted, along with a sessionId if the preparedId is not
     * set. If preparedId is set the compress and zip arguments are not used.
     *
     * @title getData
     * @param preparedId       A valid preparedId returned by a call to prepareData
     * @param sessionId        A sessionId returned by a call to the icat server.
     * @param investigationIds A comma separated list of investigation id values.
     * @param datasetIds       A comma separated list of data set id values.
     * @param datafileIds      A comma separated list of data file id values.
     * @param compress         If true use default compression otherwise no compression. This
     *                         only applies if preparedId is not set and if the results are
     *                         being zipped.
     * @param zip              If true the data should be zipped. If multiple files are
     *                         requested (or could be because a datasetId or investigationId
     *                         has been specified) the data are zipped regardless of the
     *                         specification of this flag.
     * @param outname          The file name to put in the returned header
     *                         "ContentDisposition". If it does not end in .zip but it is a
     *                         zip file then a ".zip" will be appended.
     * @param range            A range header which must match "bytes=(\\d+)-" to specify an
     *                         offset i.e. to skip a number of bytes.
     * @return a stream of json data.
     * @throws BadRequestException
     * @throws NotFoundException
     * @throws InternalException
     * @throws InsufficientPrivilegesException
     * @throws DataNotOnlineException
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getData")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getData(@Context HttpServletRequest request, @QueryParam("preparedId") String preparedId,
                            @QueryParam("sessionId") String sessionId, @QueryParam("investigationIds") String investigationIds,
                            @QueryParam("datasetIds") String datasetIds, @QueryParam("datafileIds") String datafileIds,
                            @QueryParam("compress") boolean compress, @QueryParam("zip") boolean zip,
                            @QueryParam("outname") String outname, @HeaderParam("Range") String range) throws BadRequestException,
            NotFoundException, InternalException, InsufficientPrivilegesException, DataNotOnlineException {
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
            response = idsBean.getData(preparedId, outname, offset, request.getRemoteAddr());
        } else {
            response = idsBean.getData(sessionId, investigationIds, datasetIds, datafileIds, compress, zip, outname,
                    offset, request.getRemoteAddr());
        }
        return response;
    }

    /**
     * Return list of id values of data files included in the preparedId
     * returned by a call to prepareData or by the investigationIds, datasetIds
     * and datafileIds specified along with a sessionId if the preparedId is not
     * set.
     *
     * @title getDatafileIds
     * @param preparedId       A valid preparedId returned by a call to prepareData
     * @param sessionId        A sessionId returned by a call to the icat server.
     * @param investigationIds A comma separated list of investigation id values.
     * @param datasetIds       A comma separated list of data set id values.
     * @param datafileIds      A comma separated list of datafile id values.
     * @return a list of id values
     * @throws BadRequestException
     * @throws InternalException
     * @throws NotFoundException
     * @throws InsufficientPrivilegesException
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getDatafileIds")
    @Produces(MediaType.APPLICATION_JSON)
    public String getDatafileIds(@Context HttpServletRequest request, @QueryParam("preparedId") String preparedId,
                                 @QueryParam("sessionId") String sessionId, @QueryParam("investigationIds") String investigationIds,
                                 @QueryParam("datasetIds") String datasetIds, @QueryParam("datafileIds") String datafileIds)
            throws BadRequestException, InternalException, NotFoundException, InsufficientPrivilegesException {
        if (preparedId != null) {
            return idsBean.getDatafileIds(preparedId, request.getRemoteAddr());
        } else {
            return idsBean.getDatafileIds(sessionId, investigationIds, datasetIds, datafileIds,
                    request.getRemoteAddr());
        }
    }

    /**
     * Return the url of the icat.server that this ids.server has been
     * configured to use. This is the icat.server from which a sessionId must be
     * obtained.
     *
     * @return the url of the icat server
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getIcatUrl")
    @Produces(MediaType.TEXT_PLAIN)
    public String getIcatUrl(@Context HttpServletRequest request) {
        return idsBean.getIcatUrl(request.getRemoteAddr());
    }

    /**
     * Return a hard link to a data file.
     * <p>
     * This is only useful in those cases where the user has direct access to
     * the file system where the IDS is storing data. Only read access to the
     * file is granted.
     *
     * @title getLink
     * @param sessionId  A valid ICAT session ID
     * @param datafileId the id of a data file
     * @param username   the name of the user who will will be granted access to the
     *                   linked file.
     * @return the path of the created link.
     * @throws BadRequestException
     * @throws InsufficientPrivilegesException
     * @throws NotImplementedException
     * @throws InternalException
     * @throws NotFoundException
     * @throws DataNotOnlineException
     * @statuscode 200 To indicate success
     */
    @POST
    @Path("getLink")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    @Deprecated
    public String getLink(@Context HttpServletRequest request, @FormParam("sessionId") String sessionId,
                          @FormParam("datafileId") long datafileId, @FormParam("username") String username)
            throws BadRequestException, InsufficientPrivilegesException, NotImplementedException, InternalException,
            NotFoundException, DataNotOnlineException {
        return idsBean.getLink(sessionId, datafileId, username, request.getRemoteAddr());
    }

    /**
     * Obtain detailed information about what the ids is doing. You need to be
     * privileged to use this call.
     *
     * @title getServiceStatus
     * @param sessionId A valid ICAT session ID of a user in the IDS rootUserNames
     *                  set.
     * @return a json string.
     * @throws InternalException
     * @throws InsufficientPrivilegesException
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getServiceStatus")
    @Produces(MediaType.APPLICATION_JSON)
    public String getServiceStatus(@Context HttpServletRequest request, @QueryParam("sessionId") String sessionId)
            throws InternalException, InsufficientPrivilegesException {
        return idsBean.getServiceStatus(sessionId, request.getRemoteAddr());
    }

    /**
     * Return the total size of all the data files specified by the
     * investigationIds, datasetIds and datafileIds along with a sessionId.
     *
     * @title getSize
     * @param preparedId       A valid preparedId returned by a call to prepareData
     * @param sessionId        A sessionId returned by a call to the icat server.
     * @param investigationIds If present, a comma separated list of investigation id values
     * @param datasetIds       If present, a comma separated list of data set id values or
     *                         null
     * @param datafileIds      If present, a comma separated list of data file id values.
     * @return the size in bytes
     * @throws BadRequestException
     * @throws NotFoundException
     * @throws InsufficientPrivilegesException
     * @throws InternalException
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getSize")
    @Produces(MediaType.TEXT_PLAIN)
    public long getSize(@Context HttpServletRequest request, @QueryParam("preparedId") String preparedId,
                        @QueryParam("sessionId") String sessionId, @QueryParam("investigationIds") String investigationIds,
                        @QueryParam("datasetIds") String datasetIds, @QueryParam("datafileIds") String datafileIds)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {
        if (preparedId != null) {
            return idsBean.getSize(preparedId, request.getRemoteAddr());
        } else {
            return idsBean.getSize(sessionId, investigationIds, datasetIds, datafileIds, request.getRemoteAddr());
        }
    }

    /**
     * Return the archive status of the data files specified by the
     * investigationIds, datasetIds and datafileIds along with a sessionId.
     *
     * @title getStatus
     * @param preparedId       A valid preparedId returned by a call to prepareData
     * @param sessionId        A sessionId returned by a call to the icat server. If the
     *                         sessionId is omitted or null the ids reader account will be
     *                         used which has read access to all data.
     * @param investigationIds If present, a comma separated list of investigation id values
     * @param datasetIds       If present, a comma separated list of data set id values or
     *                         null
     * @param datafileIds      If present, a comma separated list of data file id values.
     * @return a string with "ONLINE" if all data are online, "RESTORING" if one
     * or more files are in the process of being restored but none are
     * archived and no restoration has been requested or "ARCHIVED" if
     * one or more files are archived and and no restoration has been
     * requested.
     * @throws BadRequestException
     * @throws NotFoundException
     * @throws InsufficientPrivilegesException
     * @throws InternalException
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getStatus")
    @Produces(MediaType.TEXT_PLAIN)
    public String getStatus(@Context HttpServletRequest request, @QueryParam("preparedId") String preparedId,
                            @QueryParam("sessionId") String sessionId, @QueryParam("investigationIds") String investigationIds,
                            @QueryParam("datasetIds") String datasetIds, @QueryParam("datafileIds") String datafileIds)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException {
        if (preparedId != null) {
            return idsBean.getStatus(preparedId, request.getRemoteAddr());
        } else {
            return idsBean.getStatus(sessionId, investigationIds, datasetIds, datafileIds, request.getRemoteAddr());
        }
    }

    @PostConstruct
    private void init() {
        logger.info("creating IdsService");
        rangeRe = Pattern.compile("bytes=(\\d+)-");
        logger.info("created IdsService");
    }

    /**
     * Returns true if all the data files are ready to be downloaded. As a side
     * effect, if any data files are archived and no restoration has been
     * requested then a restoration of those data files will be launched.
     * <p>
     * To make this operation fast, if anything is not found on line then the
     * rest of the work is done in the background and the status returned. At a
     * subsequent call it will restart from where it failed and if it gets to
     * the end successfully will then go through all those it did not look at
     * because it did not start at the beginning.
     *
     * @title isPrepared
     * @param preparedId A valid preparedId returned by a call to prepareData
     * @return true if all the data files are ready to be downloaded else false.
     * @throws BadRequestException
     * @throws NotFoundException
     * @throws InternalException
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("isPrepared")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean isPrepared(@Context HttpServletRequest request, @QueryParam("preparedId") String preparedId)
            throws BadRequestException, NotFoundException, InternalException {
        return idsBean.isPrepared(preparedId, request.getRemoteAddr());
    }

    /**
     * An ids server can be configured to be read only. This returns the
     * readOnly status of the server.
     *
     * @title isReadOnly
     * @return true if readonly, else false
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("isReadOnly")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean isReadOnly(@Context HttpServletRequest request) {
        return idsBean.isReadOnly(request.getRemoteAddr());
    }

    /**
     * An ids server can be configured to support one or two levels of data
     * storage. This returns the twoLevel status of the server.
     *
     * @title isTwoLevel
     * @return true if twoLevel, else false
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("isTwoLevel")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean isTwoLevel(@Context HttpServletRequest request) {
        return idsBean.isTwoLevel(request.getRemoteAddr());
    }

    /**
     * Should return "IdsOK"
     *
     * @title ping
     * @return "IdsOK"
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("ping")
    @Produces(MediaType.TEXT_PLAIN)
    public String ping() {
        logger.debug("ping request received");
        return "IdsOK";
    }

    /**
     * Prepare data files for subsequent download. For single level storage the
     * only benefit of this call is that the returned preparedId may be shared
     * with others to provide access to the data. The data files are specified
     * by the investigationIds, datasetIds and datafileIds, any of which may be
     * omitted, along with a sessionId.
     *
     * @title prepareData
     * @param sessionId        A sessionId returned by a call to the icat server.
     * @param investigationIds A comma separated list of investigation id values.
     * @param datasetIds       A comma separated list of data set id values.
     * @param datafileIds      A comma separated list of datafile id values.
     * @param compress         If true use default compression otherwise no compression. This
     *                         only applies if preparedId is not set and if the results are
     *                         being zipped.
     * @param zip              If true the data should be zipped. If multiple files are
     *                         requested (or could be because a datasetId or investigationId
     *                         has been specified) the data are zipped regardless of the
     *                         specification of this flag.
     * @return a string with the preparedId
     * @throws BadRequestException
     * @throws InsufficientPrivilegesException
     * @throws NotFoundException
     * @throws InternalException
     * @statuscode 200 To indicate success
     */
    @POST
    @Path("prepareData")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    public String prepareData(@Context HttpServletRequest request, @FormParam("sessionId") String sessionId,
                              @FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
                              @FormParam("datafileIds") String datafileIds, @FormParam("compress") boolean compress,
                              @FormParam("zip") boolean zip)
            throws BadRequestException, InsufficientPrivilegesException, NotFoundException, InternalException {
        return idsBean.prepareData(sessionId, investigationIds, datasetIds, datafileIds, compress, zip,
                request.getRemoteAddr());
    }

    /**
     * Stores a data file
     *
     * @title put
     * @param body               The contents of the file to be stored
     * @param sessionId          A sessionId returned by a call to the icat server.
     * @param name               A name to assign to the data file
     * @param datafileFormatId   The id of the data file format to associate with the data file
     * @param datasetId          The id of the data set to which the data file should be
     *                           associated.
     * @param description        An optional description to associate with the data file
     * @param doi                An optional description to associate with the data file
     * @param datafileCreateTime An optional datafileCreateTime to associate with the data file
     * @param datafileModTime    An optional datafileModTime to associate with the data file
     * @return a json object with attributes of "id", "checksum", "location" and
     * "size";
     * @throws BadRequestException
     * @throws NotFoundException
     * @throws InternalException
     * @throws InsufficientPrivilegesException
     * @throws NotImplementedException
     * @throws DataNotOnlineException
     * @statuscode 201 When object successfully created
     */
    @PUT
    @Path("put")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response put(@Context HttpServletRequest request, InputStream body,
                        @QueryParam("sessionId") String sessionId, @QueryParam("name") String name,
                        @QueryParam("datafileFormatId") String datafileFormatId, @QueryParam("datasetId") String datasetId,
                        @QueryParam("description") String description, @QueryParam("doi") String doi,
                        @QueryParam("datafileCreateTime") String datafileCreateTime,
                        @QueryParam("datafileModTime") String datafileModTime) throws BadRequestException, NotFoundException,
            InternalException, InsufficientPrivilegesException, NotImplementedException, DataNotOnlineException {
        return idsBean.put(body, sessionId, name, datafileFormatId, datasetId, description, doi, datafileCreateTime,
                datafileModTime, request.getRemoteAddr());
    }

    /**
     * Reset a particular set of datafiles or datasets so that they can be
     * queried again.
     * <p>
     * If secondary storage is in use restores may fail and cause a flag to be
     * set so that isPrepared and getStatus calls will throw an exception. This
     * call clears the flag.
     * <p>
     * You must either specify a preparedId or a sessionId. If preparedId is
     * specified then investigationIds, datasetIds and datafileIds are not used.
     *
     * @title reset
     * @param preparedId       A valid preparedId returned by a call to prepareData
     * @param sessionId        A sessionId returned by a call to the icat server.
     * @param investigationIds A comma separated list of investigation id values.
     * @param datasetIds       A comma separated list of data set id values.
     * @param datafileIds      A comma separated list of data file id values.
     * @return a stream of json data.
     * @throws BadRequestException
     * @throws NotFoundException
     * @throws InternalException
     * @throws InsufficientPrivilegesException
     * @statuscode 200 To indicate success
     */
    @POST
    @Path("reset")
    public void reset(@Context HttpServletRequest request, @FormParam("preparedId") String preparedId,
                      @FormParam("sessionId") String sessionId, @FormParam("investigationIds") String investigationIds,
                      @FormParam("datasetIds") String datasetIds, @FormParam("datafileIds") String datafileIds)
            throws BadRequestException, InternalException, NotFoundException, InsufficientPrivilegesException {
        if (preparedId != null) {
            idsBean.reset(preparedId, request.getRemoteAddr());
        } else {
            idsBean.reset(sessionId, investigationIds, datasetIds, datafileIds, request.getRemoteAddr());
        }

    }

    /**
     * Restore data specified by the investigationIds, datasetIds and
     * datafileIds specified along with a sessionId. If two level storage is not
     * in use this has no effect.
     *
     * @title restore
     * @param sessionId        A sessionId returned by a call to the icat server.
     * @param investigationIds If present, a comma separated list of investigation id values
     * @param datasetIds       If present, a comma separated list of data set id values or
     *                         null
     * @param datafileIds      If present, a comma separated list of datafile id values.
     * @throws NotImplementedException
     * @throws BadRequestException
     * @throws InsufficientPrivilegesException
     * @throws InternalException
     * @throws NotFoundException
     * @statuscode 200 To indicate success
     */
    @POST
    @Path("restore")
    @Consumes("application/x-www-form-urlencoded")
    public void restore(@Context HttpServletRequest request, @FormParam("sessionId") String sessionId,
                        @FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
                        @FormParam("datafileIds") String datafileIds)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException {
        idsBean.restore(sessionId, investigationIds, datasetIds, datafileIds, request.getRemoteAddr());
    }


    /**
     * Write data specified by the investigationIds, datasetIds
     * and datafileIds specified along with a sessionId to archive
     * storage. If two level storage is not in use this has no
     * effect.
     *
     * @title write
     * @param sessionId        A sessionId returned by a call to the icat server.
     * @param investigationIds If present, a comma separated list of investigation id values
     * @param datasetIds       If present, a comma separated list of data set id values or
     *                         null
     * @param datafileIds      If present, a comma separated list of datafile id values.
     * @throws NotImplementedException
     * @throws BadRequestException
     * @throws InsufficientPrivilegesException
     * @throws InternalException
     * @throws NotFoundException
     * @statuscode 200 To indicate success
     */
    @POST
    @Path("write")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public void write(@Context HttpServletRequest request, @FormParam("sessionId") String sessionId,
                      @FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
                      @FormParam("datafileIds") String datafileIds)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException, DataNotOnlineException {
        idsBean.write(sessionId, investigationIds, datasetIds, datafileIds, request.getRemoteAddr());
    }
}
