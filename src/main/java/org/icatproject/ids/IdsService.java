package org.icatproject.ids;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

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
import org.icatproject.IcatException_Exception;
import org.icatproject.ids.enums.RequestIdNames;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.finiteStateMachine.FiniteStateMachine;
import org.icatproject.ids.helpers.Constants;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.requestHandlers.ArchiveHandler;
import org.icatproject.ids.requestHandlers.DeleteHandler;
import org.icatproject.ids.requestHandlers.GetDataFileIdsHandler;
import org.icatproject.ids.requestHandlers.GetDataHandler;
import org.icatproject.ids.requestHandlers.GetIcatUrlHandler;
import org.icatproject.ids.requestHandlers.GetServiceStatusHandler;
import org.icatproject.ids.requestHandlers.GetStatusHandler;
import org.icatproject.ids.requestHandlers.IsPreparedHandler;
import org.icatproject.ids.requestHandlers.IsReadOnlyHandler;
import org.icatproject.ids.requestHandlers.IsTwoLevelHandler;
import org.icatproject.ids.requestHandlers.PrepareDataHandler;
import org.icatproject.ids.requestHandlers.PutHandler;
import org.icatproject.ids.requestHandlers.ResetHandler;
import org.icatproject.ids.requestHandlers.RestoreHandler;
import org.icatproject.ids.requestHandlers.WriteHandler;
import org.icatproject.ids.requestHandlers.getSizeHandlers.GetSizeHandler;
import org.icatproject.ids.requestHandlers.getSizeHandlers.GetSizeHandlerForFastProcessing;
import org.icatproject.ids.services.IcatReader;
import org.icatproject.ids.services.LockManager;
import org.icatproject.ids.services.PropertyHandler;
import org.icatproject.ids.services.RequestHandlerService;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.Transmitter;

@Path("/")
@Stateless
public class IdsService {

    private final static Logger logger = LoggerFactory.getLogger(IdsService.class);

    @EJB
    Transmitter transmitter;

    @EJB
    private LockManager lockManager;

    @EJB
    private IcatReader reader;

    @EJB
    private RequestHandlerService requestService;

    private FiniteStateMachine fsm = null;

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
     * @throws DataNotOnlineException 
     * @statuscode 200 To indicate success
     */
    @POST
    @Path("archive")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public void archive(@Context HttpServletRequest request, @FormParam(RequestIdNames.sessionId) String sessionId,
                        @FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
                        @FormParam("datafileIds") String datafileIds)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException, DataNotOnlineException {

        var handler = new ArchiveHandler(request.getRemoteAddr(), sessionId, investigationIds, datasetIds, datafileIds);
        handler.handle();
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
    public void delete(@Context HttpServletRequest request, @QueryParam(RequestIdNames.sessionId) String sessionId,
                       @QueryParam("investigationIds") String investigationIds, @QueryParam("datasetIds") String datasetIds,
                       @QueryParam("datafileIds") String datafileIds) throws NotImplementedException, BadRequestException,
            InsufficientPrivilegesException, NotFoundException, InternalException, DataNotOnlineException {

        var handler = new DeleteHandler(request.getRemoteAddr(), sessionId, investigationIds, datasetIds, datafileIds);
        handler.handle();
    }

    @PreDestroy
    private void exit() {
        this.fsm.exit();
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
     * @throws NotImplementedException 
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getData")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getData(@Context HttpServletRequest request, @QueryParam(RequestIdNames.preparedId) String preparedId,
                            @QueryParam(RequestIdNames.sessionId) String sessionId, @QueryParam("investigationIds") String investigationIds,
                            @QueryParam("datasetIds") String datasetIds, @QueryParam("datafileIds") String datafileIds,
                            @QueryParam("compress") boolean compress, @QueryParam("zip") boolean zip,
                            @QueryParam("outname") String outname, @HeaderParam("Range") String range) throws BadRequestException,
            NotFoundException, InternalException, InsufficientPrivilegesException, DataNotOnlineException, NotImplementedException {

        var handler = new GetDataHandler(request.getRemoteAddr(), preparedId, sessionId, investigationIds, datasetIds, datafileIds, compress, zip,  outname, range);
        return handler.handle().getResponse();
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
     * @throws NotImplementedException 
     * @throws DataNotOnlineException 
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getDatafileIds")
    @Produces(MediaType.APPLICATION_JSON)
    public String getDatafileIds(@Context HttpServletRequest request, @QueryParam(RequestIdNames.preparedId) String preparedId,
                                 @QueryParam(RequestIdNames.sessionId) String sessionId, @QueryParam("investigationIds") String investigationIds,
                                 @QueryParam("datasetIds") String datasetIds, @QueryParam("datafileIds") String datafileIds)
            throws BadRequestException, InternalException, NotFoundException, InsufficientPrivilegesException, DataNotOnlineException, NotImplementedException {

        var handler = new GetDataFileIdsHandler(request.getRemoteAddr(), preparedId, sessionId, investigationIds, datasetIds, datafileIds);
        return handler.handle().getString();
    }

    /**
     * Return the url of the icat.server that this ids.server has been
     * configured to use. This is the icat.server from which a sessionId must be
     * obtained.
     *
     * @return the url of the icat server
     * @throws NotImplementedException 
     * @throws DataNotOnlineException 
     * @throws NotFoundException 
     * @throws InsufficientPrivilegesException 
     * @throws BadRequestException 
     * @throws InternalException 
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getIcatUrl")
    @Produces(MediaType.TEXT_PLAIN)
    public String getIcatUrl(@Context HttpServletRequest request) throws InternalException, BadRequestException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException {

        var handler = new GetIcatUrlHandler(request.getRemoteAddr());
        return handler.handle().getString();
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
     * @throws NotImplementedException 
     * @throws DataNotOnlineException 
     * @throws NotFoundException 
     * @throws BadRequestException 
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getServiceStatus")
    @Produces(MediaType.APPLICATION_JSON)
    public String getServiceStatus(@Context HttpServletRequest request, @QueryParam(RequestIdNames.sessionId) String sessionId)
            throws InternalException, InsufficientPrivilegesException, BadRequestException, NotFoundException, DataNotOnlineException, NotImplementedException {

        var handler = new GetServiceStatusHandler(request.getRemoteAddr(), sessionId);
        return handler.handle().getString();
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
     * @throws NotImplementedException 
     * @throws DataNotOnlineException 
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getSize")
    @Produces(MediaType.TEXT_PLAIN)
    public long getSize(@Context HttpServletRequest request, @QueryParam(RequestIdNames.preparedId) String preparedId,
                        @QueryParam(RequestIdNames.sessionId) String sessionId, @QueryParam("investigationIds") String investigationIds,
                        @QueryParam("datasetIds") String datasetIds, @QueryParam("datafileIds") String datafileIds)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException, DataNotOnlineException, NotImplementedException {


        var result = ValueContainer.getInvalid();

        // trying fast computation
        if(sessionId != null) {
            var fastHandler = new GetSizeHandlerForFastProcessing(request.getRemoteAddr(), sessionId, investigationIds, datasetIds, datafileIds);
            result = fastHandler.handle();
        }

        // otherwise normal computation
        if(result.isInvalid()) {
            var handler = new GetSizeHandler(request.getRemoteAddr(), preparedId, sessionId, investigationIds, datasetIds, datafileIds);
            result = handler.handle();
        }

        return result.getLong();
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
     * @throws NotImplementedException 
     * @throws DataNotOnlineException 
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("getStatus")
    @Produces(MediaType.TEXT_PLAIN)
    public String getStatus(@Context HttpServletRequest request, @QueryParam(RequestIdNames.preparedId) String preparedId,
                            @QueryParam(RequestIdNames.sessionId) String sessionId, @QueryParam("investigationIds") String investigationIds,
                            @QueryParam("datasetIds") String datasetIds, @QueryParam("datafileIds") String datafileIds)
            throws BadRequestException, NotFoundException, InsufficientPrivilegesException, InternalException, DataNotOnlineException, NotImplementedException {

        // special case for getStatus request: getting status is possible without authentification
        if (sessionId == null && preparedId == null) {
            try {
                sessionId = ServiceProvider.getInstance().getIcatReader().getSessionId();
            } catch (IcatException_Exception e) {
                throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
            }
        }

        var handler = new GetStatusHandler(request.getRemoteAddr(), preparedId, sessionId, investigationIds, datasetIds, datafileIds);
        return handler.handle().getString();
    }

    @PostConstruct
    private void init() {
        logger.info("creating IdsService");

        FiniteStateMachine.createInstance(reader, lockManager, PropertyHandler.getInstance().getStorageUnit());
        this.fsm = FiniteStateMachine.getInstance();
        this.fsm.init();
        ServiceProvider.createInstance(transmitter, fsm, lockManager, reader);

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
     * @throws NotImplementedException 
     * @throws DataNotOnlineException 
     * @throws InsufficientPrivilegesException 
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("isPrepared")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean isPrepared(@Context HttpServletRequest request, @QueryParam(RequestIdNames.preparedId) String preparedId)
            throws BadRequestException, NotFoundException, InternalException, InsufficientPrivilegesException, DataNotOnlineException, NotImplementedException {

        var handler = new IsPreparedHandler(request.getRemoteAddr(), preparedId);
        return handler.handle().getBool();
    }

    /**
     * An ids server can be configured to be read only. This returns thenew DfProcessQueue()
     * readOnly status of the server.
     *
     * @title isReadOnly
     * @return true if readonly, else false
     * @throws NotImplementedException 
     * @throws DataNotOnlineException 
     * @throws NotFoundException 
     * @throws InsufficientPrivilegesException 
     * @throws BadRequestException 
     * @throws InternalException 
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("isReadOnly")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean isReadOnly(@Context HttpServletRequest request) throws InternalException, BadRequestException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException {

        var handler = new IsReadOnlyHandler(request.getRemoteAddr());
        return handler.handle().getBool();
    }

    /**
     * An ids server can be configured to support one or two levels of data
     * storage. This returns the twoLevel status of the server.
     *
     * @title isTwoLevel
     * @return true if twoLevel, else false
     * @throws NotImplementedException 
     * @throws DataNotOnlineException 
     * @throws NotFoundException 
     * @throws InsufficientPrivilegesException 
     * @throws BadRequestException 
     * @throws InternalException 
     * @statuscode 200 To indicate success
     */
    @GET
    @Path("isTwoLevel")
    @Produces(MediaType.TEXT_PLAIN)
    public boolean isTwoLevel(@Context HttpServletRequest request) throws InternalException, BadRequestException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException {

        var handler = new IsTwoLevelHandler(request.getRemoteAddr());
        return handler.handle().getBool();
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
     * @throws NotImplementedException 
     * @throws DataNotOnlineException 
     * @statuscode 200 To indicate success
     */
    @POST
    @Path("prepareData")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    public String prepareData(@Context HttpServletRequest request, @FormParam(RequestIdNames.sessionId) String sessionId,
                              @FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
                              @FormParam("datafileIds") String datafileIds, @FormParam("compress") boolean compress,
                              @FormParam("zip") boolean zip)
            throws BadRequestException, InsufficientPrivilegesException, NotFoundException, InternalException, NotImplementedException, DataNotOnlineException {

        var handler = new PrepareDataHandler(request.getRemoteAddr(), sessionId, investigationIds, datasetIds, datafileIds, compress, zip);
        return handler.handle().getString();
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
                        @QueryParam(RequestIdNames.sessionId) String sessionId, @QueryParam("name") String name,
                        @QueryParam("datafileFormatId") String datafileFormatId, @QueryParam("datasetId") String datasetId,
                        @QueryParam("description") String description, @QueryParam("doi") String doi,
                        @QueryParam("datafileCreateTime") String datafileCreateTime,
                        @QueryParam("datafileModTime") String datafileModTime) throws BadRequestException, NotFoundException,
            InternalException, InsufficientPrivilegesException, NotImplementedException, DataNotOnlineException {

        var handler = new PutHandler(request.getRemoteAddr(), sessionId, body, name, datafileFormatId, datasetId, 
                                description, doi, datafileCreateTime, datafileModTime, false, false);
        return handler.handle().getResponse();
    }

    /**
     * This is an alternative to using PUT on the put resource. All the same
     * arguments appear as form fields. In addition there are two boolean fields
     * wrap and padding which should be set to true as a CORS work around. These
     * two fields will be removed shortly as they are only required by the old
     * (GWT based) topcat.
     *
     * @title putAsPost
     * @param request
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
    @POST
    @Path("put")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response putAsPost(@Context HttpServletRequest request) throws BadRequestException, NotFoundException,
            InternalException, InsufficientPrivilegesException, NotImplementedException, DataNotOnlineException {
                try {
                    String sessionId = null;
                    String name = null;
                    String datafileFormatId = null;
                    String datasetId = null;
                    String description = null;
                    String doi = null;
                    String datafileCreateTime = null;
                    String datafileModTime = null;
                    Response result = null;
                    boolean wrap = false;
                    boolean padding = false;
        
                    // Parse the request
                    for (Part part : request.getParts()) {
                        String fieldName = part.getName();
                        InputStream stream = part.getInputStream();
                        if (part.getSubmittedFileName() == null) {
                            String value = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
                            if (fieldName.equals("sessionId")) {
                                sessionId = value;
                            } else if (fieldName.equals("name")) {
                                name = value;
                            } else if (fieldName.equals("datafileFormatId")) {
                                datafileFormatId = value;
                            } else if (fieldName.equals("datasetId")) {
                                datasetId = value;
                            } else if (fieldName.equals("description")) {
                                description = value;
                            } else if (fieldName.equals("doi")) {
                                doi = value;
                            } else if (fieldName.equals("datafileCreateTime")) {
                                datafileCreateTime = value;
                            } else if (fieldName.equals("datafileModTime")) {
                                datafileModTime = value;
                            } else if (fieldName.equals("wrap")) {
                                wrap = (value != null && value.toUpperCase().equals("TRUE"));
                            } else if (fieldName.equals("padding")) {
                                padding = (value != null && value.toUpperCase().equals("TRUE"));
                            } else {
                                throw new BadRequestException("Form field " + fieldName + "is not recognised");
                            }
                        } else {
                            if (name == null) {
                                name = part.getSubmittedFileName();
                            }
                            var handler = new PutHandler(request.getRemoteAddr(), sessionId, stream, name, datafileFormatId, datasetId, description, 
                                            doi, datafileCreateTime, datafileModTime, wrap, padding);
                            result = handler.handle().getResponse();
                        }
                    }
                    return result;
                } catch (IOException e) {
                    throw new InternalException(e.getClass() + " " + e.getMessage());
                } catch (ServletException e) {
                    throw new BadRequestException("Multipart content expected");
                }
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
     * @throws NotImplementedException 
     * @throws DataNotOnlineException 
     * @statuscode 200 To indicate success
     */
    @POST
    @Path("reset")
    public void reset(@Context HttpServletRequest request, @FormParam(RequestIdNames.preparedId) String preparedId,
                      @FormParam(RequestIdNames.sessionId) String sessionId, @FormParam("investigationIds") String investigationIds,
                      @FormParam("datasetIds") String datasetIds, @FormParam("datafileIds") String datafileIds)
            throws BadRequestException, InternalException, NotFoundException, InsufficientPrivilegesException, DataNotOnlineException, NotImplementedException {

        var handler = new ResetHandler(request.getRemoteAddr(), preparedId, sessionId, investigationIds, datasetIds, datafileIds);
        handler.handle();

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
     * @throws DataNotOnlineException 
     * @statuscode 200 To indicate success
     */
    @POST
    @Path("restore")
    @Consumes("application/x-www-form-urlencoded")
    public void restore(@Context HttpServletRequest request, @FormParam(RequestIdNames.sessionId) String sessionId,
                        @FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
                        @FormParam("datafileIds") String datafileIds)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException, DataNotOnlineException {

        var handler = new RestoreHandler(request.getRemoteAddr(), sessionId, investigationIds, datasetIds, datafileIds);
        handler.handle();
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
    public void write(@Context HttpServletRequest request, @FormParam(RequestIdNames.sessionId) String sessionId,
                      @FormParam("investigationIds") String investigationIds, @FormParam("datasetIds") String datasetIds,
                      @FormParam("datafileIds") String datafileIds)
            throws NotImplementedException, BadRequestException, InsufficientPrivilegesException, InternalException,
            NotFoundException, DataNotOnlineException {

        var handler = new WriteHandler(request.getRemoteAddr(), sessionId, investigationIds, datasetIds, datafileIds);
        handler.handle();
    }
}
