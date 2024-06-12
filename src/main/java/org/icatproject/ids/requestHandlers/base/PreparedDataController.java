package org.icatproject.ids.requestHandlers.base;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.enums.RequestIdNames;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.models.Prepared;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.dataSelectionService.DataSelectionService;
import org.icatproject.ids.services.dataSelectionService.DataSelectionServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.json.stream.JsonGenerator;

public class PreparedDataController extends DataControllerBase {

    protected final static Logger logger = LoggerFactory
            .getLogger(RequestHandlerBase.class);

    String preparedId;

    public PreparedDataController(String preparedId) {

        this.preparedId = preparedId;
    }

    @Override
    public DataSelectionService provideDataSelectionService(
            RequestType requestType)
            throws InternalException, BadRequestException, NotFoundException,
            InsufficientPrivilegesException, NotImplementedException {

        var preparedDir = ServiceProvider.getInstance().getPropertyHandler()
                .getCacheDir().resolve("prepared");

        Prepared prepared;
        try (InputStream stream = Files
                .newInputStream(preparedDir.resolve(preparedId))) {
            prepared = Prepared.unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException(
                    "The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        return DataSelectionServiceFactory.getService(prepared, requestType);
    }

    @Override
    public void validateUUID() throws BadRequestException {
        validateUUID(RequestIdNames.preparedId, preparedId);
    }

    @Override
    public String addParametersToLogString() {
        return "preparedId = '" + preparedId;
    }

    @Override
    public void addParametersToTransmitterJSON(JsonGenerator gen)
            throws IcatException_Exception, BadRequestException {
        gen.write(RequestIdNames.preparedId, this.preparedId);
    }

    @Override
    public boolean mustZip(boolean zip,
            DataSelectionService dataSelectionService) {
        return zip;
    }

    @Override
    public String getOperationId() {
        return this.preparedId;
    }

    @Override
    public String forceGetSessionId() throws InternalException {
        return this.createSessionId();
    }

}
