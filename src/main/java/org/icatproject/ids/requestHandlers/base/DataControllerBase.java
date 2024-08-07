package org.icatproject.ids.requestHandlers.base;

import java.util.regex.Pattern;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.dataSelectionService.DataSelectionService;

import jakarta.json.stream.JsonGenerator;

public abstract class DataControllerBase {

    /**
     * matches standard UUID format of 8-4-4-4-12 hexadecimal digits
     */
    public static final Pattern uuidRegExp = Pattern
            .compile("^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$");

    public abstract DataSelectionService provideDataSelectionService(RequestType requestType) throws InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, NotImplementedException;

    public abstract void validateUUID() throws BadRequestException;

    public abstract String addParametersToLogString();

    public abstract void addParametersToTransmitterJSON(JsonGenerator gen) throws IcatException_Exception, BadRequestException;

    public abstract boolean mustZip(boolean zip, DataSelectionService dataSelectionService);

    public abstract String getOperationId();

    /**
     * returns the current sessionId. If it's null or not defined it should be created
     * @return
     * @throws InternalException 
     */
    public abstract String forceGetSessionId() throws InternalException;

    /**
     * Provides a validity check for UUIDs
     * @param thing You can give here a name of the prameter or whatever has been checked here (to provide a qualified error message if needed).
     * @param id The String which has to be checked if it is a valid UUID
     * @throws BadRequestException
     */
    public static void validateUUID(String thing, String id) throws BadRequestException {
        if (id == null || !uuidRegExp.matcher(id).matches())
            throw new BadRequestException("The " + thing + " parameter '" + id + "' is not a valid UUID");
    }

    protected String createSessionId() throws InternalException {
        String sessionId;
        try {
            sessionId = ServiceProvider.getInstance().getIcatReader().getSessionId();
        } catch (IcatException_Exception e) {
            throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
        }
        return sessionId;
    }

}
