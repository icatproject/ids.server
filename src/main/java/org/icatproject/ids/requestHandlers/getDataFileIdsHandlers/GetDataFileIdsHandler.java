package org.icatproject.ids.requestHandlers.getDataFileIdsHandlers;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.PreparedDataStatus;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.requestHandlers.RequestHandlerBase;
import org.icatproject.ids.services.ServiceProvider;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public abstract class GetDataFileIdsHandler extends RequestHandlerBase {

    public GetDataFileIdsHandler(PreparedDataStatus dataStatus) {
        super(dataStatus, RequestType.GETDATAFILEIDS);
    }

    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {
        
        long start = System.currentTimeMillis();

        DataSelectionBase dataSelection = this.provideDataSelection(parameters);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {

            this.addIndividualParametersToResponseStream(gen, parameters);

            gen.writeStartArray("ids");
            for (DataInfoBase dfInfo : dataSelection.getDfInfo().values()) {
                gen.write(dfInfo.getId());
            }
            gen.writeEnd().writeEnd().close();
        }
        String resp = baos.toString();

        var serviceProvider = ServiceProvider.getInstance();

        if (serviceProvider.getLogSet().contains(CallType.INFO)) {
            try {
                baos = this.generateStreamForTransmitter(parameters);
                serviceProvider.getTransmitter().processMessage("getDatafileIds", parameters.get("ip").getString(), baos.toString(), start);
            } catch (IcatException_Exception e) {
                logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
            }
        }

        return new ValueContainer(resp);
    }

    protected abstract DataSelectionBase provideDataSelection(HashMap<String, ValueContainer> parameters) throws InternalException, BadRequestException, NotFoundException, InsufficientPrivilegesException, NotImplementedException;

    protected abstract ByteArrayOutputStream generateStreamForTransmitter(HashMap<String, ValueContainer> parameters) throws InternalException, BadRequestException, IcatException_Exception;

    protected abstract void addIndividualParametersToResponseStream(JsonGenerator gen, HashMap<String, ValueContainer> parameters) throws InternalException;

}