package org.icatproject.ids.requestHandlers.getDataFileIdsHandlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;

import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.PreparedDataStatus;
import org.icatproject.ids.enums.RequestIdNames;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.Prepared;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class GetDataFileIdsHandlerForPreparedData extends GetDataFileIdsHandler {

    public GetDataFileIdsHandlerForPreparedData() {
        super(PreparedDataStatus.PREPARED);
    }

    @Override
    protected DataSelectionBase provideDataSelection(HashMap<String, ValueContainer> parameters) throws InternalException, BadRequestException, NotFoundException {

        String preparedId = parameters.get(RequestIdNames.preparedId).getString();

        // Log and validate
        logger.info("New webservice request: getDatafileIds preparedId = '" + preparedId);

        validateUUID(RequestIdNames.preparedId, preparedId);

        // Do it
        Prepared prepared;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            prepared = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        parameters.put("zip", new ValueContainer(prepared.zip));
        parameters.put("compress", new ValueContainer(prepared.compress));

        return this.getDataSelection(prepared.dsInfos, prepared.dfInfos, prepared.emptyDatasets, 0);
    }

    @Override
    protected ByteArrayOutputStream generateStreamForTransmitter(HashMap<String, ValueContainer> parameters) throws InternalException {
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
            gen.write(RequestIdNames.preparedId, parameters.get(RequestIdNames.preparedId).getString());
            gen.writeEnd();
        }

        return baos;
    }

    @Override
    protected void addIndividualParametersToResponseStream(JsonGenerator gen, HashMap<String, ValueContainer> parameters) throws InternalException {
                gen.write("zip", parameters.get("zip").getBool());
                gen.write("compress", parameters.get("compress").getBool());
    }
    
}