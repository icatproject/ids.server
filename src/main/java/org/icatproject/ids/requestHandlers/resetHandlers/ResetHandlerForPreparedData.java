package org.icatproject.ids.requestHandlers.resetHandlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;

import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.dataSelection.DataSelectionFactory;
import org.icatproject.ids.enums.OperationIdTypes;
import org.icatproject.ids.enums.RequestIdNames;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.Prepared;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class ResetHandlerForPreparedData extends ResetHandler {

    public ResetHandlerForPreparedData() {
        super(OperationIdTypes.PREPAREDID);
    }

    @Override
    protected DataSelectionBase provideDataSelection(HashMap<String, ValueContainer> parameters) throws InternalException, BadRequestException, NotFoundException {

        String preparedId = parameters.get(RequestIdNames.preparedId).getString();

        logger.info(String.format("New webservice request: reset preparedId=%s", preparedId));

        // Validate
        validateUUID(RequestIdNames.preparedId, preparedId);

        // Do it
        Prepared preparedJson;
        try (InputStream stream = Files.newInputStream(preparedDir.resolve(preparedId))) {
            preparedJson = unpack(stream);
        } catch (NoSuchFileException e) {
            throw new NotFoundException("The preparedId " + preparedId + " is not known");
        } catch (IOException e) {
            throw new InternalException(e.getClass() + " " + e.getMessage());
        }

        return DataSelectionFactory.get(preparedJson.dsInfos, preparedJson.dfInfos, preparedJson.emptyDatasets, preparedJson.fileLength, this.getRequestType());
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

}