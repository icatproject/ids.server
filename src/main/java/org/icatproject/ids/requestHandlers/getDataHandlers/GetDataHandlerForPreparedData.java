package org.icatproject.ids.requestHandlers.getDataHandlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;

import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.PreparedDataStatus;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.Prepared;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class GetDataHandlerForPreparedData extends GetDataHandler{

    public GetDataHandlerForPreparedData() {
        super(PreparedDataStatus.PREPARED);
    }

    @Override
    protected DataSelectionBase provideDataSelection(HashMap<String, ValueContainer> parameters, long offset) throws BadRequestException, InternalException, NotFoundException {

        String preparedId = parameters.get("preparedId").getString();
        
        // Log and validate
        logger.info("New webservice request: getData preparedId = '" + preparedId + "' outname = '" + parameters.get("outname").getString()
                + "' offset = " + offset);

        validateUUID("preparedId", preparedId);

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

        return this.getDataSelection(prepared.dsInfos, prepared.dfInfos, prepared.emptyDatasets, prepared.fileLength);
    }

    @Override
    protected ByteArrayOutputStream generateStreamForTransmitter(HashMap<String, ValueContainer> parameters, Long transferId) throws InternalException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
            gen.write("transferId", transferId);
            gen.write("preparedId", parameters.get("preparedId").getString());
            gen.writeEnd();
        }

        return baos;
    }

    @Override
    protected boolean mustZip(boolean zip, DataSelectionBase dataSelection) {
        return zip;
    }
    
}