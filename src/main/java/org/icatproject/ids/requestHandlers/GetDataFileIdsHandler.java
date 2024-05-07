package org.icatproject.ids.requestHandlers;

import java.io.ByteArrayOutputStream;

import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.requestHandlers.base.DataRequestHandler;
import org.icatproject.ids.requestHandlers.base.PreparedDataController;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;

public class GetDataFileIdsHandler extends DataRequestHandler {

    public GetDataFileIdsHandler(String ip, String preparedId, String sessionId, String investigationIds, String datasetIds, String datafileIds) {
        super(RequestType.GETDATAFILEIDS, ip, preparedId, sessionId, investigationIds, datasetIds, datafileIds);
    }

    @Override
    public ValueContainer handleDataRequest(DataSelectionBase dataSelection)
            throws BadRequestException, InternalException, InsufficientPrivilegesException, NotFoundException,
            DataNotOnlineException, NotImplementedException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {

            if(this.dataController instanceof PreparedDataController) {
                gen.write("zip", dataSelection.getZip());
                gen.write("compress", dataSelection.getCompress());
            }

            gen.writeStartArray("ids");
            for (DataInfoBase dfInfo : dataSelection.getDfInfo().values()) {
                gen.write(dfInfo.getId());
            }
            gen.writeEnd().writeEnd().close();
        }
        String resp = baos.toString();

        return new ValueContainer(resp);
    }

    @Override
    public CallType getCallType() {
        return CallType.INFO;
    }

}