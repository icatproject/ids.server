package org.icatproject.ids.finiteStateMachine;

import org.icatproject.ids.enums.DeferredOp;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.services.IcatReader;
import org.icatproject.ids.services.LockManager;

import jakarta.json.stream.JsonGenerator;

public class FiniteStateMachineForSingleLevelStorage extends FiniteStateMachine {

    protected FiniteStateMachineForSingleLevelStorage(IcatReader reader, LockManager lockManager) {
        super(reader, lockManager);
    }


    @Override
    public void queue(DataInfoBase dataInfo, DeferredOp deferredOp) throws InternalException {
        throw new InternalException("### Operation is not permitted for single level storage");
    }


    @Override
    protected void scheduleTimer() {
        //nothing to do here for single level storage
    }


    @Override
    protected void addDataInfoJson(JsonGenerator gen) {
        gen.writeStartArray("opsQueue").writeEnd();
    }

}
