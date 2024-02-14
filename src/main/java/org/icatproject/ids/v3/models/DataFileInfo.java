package org.icatproject.ids.v3.models;
import org.icatproject.ids.DeferredOp;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.v3.ServiceProvider;

/**
 * Contains information about a Datafile. Replaces DsInfo in v3
 * May should implement DfInfo interface
 */
public class DataFileInfo extends DataInfoBase implements Comparable<DataFileInfo>, DfInfo {

    protected String createId;
    protected String modId; 
    protected long datasId;

    public DataFileInfo(Long id, String name, String location, String createId, String modId, Long datasId) {
        super(id, name, location);

        this.createId = createId;
        this.modId = modId;
        this.datasId = datasId;
    }

    public String getCreateId() {
        return this.createId;
    }

    public String getModId() {
        return this.modId;
    }

    public long getDsId() {
        return this.datasId;
    }

    @Override
    public String toString() {
        return this.location;
    }

    @Override
    public int compareTo(DataFileInfo o) {
        if (this.id > o.getId()) {
            return 1;
        }
        if (this.id < o.getId()) {
            return -1;
        }
        return 0;
    }

    public boolean restoreIfOffline() throws InternalException {
        boolean maybeOffline = false;
        var serviceProvider = ServiceProvider.getInstance();
        if (serviceProvider.getFsm().getDfMaybeOffline().contains(this)) {
            maybeOffline = true;
        } else if (!serviceProvider.getMainStorage().exists(this.getDfLocation())) {
            serviceProvider.getFsm().queue(this, DeferredOp.RESTORE);
            maybeOffline = true;
        }
        return maybeOffline;
    }

    // implementing DfInfo
    @Override
    public Long getDfId() { return this.getId(); }
    @Override
    public String getDfLocation() { return this.getLocation(); }
    @Override
    public String getDfName() { return this.getName(); }
}