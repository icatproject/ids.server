package org.icatproject.ids.v3.models;

import java.util.Set;

import org.icatproject.Dataset;
import org.icatproject.Facility;
import org.icatproject.Investigation;
import org.icatproject.ids.DeferredOp;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.v3.ServiceProvider;

/**
 * Contains information about a Dataset. Replaces DsInfo in v3.
 * May should implement DsInfo interface
 */
public class DataSetInfo extends DataInfoBase implements DsInfo {

    protected Long facilityId;
    protected String facilityName;
    protected Long investigationId;
    protected String investigationName;
    protected String visitId;


    public DataSetInfo(Dataset ds) throws InsufficientPrivilegesException {
        super(ds.getId(), ds.getName(), ds.getLocation()); 

        Investigation investigation = ds.getInvestigation();
        if (investigation == null) {
            throw new InsufficientPrivilegesException(
                    "Probably not able to read Investigation for dataset id " + ds.getId());
        }
        Facility facility = investigation.getFacility();
        if (facility == null) {
            throw new InsufficientPrivilegesException(
                    "Probably not able to read Facility for investigation id "
                            + investigation.getId());
        }

        this.investigationId = investigation.getId();
        this.investigationName = investigation.getName();
        this.visitId = investigation.getVisitId();
        this.facilityId = facility.getId();
        this.facilityName = facility.getName();
    }


    public DataSetInfo(Long id, String name, String location, Long investigationId, String investigationName, String visitId, Long facilityId, String facilityName) {
        super(id, name, location);

        this.facilityId = facilityId;
        this.facilityName = facilityName;
        this.investigationId = investigationId;
        this.investigationName = investigationName;
        this.visitId = visitId;
    }

    @Override
    public String toString() {
        return this.investigationId + "/" + this.id + " (" + this.facilityName + "/" + this.investigationName + "/" + this.visitId + "/"
                + this.name + ")";
    }

    public boolean restoreIfOffline(Set<Long> emptyDatasets) throws InternalException {
        boolean maybeOffline = false;
        var serviceProvider = ServiceProvider.getInstance();
        if (serviceProvider.getFsm().getDsMaybeOffline().contains(this)) {
            maybeOffline = true;
        } else if (!emptyDatasets.contains(this.getId()) && !serviceProvider.getMainStorage().exists(this)) {
            serviceProvider.getFsm().queue(this, DeferredOp.RESTORE);
            maybeOffline = true;
        }
        return maybeOffline;
    }

    public Long getFacilityId() {
        return facilityId;
    }

    public String getFacilityName() {
        return facilityName;
    }

    public Long getInvestigationId() {
        return investigationId;
    }

    public String getInvestigationName() {
        return investigationName;
    }

    public String getVisitId() {
        return visitId;
    }


    // implementing DsInfo

    @Override
    public Long getDsId() { return this.getId(); }
    @Override
    public String getDsName() { return this.getName(); }
    @Override
    public String getDsLocation() { return this.getLocation(); }
    @Override
    public Long getInvId() { return this.getInvestigationId(); }
    @Override
    public String getInvName() { return this.getInvestigationName(); }

}