package org.icatproject.ids.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.util.StatusInfo;

@SuppressWarnings("serial")
@Entity
@Table(name = "IDS_DATAFILE")
public class IdsDatafileEntity implements IdsDataEntity, Serializable {

    @Id
    @GeneratedValue
    private Long id;
    
    @Enumerated(EnumType.STRING)
    private StatusInfo status;
    
    @JoinColumn(name = "REQUESTID", referencedColumnName = "ID")
    @ManyToOne(optional = true)
    private IdsRequestEntity request;

    private Long icatDatafileId;
    private String location;
    
    /*
     * This file should always be set when object is correctly initialized!
     * The amount of data related to this object retrieved from ICAT should be
     * sufficient for the overlapsWith method (so if Dataset of this Datafile
     * is needed, it should also be downloaded form ICAT)
     */
    @Transient
    private Datafile icatDatafile;
    
    public IdsDatafileEntity() {}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	@Override
	public StatusInfo getStatus() {
		return status;
	}

	@Override
	public void setStatus(StatusInfo status) {
		this.status = status;
	}

	@Override
	public IdsRequestEntity getRequest() {
		return request;
	}

	public void setRequest(IdsRequestEntity request) {
		this.request = request;
	}

	public Long getIcatDatafileId() {
		return icatDatafileId;
	}

	public void setIcatDatafileId(Long icatDatafileId) {
		this.icatDatafileId = icatDatafileId;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public Datafile getIcatDatafile() {
		return icatDatafile;
	}

	public void setIcatDatafile(Datafile icatDatafile) {
		this.icatDatafile = icatDatafile;
	}
    
	@Override
	public boolean overlapsWith(IdsDataEntity de) {
		if (de instanceof IdsDatafileEntity) {
			IdsDatafileEntity df = (IdsDatafileEntity) de;
			return this.icatDatafile.getDataset().getId().equals(df.getIcatDatafile().getDataset().getId());
		}
		if (de instanceof IdsDatasetEntity) {
			IdsDatasetEntity ds = (IdsDatasetEntity) de;
			return this.icatDatafile.getDataset().getId().equals(ds.getIcatDataset().getId());
		}
		return false;
	}
	
	@Override
	public Dataset getIcatDataset() {
		return this.icatDatafile.getDataset();
	}
	
	@Override
	public List<Datafile> getIcatDatafiles() {
		List<Datafile> datafiles = new ArrayList<Datafile>();
		datafiles.add(this.icatDatafile);
		return datafiles;
	}
	
	@Override
	public String toString() {
		return String.format("Ids2Datafile %s (real: %s)", id, icatDatafileId);
	}
    
}
