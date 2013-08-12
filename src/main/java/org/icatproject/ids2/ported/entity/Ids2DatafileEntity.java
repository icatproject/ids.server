package org.icatproject.ids2.ported.entity;

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
@Table(name = "IDS2_DATAFILE")
public class Ids2DatafileEntity implements Ids2DataEntity, Serializable {

    @Id
    @GeneratedValue
    private Long id;
    
    @Enumerated(EnumType.STRING)
    private StatusInfo status;
    
    // only set when this single file is requested (not a dataset with this file)
    @JoinColumn(name = "REQUESTID", referencedColumnName = "ID")
    @ManyToOne(optional = true)
    private RequestEntity request;
    
//    @JoinColumn(name = "DATASETID", referencedColumnName = "ID")
//    @ManyToOne(optional = false)
//    private Ids2DatasetEntity dataset; 

    private Long icatDatafileId;
    private String location;
    
    /**
     * This file should always be set when object is correctly initialized!
     * The amount of data related to this object retrieved from ICAT should be
     * sufficient for the overlapsWith method.
     */
    @Transient
    private Datafile icatDatafile;
    
    public Ids2DatafileEntity() {}

    public Ids2DatafileEntity(Long id) {
        this.id = id;
    }

    public Ids2DatafileEntity(Long id, Long icatDatafileId, String location) {
        this.id = id;
        this.icatDatafileId = icatDatafileId;
        this.location = location;
    }

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public StatusInfo getStatus() {
		return status;
	}

	@Override
	public void setStatus(StatusInfo status) {
		this.status = status;
	}

	@Override
	public RequestEntity getRequest() {
		return request;
	}

	public void setRequest(RequestEntity request) {
		this.request = request;
	}

//	public Ids2DatasetEntity getDataset() {
//		return dataset;
//	}
//
//	public void setDataset(Ids2DatasetEntity dataset) {
//		this.dataset = dataset;
//	}

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
	public boolean overlapsWith(Ids2DataEntity de) {
		if (de instanceof Ids2DatafileEntity) {
			Ids2DatafileEntity df = (Ids2DatafileEntity) de;
			return this.icatDatafile.getDataset().getId().equals(df.getIcatDatafile().getDataset().getId());
		}
		if (de instanceof Ids2DatasetEntity) {
			Ids2DatasetEntity ds = (Ids2DatasetEntity) de;
			return this.icatDatafile.getDataset().getId().equals(ds.getIcatDataset().getId());
		}
		return false;
	}
	
	@Override
	public List<Dataset> getDatasets() {
		List<Dataset> datasets = new ArrayList<Dataset>();
		datasets.add(this.icatDatafile.getDataset());
		return datasets;
	}
    
}
