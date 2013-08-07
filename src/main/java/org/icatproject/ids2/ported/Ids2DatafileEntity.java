package org.icatproject.ids2.ported;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.icatproject.ids.util.StatusInfo;

@SuppressWarnings("serial")
@Entity
@Table(name = "IDS2_DATAFILE")
public class Ids2DatafileEntity implements Serializable {

    @Id
    @GeneratedValue
    private Long id;
    
    @Enumerated(EnumType.STRING)
    private StatusInfo status;
    
    // only set when this single file is requested (not a dataset with this file)
    @JoinColumn(name = "REQUESTID", referencedColumnName = "ID")
    @ManyToOne(optional = true)
    private RequestEntity request;
    
    @JoinColumn(name = "DATASETID", referencedColumnName = "ID")
    @ManyToOne(optional = false)
    private Ids2DatasetEntity dataset;

    private Long datafileId;
    private String location;
    
    public Ids2DatafileEntity() {}

    public Ids2DatafileEntity(Long id) {
        this.id = id;
    }

    public Ids2DatafileEntity(Long id, Long datafileId, String location) {
        this.id = id;
        this.datafileId = datafileId;
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

	public void setStatus(StatusInfo status) {
		this.status = status;
	}

	public RequestEntity getRequest() {
		return request;
	}

	public void setRequest(RequestEntity request) {
		this.request = request;
	}

	public Ids2DatasetEntity getDataset() {
		return dataset;
	}

	public void setDataset(Ids2DatasetEntity dataset) {
		this.dataset = dataset;
	}

	public Long getDatafileId() {
		return datafileId;
	}

	public void setDatafileId(Long datafileId) {
		this.datafileId = datafileId;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}
    
	
    
}
