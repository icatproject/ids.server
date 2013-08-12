package org.icatproject.ids2.ported.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.icatproject.Dataset;
import org.icatproject.ids.util.StatusInfo;

@SuppressWarnings("serial")
@Entity
@Table(name = "IDS2_DATASET")
public class Ids2DatasetEntity implements Ids2DataEntity, Serializable {

	@Id
	@GeneratedValue
	private Long id;
	
	@Enumerated(EnumType.STRING)
	private StatusInfo status;
	
	@JoinColumn(name = "REQUESTID", referencedColumnName = "ID")
	@ManyToOne(optional = false)
	private RequestEntity request;
	
//	@OneToMany(cascade = CascadeType.ALL, mappedBy = "dataset")
//    private List<Ids2DatafileEntity> datafiles;
	
	private String location;	
	private Long icatDatasetId; // this dataset's ID in ICAT database
		
	/**
     * This file should always be set when object is correctly initialized!
     * The amount of data related to this object retrieved from ICAT should be
     * sufficient for the overlapsWith method.
     */
	@Transient
	private Dataset icatDataset;

	public Ids2DatasetEntity() {
	}

	public Ids2DatasetEntity(Long id) {
		this.id = id;
	}

	public Ids2DatasetEntity(Long id, Long datasetId) {
		this.id = id;
		this.icatDatasetId = datasetId;
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

//	public List<Ids2DatafileEntity> getDatafiles() {
//		return datafiles;
//	}
//
//	public void setDatafiles(List<Ids2DatafileEntity> datafiles) {
//		this.datafiles = datafiles;
//	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public Long getIcatDatasetId() {
		return icatDatasetId;
	}

	public void setIcatDatasetId(Long icatDatasetId) {
		this.icatDatasetId = icatDatasetId;
	}
	
	public Dataset getIcatDataset() {
		return icatDataset;
	}

	public void setIcatDataset(Dataset icatDataset) {
		this.icatDataset = icatDataset;
	}

	// equality based on real dataset (in ICAT database), so it won't be
	// possible to have duplicate restore requests (pertaining to the same real
	// dataset) in queue
//	@Override
//	public boolean equals(Object o) {
//		if (o == null)
//			return false;
//		if (o == this)
//			return true;
//		if (!(o instanceof Ids2DatasetEntity))
//			return false;
//		Ids2DatasetEntity rhs = (Ids2DatasetEntity) o;
//		return this.getIcatDatasetId().equals(rhs.getId());
//	}
//
//	@Override
//	public int hashCode() {
//		return this.getIcatDatasetId().hashCode();
//	}

	@Override
	public boolean overlapsWith(Ids2DataEntity de) {
		if (de instanceof Ids2DatafileEntity) {
			Ids2DatafileEntity df = (Ids2DatafileEntity) de;
			return this.icatDataset.getId().equals(df.getIcatDatafile().getDataset().getId());
		}
		if (de instanceof Ids2DatasetEntity) {
			Ids2DatasetEntity ds = (Ids2DatasetEntity) de;
			return this.icatDataset.getId().equals(ds.getIcatDataset().getId());
		}
		return false;
	}
	
	@Override
	public List<Dataset> getDatasets() {
		List<Dataset> datasets = new ArrayList<Dataset>();
		datasets.add(icatDataset);
		return datasets;
	}
	
	@Override
	public String toString() {
		return String.format("Ids2Dataset %s (real: %s)", id, icatDatasetId);
	}
}