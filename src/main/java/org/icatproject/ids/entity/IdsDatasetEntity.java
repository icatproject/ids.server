package org.icatproject.ids.entity;

import java.io.Serializable;
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
@Table(name = "IDS_DATASET")
public class IdsDatasetEntity implements IdsDataEntity, Serializable {

	@Id
	@GeneratedValue
	private Long id;
	
	@Enumerated(EnumType.STRING)
	private StatusInfo status;
	
	@JoinColumn(name = "REQUESTID", referencedColumnName = "ID")
	@ManyToOne(optional = false)
	private IdsRequestEntity request;
	
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

	public IdsDatasetEntity() {
	}

//	public Ids2DatasetEntity(Long id) {
//		this.id = id;
//	}
//
//	public Ids2DatasetEntity(Long id, Long datasetId) {
//		this.id = id;
//		this.icatDatasetId = datasetId;
//	}

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
	
	@Override
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
	public boolean overlapsWith(IdsDataEntity de) {
		if (de instanceof IdsDatafileEntity) {
			IdsDatafileEntity df = (IdsDatafileEntity) de;
			return this.icatDataset.getId().equals(df.getIcatDatafile().getDataset().getId());
		}
		if (de instanceof IdsDatasetEntity) {
			IdsDatasetEntity ds = (IdsDatasetEntity) de;
			return this.icatDataset.getId().equals(ds.getIcatDataset().getId());
		}
		return false;
	}
	
	@Override
	public List<Datafile> getIcatDatafiles() {
		return this.icatDataset.getDatafiles();
	}
	
	@Override
	public String toString() {
		return String.format("Ids2Dataset %s (real: %s)", id, icatDatasetId);
	}
}