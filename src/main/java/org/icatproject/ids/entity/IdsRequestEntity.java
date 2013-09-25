package org.icatproject.ids.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlRootElement;

import org.icatproject.Datafile;
import org.icatproject.Dataset;
import org.icatproject.ids.util.StatusInfo;
import org.icatproject.ids.webservice.DeferredOp;

@SuppressWarnings("serial")
@Entity
@Table(name = "IDS_REQUEST")
@XmlRootElement
public class IdsRequestEntity implements Serializable {

	@Id
	@GeneratedValue
	private Long id;

	@Enumerated(EnumType.STRING)
	private StatusInfo status;

	@Temporal(TemporalType.TIMESTAMP)
	private Date submittedTime;

	@Temporal(TemporalType.TIMESTAMP)
	private Date expireTime;

	@OneToMany(cascade = CascadeType.ALL, mappedBy = "request")
	private List<IdsDatafileEntity> datafiles;

	@OneToMany(cascade = CascadeType.ALL, mappedBy = "request")
	private List<IdsDatasetEntity> datasets;

	@Enumerated(EnumType.STRING)
	private DeferredOp deferredOp;

	private String preparedId;
	private String sessionId;
	private String userId;

	@Column(name = "IDS_COMPRESS")
	private boolean compress;

	public IdsRequestEntity() {
	}

	public IdsRequestEntity(Long id) {
		this.id = id;
	}

	public IdsRequestEntity(Long id, String preparedId, String userId, Date submittedTime, Date expireTime,
			DeferredOp type) {
		this.id = id;
		this.preparedId = preparedId;
		this.userId = userId;
		this.submittedTime = submittedTime;
		this.expireTime = expireTime;
		this.deferredOp = type;
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

	public Date getSubmittedTime() {
		return submittedTime;
	}

	public void setSubmittedTime(Date submittedTime) {
		this.submittedTime = submittedTime;
	}

	public Date getExpireTime() {
		return expireTime;
	}

	public void setExpireTime(Date expireTime) {
		this.expireTime = expireTime;
	}

	public List<IdsDatafileEntity> getDatafiles() {
		return datafiles;
	}

	public void setDatafiles(List<IdsDatafileEntity> datafiles) {
		this.datafiles = datafiles;
	}

	public List<IdsDatasetEntity> getDatasets() {
		return datasets;
	}

	public void setDatasets(List<IdsDatasetEntity> datasets) {
		this.datasets = datasets;
	}

	public DeferredOp getDeferredOp() {
		return deferredOp;
	}

	public void setDeferredOp(DeferredOp deferredOp) {
		this.deferredOp = deferredOp;
	}

	public String getPreparedId() {
		return preparedId;
	}

	public void setPreparedId(String preparedId) {
		this.preparedId = preparedId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public boolean isCompress() {
		return compress;
	}

	public void setCompress(boolean compress) {
		this.compress = compress;
	}

	@Override
	public String toString() {
		return String.format("RequestEntity id=%s, requestedState=%s", id, deferredOp);
	}

	public List<IdsDataEntity> getDataEntities() {
		List<IdsDataEntity> res = new ArrayList<IdsDataEntity>();
		res.addAll(datafiles);
		res.addAll(datasets);
		return res;
	}

	/*
	 * Returns all ICAT Datafiles that were requested directly (Datafiles from
	 * requested Datasets don't count)
	 */
	public Set<Datafile> getIcatDatafiles() {
		Set<Datafile> datafiles = new HashSet<Datafile>();
		for (IdsDatafileEntity df : this.getDatafiles()) {
			datafiles.addAll(df.getIcatDatafiles()); // will only add one DF
		}
		return datafiles;
	}

	/*
	 * Returns all ICAT Datasets that were requested directly (Datasets
	 * operation on which has been caused by a requested Datafile don't count)
	 */
	public Set<Dataset> getIcatDatasets() {
		Set<Dataset> datasets = new HashSet<Dataset>();
		for (IdsDatasetEntity ds : this.getDatasets()) {
			datasets.add(ds.getIcatDataset());
		}
		return datasets;
	}

}