package org.icatproject.ids.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.xml.bind.annotation.XmlRootElement;

@SuppressWarnings("serial")
@Entity
@Table(name = "DOWNLOAD_REQUEST")
@XmlRootElement
public class DownloadRequestEntity implements Serializable {

    @Id
    @GeneratedValue
    private Long id;
    private String preparedId;
    private String sessionId;
    private String userId;
    private String status;
    private boolean compress;
    @Temporal(TemporalType.TIMESTAMP)
    private Date submittedTime;
    @Temporal(TemporalType.TIMESTAMP)
    private Date expireTime;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "downloadRequestId")
    private List<DatafileEntity> datafileList;
    @OneToMany(cascade = CascadeType.ALL, mappedBy = "downloadRequestId")
    private List<DatasetEntity> datasetList;

    public DownloadRequestEntity() {}

    public DownloadRequestEntity(Long id) {
        this.id = id;
    }

    public DownloadRequestEntity(Long id, String preparedId, String userId, Date submittedTime,
            Date expireTime) {
        this.id = id;
        this.preparedId = preparedId;
        this.userId = userId;
        this.submittedTime = submittedTime;
        this.expireTime = expireTime;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public String getUserid() {
        return userId;
    }

    public void setUserid(String userid) {
        this.userId = userid;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public boolean getCompress() {
        return compress;
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public List<DatafileEntity> getDatafileList() {
        return datafileList;
    }

    public void setDatafileList(List<DatafileEntity> datafileList) {
        this.datafileList = datafileList;
    }

    public List<DatasetEntity> getDatasetList() {
        return datasetList;
    }

    public void setDatasetList(List<DatasetEntity> datasetList) {
        this.datasetList = datasetList;
    }
    
    @Override
    public String toString() {
        return DownloadRequestEntity.class.getName() + "[id=" + id + "]";
    }
}
