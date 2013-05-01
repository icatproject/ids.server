package org.icatproject.ids.entity;

import java.io.Serializable;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlTransient;

@SuppressWarnings("serial")
@Entity
@Table(name = "DATASET")
public class DatasetEntity implements Serializable {

    @Id
    @GeneratedValue
    private Long id;
    private Long datasetId;
    private String datasetName;
    private String status;
    @JoinColumn(name = "DOWNLOADREQUESTID", referencedColumnName = "ID")
    @ManyToOne(optional = false)
    //@XmlTransient
    private DownloadRequestEntity downloadRequestId;

    public DatasetEntity() {}

    public DatasetEntity(Long id) {
        this.id = id;
    }

    public DatasetEntity(Long id, Long datasetId) {
        this.id = id;
        this.datasetId = datasetId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getDatasetId() {
        return datasetId;
    }

    public void setDatasetid(Long datasetId) {
        this.datasetId = datasetId;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @XmlTransient
    public DownloadRequestEntity getDownloadRequestId() {
        return downloadRequestId;
    }

    public void setDownloadRequestId(DownloadRequestEntity downloadRequestId) {
        this.downloadRequestId = downloadRequestId;
    }

    @Override
    public String toString() {
        return DatasetEntity.class.getName() + "[id=" + id + "]";
    }
}
