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
@Table(name = "DATAFILE")
public class DatafileEntity implements Serializable {

    @Id
    @GeneratedValue
    private Long id;
    private Long datafileId;
    private Long datasetId;
    private String name;
    private String status;
    @JoinColumn(name = "DOWNLOADREQUESTID", referencedColumnName = "ID")
    @ManyToOne(optional = false)
    private DownloadRequestEntity downloadRequestId;

    public DatafileEntity() {}

    public DatafileEntity(Long id) {
        this.id = id;
    }

    public DatafileEntity(Long id, Long datafileId, String name) {
        this.id = id;
        this.datafileId = datafileId;
        this.name = name;
    }

    public Long getDatafileId() {
        return datafileId;
    }

    public void setDatafileId(Long datafileId) {
        this.datafileId = datafileId;
    }

    public Long getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(Long datasetId) {
        this.datasetId = datasetId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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
        return DatafileEntity.class.getName() + "[id=" + id + "]";
    }
}
