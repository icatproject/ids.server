package org.icatproject.ids.entity;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;

@Entity
@Table(name = "IDS_WRITE_TIMES")
@XmlRootElement
public class IdsWriteTimesEntity {
	
	@Id
	private Long icatDatasetId;
	
	private Long writeTime;
	
	public IdsWriteTimesEntity() {}
	
	public IdsWriteTimesEntity(Long icatDatasetId, Long writeTime) {
		this.icatDatasetId = icatDatasetId;
		this.writeTime = writeTime;
	}

	public Long getIcatDatasetId() {
		return icatDatasetId;
	}

	public void setIcatDatasetId(Long icatDatasetId) {
		this.icatDatasetId = icatDatasetId;
	}

	public Long getWriteTime() {
		return writeTime;
	}

	public void setWriteTime(Long writeTime) {
		this.writeTime = writeTime;
	}
	
}
