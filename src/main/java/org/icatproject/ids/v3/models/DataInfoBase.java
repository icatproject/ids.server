package org.icatproject.ids.v3.models;

/**
 * A Base class for Data objct types. like Datasets or Datafiles
 */
public abstract class DataInfoBase {

    protected long id;
    protected String name;
    protected String location;

    protected DataInfoBase(long id, String name, String location){
        this.name = name;
        this.id = id;
        this.location = location;
    }

    public abstract String toString();

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getLocation() {
        return location;
    }

    public int hashCode() {
        return (int) (this.id ^ (this.id >>> 32));
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        return this.id == ((DataInfoBase) obj).getId();
    }
    
}