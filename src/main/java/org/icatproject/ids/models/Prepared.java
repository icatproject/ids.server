package org.icatproject.ids.models;

import java.util.Set;
import java.util.SortedMap;

/* This is a POJO with only package access so don't make data private */
public class Prepared {
    public boolean zip;
    public boolean compress;
    public long fileLength;
    public SortedMap<Long, DataInfoBase> dfInfos;
    public SortedMap<Long, DataInfoBase> dsInfos;
    public Set<Long> emptyDatasets;
}