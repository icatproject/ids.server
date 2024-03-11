package org.icatproject.ids.v3;

import java.util.Set;
import java.util.SortedMap;

import org.icatproject.ids.models.DataInfoBase;

/* This is a POJO with only package access so don't make data private */
public class PreparedV3 {
    public boolean zip;
    public boolean compress;
    public SortedMap<Long, DataInfoBase> dfInfos;
    public SortedMap<Long, DataInfoBase> dsInfos;
    public Set<Long> emptyDatasets;
}