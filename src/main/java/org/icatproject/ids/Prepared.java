package org.icatproject.ids;

import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataSetInfo;

/* This is a POJO with only package access so don't make data private */
public class Prepared {
    public boolean zip;
    public boolean compress;
    public SortedSet<DataFileInfo> dfInfos;
    public SortedMap<Long, DataSetInfo> dsInfos;
    public Set<Long> emptyDatasets;
}
