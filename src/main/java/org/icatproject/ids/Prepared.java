package org.icatproject.ids;

import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import org.icatproject.ids.v3.model.DataFileInfo;
import org.icatproject.ids.v3.model.DataSetInfo;

/* This is a POJO with only package access so don't make data private */
class Prepared {
    boolean zip;
    boolean compress;
    SortedSet<DataFileInfo> dfInfos;
    SortedMap<Long, DataSetInfo> dsInfos;
    Set<Long> emptyDatasets;
}
