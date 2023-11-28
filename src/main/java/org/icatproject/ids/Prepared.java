package org.icatproject.ids;

import org.icatproject.ids.plugin.DsInfo;

import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

/* This is a POJO with only package access so don't make data private */
class Prepared {
    boolean zip;
    boolean compress;
    SortedSet<DfInfoImpl> dfInfos;
    SortedMap<Long, DsInfo> dsInfos;
    Set<Long> emptyDatasets;
}