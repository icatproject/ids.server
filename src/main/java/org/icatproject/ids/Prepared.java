package org.icatproject.ids;

import java.util.Map;
import java.util.Set;

import org.icatproject.ids.plugin.DsInfo;

/* This is a POJO with only package access so don't make data private */
class Prepared {
	boolean zip;
	boolean compress;
	Set<DfInfoImpl> dfInfos;
	Map<Long, DsInfo> dsInfos;
	Set<Long> emptyDatasets;
}