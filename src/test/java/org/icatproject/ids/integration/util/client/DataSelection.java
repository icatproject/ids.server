package org.icatproject.ids.integration.util.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataSelection {

    private Set<Long> datafileIds = new HashSet<>();

    private Set<Long> datasetIds = new HashSet<>();

    private Set<Long> investigationIds = new HashSet<>();

    public DataSelection addDatafile(Long id) {
        datafileIds.add(id);
        return this;
    }

    public DataSelection addDatafiles(List<Long> ids) {
        datafileIds.addAll(ids);
        return this;
    }

    public DataSelection addDataset(long id) {
        datasetIds.add(id);
        return this;
    }

    public DataSelection addDatasets(List<Long> ids) {
        datasetIds.addAll(ids);
        return this;
    }

    public DataSelection addInvestigation(long invId) {
        investigationIds.add(invId);
        return this;
    }

    public DataSelection addInvestigations(List<Long> ids) {
        investigationIds.addAll(ids);
        return this;
    }

    public Map<String, String> getParameters() {
        Map<String, String> parameters = new HashMap<>();
        if (!investigationIds.isEmpty()) {
            parameters.put("investigationIds", setToString(investigationIds));
        }
        if (!datasetIds.isEmpty()) {
            parameters.put("datasetIds", setToString(datasetIds));
        }
        if (!datafileIds.isEmpty()) {
            parameters.put("datafileIds", setToString(datafileIds));
        }
        return parameters;
    }

    private String setToString(Set<Long> ids) {
        StringBuilder sb = new StringBuilder();
        for (long id : ids) {
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(Long.toString(id));
        }
        return sb.toString();
    }
}
