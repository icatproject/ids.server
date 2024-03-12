package org.icatproject.ids;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.icatproject.ids.models.DataFileInfo;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.models.DataSetInfo;
import org.icatproject.ids.models.Prepared;
import org.icatproject.ids.requestHandlers.RequestHandlerBase;
import org.junit.Test;

public class PreparePackingTest {

    @Test
    public void packAndUnpack() throws Exception {
        boolean zip = true;
        boolean compress = false;
        Map<Long, DataInfoBase> dsInfos = new HashMap<>();
        Map<Long, DataInfoBase> dfInfos = new HashMap<>();
        Set<Long> emptyDatasets = new HashSet<>();

        long dsid1 = 17L;
        long dsid2 = 18L;
        long invId = 15L;
        long facilityId = 45L;
        dfInfos.put(5L, new DataFileInfo(5L, "dfName", "dfLocation", "createId", "modId", dsid1));

        dfInfos.put(51L, new DataFileInfo(51L, "dfName2", null, "createId", "modId", dsid1));

        dsInfos.put(dsid1, new DataSetInfo(dsid1, "dsName", "dsLocation", invId, "invName",
                "visitId", facilityId, "facilityName"));

        dsInfos.put(dsid2, new DataSetInfo(dsid2, "dsName2", null, invId, "invName", "visitId",
                facilityId, "facilityName"));

        emptyDatasets.add(dsid2);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (OutputStream stream = new BufferedOutputStream(baos)) {
            RequestHandlerBase.pack(stream, zip, compress, dsInfos, dfInfos, emptyDatasets);
        }
        System.out.println(baos.toString());
        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        Prepared prepared = RequestHandlerBase.unpack(stream);
        assertTrue(prepared.zip);
        assertFalse(prepared.compress);
        for (DataInfoBase dataInfo : prepared.dfInfos.values()) {
            if (dataInfo.getId() == 5L) {
                assertEquals("dfName", dataInfo.getName());
                assertEquals("dfLocation", dataInfo.getLocation());
            } else if (dataInfo.getId() == 51L) {
                assertEquals("dfName2", dataInfo.getName());
                assertNull(dataInfo.getLocation());
            } else {
                fail();
            }
            var dfInfo = (DataFileInfo) dataInfo;
            assertEquals("createId", dfInfo.getCreateId());
            assertEquals("modId", dfInfo.getModId());
            assertEquals(dsid1, dfInfo.getDsId());
        }
        for (Entry<Long, DataInfoBase> entry : prepared.dsInfos.entrySet()) {
            Long key = entry.getKey();
            DataInfoBase value = entry.getValue();
            assertEquals((Long) key, (Long) value.getId());
            if (value.getId() == dsid1) {
                assertEquals("dsName", value.getName());
                assertEquals("dsLocation", value.getLocation());
            } else if (value.getId() == dsid2) {
                assertEquals("dsName2", value.getName());
                assertNull(value.getLocation());
            } else {
                fail();
            }

            var dsInfo = (DataSetInfo) value;

            assertEquals((Long) invId, dsInfo.getInvId());
            assertEquals("invName", dsInfo.getInvName());

            assertEquals("visitId", dsInfo.getVisitId());
            assertEquals((Long) facilityId, dsInfo.getFacilityId());
            assertEquals("facilityName", dsInfo.getFacilityName());

        }
        assertEquals(1, prepared.emptyDatasets.size());
        assertEquals((Long) dsid2, prepared.emptyDatasets.iterator().next());
    }
}
