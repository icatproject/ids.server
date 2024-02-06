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

import org.icatproject.ids.v3.model.DataFileInfo;
import org.icatproject.ids.v3.model.DataSetInfo;
import org.junit.Test;

public class PreparePackingTest {

    @Test
    public void packAndUnpack() throws Exception {
        boolean zip = true;
        boolean compress = false;
        Map<Long, DataSetInfo> dsInfos = new HashMap<>();
        Set<DataFileInfo> dfInfos = new HashSet<>();
        Set<Long> emptyDatasets = new HashSet<>();

        long dsid1 = 17L;
        long dsid2 = 18L;
        long invId = 15L;
        long facilityId = 45L;
        dfInfos.add(new DataFileInfo(5L, "dfName", "dfLocation", "createId", "modId", dsid1));

        dfInfos.add(new DataFileInfo(51L, "dfName2", null, "createId", "modId", dsid1));

        dsInfos.put(dsid1, new DataSetInfo(dsid1, "dsName", "dsLocation", invId, "invName",
                "visitId", facilityId, "facilityName"));

        dsInfos.put(dsid2, new DataSetInfo(dsid2, "dsName2", null, invId, "invName", "visitId",
                facilityId, "facilityName"));

        emptyDatasets.add(dsid2);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (OutputStream stream = new BufferedOutputStream(baos)) {
            IdsBean.pack(stream, zip, compress, dsInfos, dfInfos, emptyDatasets);
        }
        System.out.println(baos.toString());
        InputStream stream = new ByteArrayInputStream(baos.toByteArray());
        Prepared prepared = IdsBean.unpack(stream);
        assertTrue(prepared.zip);
        assertFalse(prepared.compress);
        for (DataFileInfo dfInfo : prepared.dfInfos) {
            if (dfInfo.getDfId() == 5L) {
                assertEquals("dfName", dfInfo.getDfName());
                assertEquals("dfLocation", dfInfo.getDfLocation());
            } else if (dfInfo.getDfId() == 51L) {
                assertEquals("dfName2", dfInfo.getDfName());
                assertNull(dfInfo.getDfLocation());
            } else {
                fail();
            }
            assertEquals("createId", dfInfo.getCreateId());
            assertEquals("modId", dfInfo.getModId());
            assertEquals(dsid1, dfInfo.getDsId());
        }
        for (Entry<Long, DataSetInfo> entry : prepared.dsInfos.entrySet()) {
            Long key = entry.getKey();
            DataSetInfo value = entry.getValue();
            assertEquals((Long) key, (Long) value.getDsId());
            if (value.getDsId() == dsid1) {
                assertEquals("dsName", value.getDsName());
                assertEquals("dsLocation", value.getDsLocation());
            } else if (value.getDsId() == dsid2) {
                assertEquals("dsName2", value.getDsName());
                assertNull(value.getDsLocation());
            } else {
                fail();
            }
            assertEquals((Long) invId, value.getInvId());
            assertEquals("invName", value.getInvName());

            assertEquals("visitId", value.getVisitId());
            assertEquals((Long) facilityId, value.getFacilityId());
            assertEquals("facilityName", value.getFacilityName());

        }
        assertEquals(1, prepared.emptyDatasets.size());
        assertEquals((Long) dsid2, prepared.emptyDatasets.iterator().next());
    }
}
