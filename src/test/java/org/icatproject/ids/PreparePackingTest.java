package org.icatproject.ids;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.icatproject.ids.plugin.DsInfo;
import org.junit.Test;

public class PreparePackingTest {

	@Test
	public void packAndUnpack() throws Exception {
		Path preparedDir = Paths.get(System.getProperty("java.io.tmpdir")).resolve(Constants.PREPARED_DIR_NAME);
		Files.createDirectories(preparedDir);
		PreparedFilesManager preparedFilesManager = new PreparedFilesManager(preparedDir);
		String preparedId = this.getClass().getSimpleName() + "-" + System.currentTimeMillis();

		boolean zip = true;
		boolean compress = false;
		Map<Long, DsInfo> dsInfos = new HashMap<>();
		Set<DfInfoImpl> dfInfos = new HashSet<>();
		Set<Long> emptyDatasets = new HashSet<>();

		long dsid1 = 17L;
		long dsid2 = 18L;
		long invId = 15L;
		long facilityId = 45L;
		dfInfos.add(new DfInfoImpl(5L, "dfName", "dfLocation", "createId", "modId", dsid1));

		dfInfos.add(new DfInfoImpl(51L, "dfName2", null, "createId", "modId", dsid1));

		dsInfos.put(dsid1, new DsInfoImpl(dsid1, "dsName", "dsLocation", invId, "invName",
				"visitId", facilityId, "facilityName"));

		dsInfos.put(dsid2, new DsInfoImpl(dsid2, "dsName2", null, invId, "invName", "visitId",
				facilityId, "facilityName"));

		emptyDatasets.add(dsid2);

		preparedFilesManager.pack(preparedId, zip, compress, dsInfos, dfInfos, emptyDatasets);

		Prepared prepared = preparedFilesManager.unpack(preparedId);
		assertTrue(prepared.zip);
		assertFalse(prepared.compress);
		for (DfInfoImpl dfInfo : prepared.dfInfos) {
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
		for (Entry<Long, DsInfo> entry : prepared.dsInfos.entrySet()) {
			Long key = entry.getKey();
			DsInfo value = entry.getValue();
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

		// tidy up
		preparedFilesManager.deletePreparedFile(preparedId);
	}
}