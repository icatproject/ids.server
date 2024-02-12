package org.icatproject.ids.v3.helper;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import org.icatproject.ids.RangeOutputStream;
import org.icatproject.ids.LockManager.Lock;
import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.v3.ServiceProvider;
import org.icatproject.ids.v3.models.DataFileInfo;
import org.icatproject.ids.v3.models.DataSetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.json.Json;
import jakarta.json.stream.JsonGenerator;
import jakarta.ws.rs.core.StreamingOutput;

public class SO implements StreamingOutput {

    private long offset;
    private boolean zip;
    private Map<Long, DataSetInfo> dsInfos;
    private Lock lock;
    private boolean compress;
    private Set<DataFileInfo> dfInfos;
    private String ip;
    private long start;
    private Long transferId;
    private ServiceProvider serviceProvider;

    private static final int BUFSIZ = 2048;
    private final static Logger logger = LoggerFactory.getLogger(SO.class);

    public SO(Map<Long, DataSetInfo> dsInfos, Set<DataFileInfo> dfInfos, long offset, boolean zip, boolean compress,
       Lock lock, Long transferId, String ip, long start, ServiceProvider serviceProvider) {
        this.offset = offset;
        this.zip = zip;
        this.dsInfos = dsInfos;
        this.dfInfos = dfInfos;
        this.lock = lock;
        this.compress = compress;
        this.transferId = transferId;
        this.ip = ip;
        this.start = start;
        this.serviceProvider = serviceProvider;
    }

    @Override
    public void write(OutputStream output) throws IOException {

        Object transfer = "??";
        try {
            if (offset != 0) { // Wrap the stream if needed
                output = new RangeOutputStream(output, offset, null);
            }
            byte[] bytes = new byte[BUFSIZ];
            if (zip) {
                ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(output));
                if (!compress) {
                    zos.setLevel(0); // Otherwise use default compression
                }

                for (DataFileInfo dfInfo : dfInfos) {
                    logger.debug("Adding " + dfInfo + " to zip");
                    transfer = dfInfo;
                    DsInfo dsInfo = dsInfos.get(dfInfo.getDsId());
                    String entryName = this.serviceProvider.getPropertyHandler().getZipMapper().getFullEntryName(dsInfo, dfInfo);
                    InputStream stream = null;
                    try {
                        zos.putNextEntry(new ZipEntry(entryName));
                        stream = this.serviceProvider.getMainStorage().get(dfInfo.getDfLocation(), dfInfo.getCreateId(), dfInfo.getModId());
                        int length;
                        while ((length = stream.read(bytes)) >= 0) {
                            zos.write(bytes, 0, length);
                        }
                    } catch (ZipException e) {
                        logger.debug("Skipped duplicate");
                    }
                    zos.closeEntry();
                    if (stream != null) {
                        stream.close();
                    }
                }
                zos.close();
            } else {
                DataFileInfo dfInfo = dfInfos.iterator().next();
                transfer = dfInfo;
                InputStream stream = this.serviceProvider.getMainStorage().get(dfInfo.getDfLocation(), dfInfo.getCreateId(),
                        dfInfo.getModId());
                int length;
                while ((length = stream.read(bytes)) >= 0) {
                    output.write(bytes, 0, length);
                }
                output.close();
                stream.close();
            }

            if (transferId != null) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                    gen.write("transferId", transferId);
                    gen.writeEnd();
                }
                this.serviceProvider.getTransmitter().processMessage("getData", ip, baos.toString(), start);
            }

        } catch (IOException e) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write("transferId", transferId);
                gen.write("exceptionClass", e.getClass().toString());
                gen.write("exceptionMessage", e.getMessage());
                gen.writeEnd();
            }
            this.serviceProvider.getTransmitter().processMessage("getData", ip, baos.toString(), start);
            logger.error("Failed to stream " + transfer + " due to " + e.getMessage());
            throw e;
        } finally {
            lock.release();
        }
    }

}