package org.icatproject.ids;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipOutputStream;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.core.StreamingOutput;

import org.icatproject.ids.plugin.DsInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.icatproject.ids.plugin.ZipMapperInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle the streaming of either a single file, or multiple files in
 * a zip file, from the main storage area into an output stream for download.
 * 
 * This is done on the fly, without having to write the zip file to disk first.
 * 
 * The zip file will be compressed if the corresponding flag is set.
 */
public class IdsStreamingOutput implements StreamingOutput {

	private static final Logger logger = LoggerFactory.getLogger(IdsStreamingOutput.class);

    private static final int BUFSIZ = 2048;

	private PropertyHandler propertyHandler;
	private MainStorageInterface mainStorage;
	private ZipMapperInterface zipMapper;

    private long offset;
    private boolean zip;
    private Map<Long, DsInfo> dsInfos;
    private boolean compress;
    private Set<DfInfoImpl> dfInfos;
    private String ip;
    private long start;
    private Long transferId;

    public IdsStreamingOutput(Map<Long, DsInfo> dsInfos, Set<DfInfoImpl> dfInfos, long offset, 
            boolean zip, boolean compress, Long transferId, String ip, long start) {

        propertyHandler = PropertyHandler.getInstance();
        mainStorage = propertyHandler.getMainStorage();
        zipMapper = propertyHandler.getZipMapper();
        
        this.offset = offset;
        this.zip = zip;
        this.dsInfos = dsInfos;
        this.dfInfos = dfInfos;
        this.compress = compress;
        this.transferId = transferId;
        this.ip = ip;
        this.start = start;
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

                // create an ordered set of missing files
                Set<String> missingFiles = new TreeSet<>();
                for (DfInfoImpl dfInfo : dfInfos) {
                    logger.debug("Adding {} to zip", dfInfo);
                    transfer = dfInfo;
                    DsInfo dsInfo = dsInfos.get(dfInfo.getDsId());
                    String entryName = zipMapper.getFullEntryName(dsInfo, dfInfo);
                    InputStream stream = null;
                    try {
                        stream = mainStorage.get(dfInfo.getDfLocation(), dfInfo.getCreateId(), dfInfo.getModId());
                        zos.putNextEntry(new ZipEntry(entryName));
                        int length;
                        while ((length = stream.read(bytes)) >= 0) {
                            zos.write(bytes, 0, length);
                        }
                        zos.closeEntry();
                    } catch (ZipException e) {
                        logger.debug("Skipped duplicate");
                    } catch (IOException e) {
                        logger.warn("Caught IOException {} {}", e.getClass().getSimpleName(), e.getMessage());
                        logger.warn("Skipping missing file in zip: {}", entryName);
                        missingFiles.add(entryName);
                    }
                    if (stream != null) {
                        stream.close();
                    }
                }
                if (!missingFiles.isEmpty()) {
                    // add a file to the zip file listing the missing files
                    String missingFilesZipEntryName = propertyHandler.getMissingFilesZipEntryName();
                    logger.debug("Adding missing files listing {} to zip", missingFilesZipEntryName);
                    StringBuilder sb = new StringBuilder();
                    sb.append("The following files were not found:").append("\n");
                    for (String filename : missingFiles) {
                        sb.append(filename).append("\n");
                    }
                    byte[] data = sb.toString().getBytes();
                    ZipEntry e = new ZipEntry(missingFilesZipEntryName);
                    zos.putNextEntry(e);
                    zos.write(data, 0, data.length);
                    zos.closeEntry();
                }
                zos.close();
            } else {
                DfInfoImpl dfInfo = dfInfos.iterator().next();
                transfer = dfInfo;
                InputStream stream = mainStorage.get(dfInfo.getDfLocation(), dfInfo.getCreateId(),
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
            }

        } catch (IOException e) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (JsonGenerator gen = Json.createGenerator(baos).writeStartObject()) {
                gen.write("transferId", transferId);
                gen.write("exceptionClass", e.getClass().toString());
                gen.write("exceptionMessage", e.getMessage());
                gen.writeEnd();
            }
            logger.error("Failed to stream {} due to {}", transfer, e.getMessage());
            throw e;
        }
    }

}
