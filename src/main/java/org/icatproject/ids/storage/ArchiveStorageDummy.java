package org.icatproject.ids.storage;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.stfc.storaged.DfInfoWithLocation;

/**
 * A dummy implementation of the ArchiveStorageInterfaceV2 which is used for 
 * development and testing. It simulates an archive storage interface like the
 * StorageD one by allowing for a list of files to be requested and then 
 * restoring them at a configurable rate. 
 * 
 * So that the files do not even need to exist, the dummy implementation works
 * from a supplied list of absolute filepaths, each of them prepended with the
 * size of the file in bytes. For example, for the first implementation, a 
 * basic command was run to list all of the files in a Payara server 
 * installation and their sizes.
 * 
 * The dummy implementation creates the files by simply adding lines to a 
 * file as it is created, repeating the filepath on each line as many times as
 * necessary to create the number of bytes specified. This means that each 
 * file has a known size and the exact contents are known.
 */
public class ArchiveStorageDummy implements ArchiveStorageInterfaceV2 {

    private static final Logger logger = LoggerFactory.getLogger(ArchiveStorageDummy.class);

    private Map<String, Integer> pathToSizeMap = new LinkedHashMap<>();

    private int numFilesRemaining;

    public static String RESTORE_FAIL_PROPERTY = "ArchiveStorageDummy-FAIL";
    private boolean restoreShouldFail = false;

    // parameters that affect how quickly dummy files are created
    private int timeReductionFactor = 100;   // for the tests to run faster!
    private int maxMsPerFile = 1000;         // ditto!
    // private int timeReductionFactor = 10;    // for more realistic/human followable output
    // private int maxMsPerFile = 10000;        // ditto

    public ArchiveStorageDummy(Properties props) throws InstantiationException {
        if (props != null) {
            String timeReductionFactorKey = "plugin.archive.dummy.timeReductionFactor";
            if (props.containsKey(timeReductionFactorKey)) {
                timeReductionFactor = Integer.parseInt(props.getProperty(timeReductionFactorKey));
            }
            String maxMsPerFileKey = "plugin.archive.dummy.maxMsPerFile";
            if (props.containsKey(maxMsPerFileKey)) {
                maxMsPerFile = Integer.parseInt(props.getProperty(maxMsPerFileKey));
            }
        }
        logger.debug("timeReductionFactor = {}, maxMsPerFile = {}", timeReductionFactor, maxMsPerFile);

        String dummyFileListingFilename = "payara5_file_listing_with_sizes.txt";
        InputStream inputStream = ArchiveStorageDummy.class.getClassLoader().getResourceAsStream(dummyFileListingFilename);
        if (inputStream != null) {
            try {
                // read the file containing entries like as below into a Map
                // where the key is the file path and the value is the size
                // line format is: 
                // <file size in bytes><space><filePath> 
                // eg.
                // 3649 /dummy/payara5/README.txt
                // 322535 /dummy/payara5/glassfish/legal/3RD-PARTY-LICENSE.txt
                // 38052 /dummy/payara5/glassfish/legal/LICENSE.txt
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                while(reader.ready()) {
                    String line = reader.readLine();
                    String[] lineParts = line.split("\\s+", 2);
                    Integer fileSize = Integer.parseInt(lineParts[0]);
                    String filePath = lineParts[1];
                    pathToSizeMap.put(filePath, fileSize);
                }
                inputStream.close();
            } catch (IOException e) {
                throw new InstantiationException(
                    "Problem reading dummy file listing: " + dummyFileListingFilename + " : " + e.getMessage());
            }
            if (System.getProperty(RESTORE_FAIL_PROPERTY) != null) {
                // clear the system property so that no other restore threads fail
                // and set a flag to say that this restore should fail
                logger.debug("This thread is set to fail part way through restoring");
                System.clearProperty(RESTORE_FAIL_PROPERTY);
                restoreShouldFail = true;
            }
        } else {
            throw new InstantiationException("Dummy file listing not found: " + dummyFileListingFilename);
        }
    }

    /**
     * Return a Map representation of the files listed in the dummy file 
     * listing and their sizes.
     * 
     * @return a Map keyed on the file path with the corresponding file size in 
     *         bytes as the value
     */
    public Map<String, Integer> getPathToSizeMap() {
        return pathToSizeMap;
    }

    @Override
    public Set<DfInfo> restore(MainStorageInterface mainStorageInterface, List<DfInfo> dfInfos, AtomicBoolean stopRestoring) throws IOException {
        int numFilesRequested = dfInfos.size();
        logger.info("Requesting files from Dummy Archive Storage: {}", numFilesRequested);
        numFilesRemaining = numFilesRequested;

        int restoreToFailOn = -1;
        if (restoreShouldFail) {
            restoreToFailOn = dfInfos.size()/2;
        }

        Map<String, DfInfo> dfInfoFromLocation = new HashMap<>();
        for (DfInfo dfInfo : dfInfos) {
            dfInfoFromLocation.put(dfInfo.getDfLocation(), dfInfo);
        }
        int restoredFileCount = 0;
        for (DfInfo dfInfo : dfInfos) {
            String location = dfInfo.getDfLocation();
            logger.debug("Requesting {} from Dummy Archive Storage", location);
            Integer fileSize = pathToSizeMap.get(location);
            if (fileSize == null) {
                logger.warn("File does not exist on Dummy Archive Storage: {}", location);
                continue;
            } else {
                try {
                    Thread.sleep(calculateDummyTimeToGetFile(fileSize));
                } catch (InterruptedException e) {
                    // ignore this
                }
                ByteArrayInputStream byteStream = new ByteArrayInputStream(createFile(location, fileSize));
                createFile(location, fileSize);
                mainStorageInterface.put(byteStream, location);
                dfInfoFromLocation.remove(location);
                restoredFileCount++;
            }
            numFilesRemaining--;
            if (restoredFileCount == restoreToFailOn) {
                throw new IOException("Test exception causing restore to fail");
            }
            // check whether the stop flag has been set
            if (stopRestoring.get()) {
                // if it is then this is a safe place to exit
                logger.info("Stopping restore with {}/{} files still to be restored from Dummy Archive Storage", 
                        numFilesRemaining, numFilesRequested);
                return Collections.emptySet();
            }
        }
        logger.info("{}/{} files were successfully restored from Dummy Archive Storage", restoredFileCount, numFilesRequested);
        // getting the remaining dfInfos from the dfInfoFromLocation map
        // will tell us which files were not returned
        Set<DfInfo> dfInfosNotFound = new HashSet<>(dfInfoFromLocation.values());
        if (!dfInfosNotFound.isEmpty()) {
            logger.warn("The following {} files were not returned from Dummy Archive Storage:", dfInfosNotFound.size());
            for (DfInfo dfInfo : dfInfosNotFound) {
                logger.warn("File not returned: {}", dfInfo.getDfLocation());
            }
        }
        return dfInfosNotFound;
    }

    @Override
    public int getNumFilesRemaining() {
        return numFilesRemaining;
    }

    /**
     * Create the contents of a file by repeating the location/path of the file
     * as many times as necessary to create a file of the requested size. Each
     * line has a carriage return on the end and the last line will nearly 
     * always be truncated so that the file is exactly the right number of 
     * bytes.
     * 
     * @param location the (full) file path
     * @param fileSize the size of the file to create in bytes
     * @return a byte array containing the file contents
     */
    private static byte[] createFile(String location, int fileSize) {
        byte[] bytes = new byte[fileSize];
        String repeatString = location + "\n";
        for (int count=0; count<bytes.length; count++) {
            int charIndex = count % repeatString.length();
            bytes[count] = (byte)repeatString.charAt(charIndex);
        }
        return bytes;
    }

    /**
     * Calculate a dummy time that it might take to retrieve the file from
     * archive storage. This is based on the filesize but capped at a maximum
     * delay of 10 seconds so that excessively long waits are not created.
     * 
     * @param fileSize the size of the file in bytes
     * @return the number of milliseconds to wait for the file
     */
    private int calculateDummyTimeToGetFile(int fileSize) {
        int timeMs = fileSize/timeReductionFactor;
        if (timeMs > maxMsPerFile) {
            timeMs = maxMsPerFile;
        }
        return timeMs;
    }

    /**
     * Create a list of DatafileInfo objects (only the location/path will be
     * populated in each) by using a subset of the file list available to the
     * dummy archive storage. 
     * 
     * This provides a convenient way to create a list of files to request for
     * restore without knowing in advance what files are available via the 
     * dummy storage. 
     * 
     * @param startIndex
     * @param endIndex
     * @return a List of DfInfo (DatafileInfo) objects 
     */
    public List<DfInfo> createDfInfosList(int startIndex, int endIndex) {
        List<DfInfo> dfInfos = new ArrayList<>();
        int index = 0;
        for (String filePath : pathToSizeMap.keySet()) {
            if (index >= startIndex) {
                dfInfos.add(new DfInfoWithLocation(Paths.get(filePath)));
            }
            index++;
            if (index >= endIndex) {
                break;
            }
        }
        return dfInfos;
    }

}
