package org.icatproject.ids.storage;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.bind.DatatypeConverter;

import org.icatproject.ids.plugin.DfInfo;
import org.icatproject.ids.plugin.MainStorageInterface;

import com.google.protobuf.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.stfc.storaged.ChunkedInputStream;
import uk.ac.stfc.storaged.StorageD;

/**
 * This is an updated version of the class ArchiveSDStorage from the StorageD 
 * IDS Plugin. Rather than extending AbstractArchiveStorage, this class
 * implements an updated version of ArchiveStorageInterface which defines the 
 * methods that are required to interact with the archive storage and allows 
 * for a test class to be defined, also implementing the updated 
 * ArchiveStorageInterface and allowing for this test class to return 
 * test/dummy files such that a connection to a real StorageD server is not 
 * required for the tests to run.
 * 
 * The original ArchiveSDStorage class from the plugin is no longer used but 
 * all of the other classes in the plugin are. Another thing to note is that 
 * the plugin is no longer installed as a separate component but is included
 * in the distribution of ids.r2dfoo as a dependency. 
 * 
 * Note that the code appears to be based on an SDClient class found in the 
 * StorageD SVN repository at:
 * https://svn.esc.rl.ac.uk/trac/StorageD/browser/StorageD_Code/trunk/java/uk/ac/stfc/storaged/SDClient.java
 * The code is not great but as it appears to work and I don't fully understand 
 * StorageD or the protocol that is used to communicate with it, I haven't 
 * attempted to modify it.
 */
public class ArchiveSDStorageV2 implements ArchiveStorageInterfaceV2 {

    private static final int MSG_FIELD_LEN = 8;

    private URI uri;
    // number of milliseconds to wait for the intial connection to StorageD
    // before a SocketTimeoutException is thrown
    private int connTimeout;
    // number of milliseconds to wait without reading any bytes from StorageD 
    // before a SocketTimeoutException is thrown
    private int readTimeout;

    private int numFilesRemaining;

    private static final Logger logger = LoggerFactory.getLogger(ArchiveSDStorageV2.class);

    // TODO: what exception to throw here. Need something different to the 
    // IOException used previously and different from the IOException thrown by
    // the restore method but InstantiationException is probably not right.
    public ArchiveSDStorageV2(Properties props) throws InstantiationException {
        try {
            String name = props.getProperty("plugin.archive.uri");
            if (name == null) {
                throw new InstantiationException("\"plugin.archive.uri\" is not defined");
            }
            logger.debug("Using property plugin.archive.uri: {}", name);
            uri = new URI(name);
            name = props.getProperty("plugin.archive.connectionTimeout");
            if (name == null) {
                throw new InstantiationException("\"plugin.archive.connectionTimeout\" is not defined");
            }
            logger.debug("Using property plugin.archive.connectionTimeout: {}", name);
            connTimeout = Integer.parseInt(name.trim());

            name = props.getProperty("plugin.archive.readTimeout");
            if (name == null) {
                throw new InstantiationException("\"plugin.archive.readTimeout\" is not defined");
            }
            logger.debug("Using property plugin.archive.readTimeout: {}", name);
            readTimeout = Integer.parseInt(name.trim());

        } catch (NumberFormatException | URISyntaxException e) {
            throw new InstantiationException(e.getClass() + " " + e.getMessage());
        }

        logger.info("ArchiveSDStorageV2 initialized");
    }

    private byte[] readData(InputStream in, int datasize) throws IOException {
        byte[] buffer = new byte[datasize];
        int totalread = 0;
        int read = 0;

        while (totalread < datasize) {
            read = in.read(buffer, totalread, datasize - totalread);
            totalread += read;
        }
        return buffer;
    }

    private void sendRequest(OutputStream out, StorageD.RequestType type, Message message) throws IOException {
        StorageD.Request.Builder reqbuilder = StorageD.Request.newBuilder();
        reqbuilder.setType(type);
        reqbuilder.setPayload(message.toByteString());
        StorageD.Request req = reqbuilder.build();
        // The first MSG_FIELD_LEN bytes should be the message length
        byte[] bytes = req.toByteArray();
        String size = String.format("%" + MSG_FIELD_LEN + "s", bytes.length).replace(" ", "0");
        out.write(size.getBytes());
        out.write(bytes);
        logger.debug("Sent request type {} in {} bytes to StorageD", type, bytes.length);
    }

    private StorageD.Response readResponse(InputStream stream) throws IOException {
        int length;
        byte[] lengthBytes = readData(stream, MSG_FIELD_LEN);
        try {
            length = Integer.parseInt(new String(lengthBytes));
        } catch (NumberFormatException e) {
            throw new IOException("Length of response starting (Hex) '" + DatatypeConverter.printHexBinary(lengthBytes)
                    + "' is not a valid integer");
        }
        byte[] responseData = readData(stream, length);
        StorageD.Response response = StorageD.Response.parseFrom(responseData);
        logger.debug("Received response type {} in {} bytes from StorageD.", response.getType(), length);
        return response;
    }

    @Override
    public Set<DfInfo> restore(MainStorageInterface mainStorageInterface, List<DfInfo> dfInfos, AtomicBoolean stopRestoring) throws IOException {
        int numFilesRequested = dfInfos.size();
        logger.info("Requesting files from StorageD: {}", numFilesRequested);
        numFilesRemaining = numFilesRequested;
        StorageD.Fileset fileset = null;
        Map<String, DfInfo> dfInfoFromLocation = new HashMap<>();
        StorageD.Fileset.Builder filesetbuilder = StorageD.Fileset.newBuilder();
        for (DfInfo dfInfo : dfInfos) {
            String location = dfInfo.getDfLocation();
            dfInfoFromLocation.put(location, dfInfo);
            StorageD.File.Builder filebuilder = StorageD.File.newBuilder();
            filebuilder.setFilename(location);
            StorageD.File file = filebuilder.build();
            filesetbuilder.addFile(file);
        }
        fileset = filesetbuilder.build();

        try (Socket sock = new Socket()) {
            String host = uri.getHost();
            int port = uri.getPort();
            logger.debug("Connecting to {}:{}", host, port);
            SocketAddress endpoint = new InetSocketAddress(host, port);
            sock.connect(endpoint, connTimeout);
            sock.setSoTimeout(readTimeout);

            OutputStream outputStream = sock.getOutputStream();
            sendRequest(outputStream, StorageD.RequestType.GET_FILES, fileset);
            ChunkedInputStream chunkedInputStream = new ChunkedInputStream(
                    new BufferedInputStream(sock.getInputStream()));

            logger.info("Connected to {}:{}", host, port);
            int returnedFileCount = 0;
            while (true) {
                StorageD.Response resp = readResponse(chunkedInputStream);
                if (resp.getType() == StorageD.ResponseType.FILE_DETAILS) {
                    fileset = StorageD.Fileset.parseFrom(resp.getPayload());
                    logger.info("Restoring fileset containing {} files.", fileset.getFileCount());
                    for (StorageD.File returnedFile : fileset.getFileList()) {
                        returnedFileCount++;
                        String location = returnedFile.getFilename();
                        if (dfInfoFromLocation.containsKey(location)) {
                            // remove each returned file so that only
                            // the failures are left at the end
                            dfInfoFromLocation.remove(location);
                        } else {
                            logger.warn("Returned file with location {} was not requested!", location);
                        }
                        long returnedFileSize = returnedFile.getFilesize();
                        chunkedInputStream.setFileSize(returnedFileSize);
                        logger.debug("{}/{} Restoring {} bytes to {}.", 
                            new Object[]{returnedFileCount, numFilesRequested, returnedFileSize, location});
                        mainStorageInterface.put(chunkedInputStream, location);
                        numFilesRemaining--;
                        // check whether the stop flag has been set
                        if (stopRestoring.get()) {
                            // if it is then this is a safe place to exit
                            logger.info("Stopping restore with {}/{} files still to be restored from StorageD", 
                                    numFilesRemaining, numFilesRequested);
                            return Collections.emptySet();
                        }
                    }
                    chunkedInputStream.setFileSize(null);
                } else if (resp.getType() == StorageD.ResponseType.STATUS) {
                    // A status response is returned both after some filesets have been returned and
                    // also if no files were found, but the response from StorageD is not correct. 
                    // see: https://svn.esc.rl.ac.uk/trac/StorageD/ticket/10

                    logger.info("{}/{} files were successfully restored from StorageD", returnedFileCount, numFilesRequested);
                    // Getting the remaining dfInfos from the dfInfoFromLocation map
                    // will tell us which files were not returned anyway.
                    Set<DfInfo> dfInfosNotFound = new HashSet<>(dfInfoFromLocation.values());
                    if (!dfInfosNotFound.isEmpty()) {
                        logger.warn("The following {} files were not returned from StorageD:", dfInfosNotFound.size());
                        for (DfInfo dfInfo : dfInfosNotFound) {
                            logger.warn("File not returned: id: {}, location: {}", dfInfo.getDfId(), dfInfo.getDfLocation());
                        }
                    }
                    return dfInfosNotFound;
                }
            }
        } catch (IOException e) {
            logger.error("IOException restoring from StorageD: {} {}", e.getClass(), e.getMessage());
            throw e;
        }
    }

    @Override
    public int getNumFilesRemaining() {
        return numFilesRemaining;
    }

}