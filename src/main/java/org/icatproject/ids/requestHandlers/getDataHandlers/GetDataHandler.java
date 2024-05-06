package org.icatproject.ids.requestHandlers.getDataHandlers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.icatproject.IcatException_Exception;
import org.icatproject.ids.dataSelection.DataSelectionBase;
import org.icatproject.ids.enums.CallType;
import org.icatproject.ids.enums.OperationIdTypes;
import org.icatproject.ids.enums.RequestType;
import org.icatproject.ids.exceptions.BadRequestException;
import org.icatproject.ids.exceptions.DataNotOnlineException;
import org.icatproject.ids.exceptions.IdsException;
import org.icatproject.ids.exceptions.InsufficientPrivilegesException;
import org.icatproject.ids.exceptions.InternalException;
import org.icatproject.ids.exceptions.NotFoundException;
import org.icatproject.ids.exceptions.NotImplementedException;
import org.icatproject.ids.helpers.SO;
import org.icatproject.ids.helpers.ValueContainer;
import org.icatproject.ids.models.DataInfoBase;
import org.icatproject.ids.plugin.AlreadyLockedException;
import org.icatproject.ids.requestHandlers.base.RequestHandlerBase;
import org.icatproject.ids.services.ServiceProvider;
import org.icatproject.ids.services.LockManager.Lock;
import org.icatproject.ids.services.LockManager.LockType;

import jakarta.ws.rs.core.Response;
import static jakarta.ws.rs.core.HttpHeaders.CONTENT_LENGTH;

public abstract class GetDataHandler extends RequestHandlerBase {

    private Pattern rangeRe;
    private static AtomicLong atomicLong = new AtomicLong();


    public GetDataHandler(OperationIdTypes operationIdType) {
        super(operationIdType, RequestType.GETDATA );
    }


    public void init() throws InternalException {
        logger.info("Initializing GetDataHandler...");
        super.init();        
        this.rangeRe = Pattern.compile("bytes=(\\d+)-");
        logger.info("GetDataHandler initialized");
    }


    @Override
    public ValueContainer handle(HashMap<String, ValueContainer> parameters) throws BadRequestException, NotFoundException, InternalException, InsufficientPrivilegesException, DataNotOnlineException, NotImplementedException  {

        long offset = 0;
        var range = parameters.get("range");
        if ( range != null && range.getString() != null) {
            var rangeValue = range.getString();

            Matcher m = rangeRe.matcher(rangeValue);
            if (!m.matches()) {
                throw new BadRequestException("The range must match " + rangeRe.pattern());
            }
            offset = Long.parseLong(m.group(1));
            logger.debug("Range " + rangeValue + " -> offset " + offset);
        }

        return new ValueContainer(this.getData(parameters, offset));
    }

    protected abstract DataSelectionBase provideDataSelection(HashMap<String, ValueContainer> parameters, long offset) throws BadRequestException, InternalException, NotFoundException, InsufficientPrivilegesException, NotImplementedException;

    protected abstract ByteArrayOutputStream generateStreamForTransmitter(HashMap<String, ValueContainer> parameters, Long transferId) throws InternalException, IcatException_Exception, BadRequestException;

    protected abstract boolean mustZip(boolean zip, DataSelectionBase dataSelection);


    private Response getData(HashMap<String, ValueContainer> parameters, final long offset) throws BadRequestException,
            NotFoundException, InternalException, InsufficientPrivilegesException, NotFoundException, DataNotOnlineException, NotImplementedException {

        long start = System.currentTimeMillis();

        DataSelectionBase dataSelection = this.provideDataSelection(parameters, offset);

        final boolean zip = parameters.get("zip").getBool();
        final boolean compress = parameters.get("compress").getBool();
        String outname = parameters.get("outname").getString();
        String ip = parameters.get("request").getRequest().getRemoteAddr();
        var length = zip ? OptionalLong.empty() : dataSelection.getFileLength();

        Lock lock = null;
        try {
            var serviceProvider = ServiceProvider.getInstance();
            lock = serviceProvider.getLockManager().lock(dataSelection.getDsInfo().values(), LockType.SHARED);

            if (twoLevel) {
                dataSelection.checkOnline();
            }
            checkDatafilesPresent(dataSelection.getDfInfo().values());

            final boolean finalZip = this.mustZip(zip, dataSelection); 

            /* Construct the name to include in the headers */
            String name;
            if (outname == null) {
                if (finalZip) {
                    name = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date()) + ".zip";
                } else {
                    name = dataSelection.getDfInfo().values().iterator().next().getName();
                }
            } else {
                if (finalZip) {
                    String ext = outname.substring(outname.lastIndexOf(".") + 1, outname.length());
                    if ("zip".equals(ext)) {
                        name = outname;
                    } else {
                        name = outname + ".zip";
                    }
                } else {
                    name = outname;
                }
            }

            Long transferId = null;
            if (serviceProvider.getPropertyHandler().getLogSet().contains(CallType.READ)) {
                try {
                    transferId = atomicLong.getAndIncrement();
                    ByteArrayOutputStream baos = this.generateStreamForTransmitter(parameters, transferId);
                    serviceProvider.getTransmitter().processMessage("getDataStart", ip, baos.toString(), start);
                } catch (IcatException_Exception e) {
                    logger.error("Failed to prepare jms message " + e.getClass() + " " + e.getMessage());
                }
            }

            var response = Response.status(offset == 0 ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_PARTIAL)
                    .entity(new SO(dataSelection.getDsInfo(), dataSelection.getDfInfo(), offset, finalZip, compress, lock, transferId, ip, start, serviceProvider))
                    .header("Content-Disposition", "attachment; filename=\"" + name + "\"").header("Accept-Ranges", "bytes");
            length.stream()
                    .map(l -> Math.max(0L, l - offset))
                    .forEach(l -> response.header(CONTENT_LENGTH, l));
                
            return response.build();

        } catch (AlreadyLockedException e) {
            logger.debug("Could not acquire lock, getData failed");
            throw new DataNotOnlineException("Data is busy");
        } catch (IOException e) {
            // if (lock != null) {
            //     lock.release();
            // }
            logger.error("I/O error " + e.getMessage());
            throw new InternalException(e.getClass() + " " + e.getMessage());
        } catch (IdsException e) {
            lock.release();
            throw e;
        }
    }


    private void checkDatafilesPresent(Collection<DataInfoBase> dfInfos)
            throws NotFoundException, InternalException {

        var serviceProvider = ServiceProvider.getInstance();
        /* Check that datafiles have not been deleted before locking */
        int n = 0;
        StringBuffer sb = new StringBuffer("SELECT COUNT(df) from Datafile df WHERE (df.id in (");
        for (DataInfoBase dfInfo : dfInfos) {
            if (n != 0) {
                sb.append(',');
            }
            sb.append(dfInfo.getId());
            if (++n == serviceProvider.getPropertyHandler().getMaxIdsInQuery()) {
                try {
                    if (((Long) serviceProvider.getIcatReader().search(sb.append("))").toString()).get(0)).intValue() != n) {
                        throw new NotFoundException("One of the data files requested has been deleted");
                    }
                    n = 0;
                    sb = new StringBuffer("SELECT COUNT(df) from Datafile df WHERE (df.id in (");
                } catch (IcatException_Exception e) {
                    throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
                }
            }
        }
        if (n != 0) {
            try {
                if (((Long) serviceProvider.getIcatReader().search(sb.append("))").toString()).get(0)).intValue() != n) {
                    throw new NotFoundException("One of the datafiles requested has been deleted");
                }
            } catch (IcatException_Exception e) {
                throw new InternalException(e.getFaultInfo().getType() + " " + e.getMessage());
            }
        }

    }

}