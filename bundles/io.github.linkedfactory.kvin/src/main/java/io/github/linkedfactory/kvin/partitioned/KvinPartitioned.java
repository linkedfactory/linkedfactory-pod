package io.github.linkedfactory.kvin.partitioned;

import io.github.linkedfactory.kvin.Kvin;
import io.github.linkedfactory.kvin.KvinListener;
import io.github.linkedfactory.kvin.KvinTuple;
import io.github.linkedfactory.kvin.archive.DatabaseArchiver;
import io.github.linkedfactory.kvin.leveldb.KvinLevelDb;
import io.github.linkedfactory.kvin.parquet.KvinParquet;
import net.enilink.commons.iterator.IExtendedIterator;
import net.enilink.commons.iterator.NiceIterator;
import net.enilink.komma.core.URI;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KvinPartitioned implements Kvin {
    ArrayList<KvinListener> listeners = new ArrayList<>();
    ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);
    Future<?> archivalTaskfuture;
    File currentStorePath, oldLevelDbHotDataStorePath;
    KvinLevelDb levelDbHotDataStore, oldLevelDbHotDataStore;
    KvinParquet archiveStore;
    AtomicBoolean isArchivalInProcess = new AtomicBoolean(false);


    public KvinPartitioned(File storePath, long archivalSize, TimeUnit timeUnit) {
        this.currentStorePath = storePath;
        levelDbHotDataStore = new KvinLevelDb(storePath);
        oldLevelDbHotDataStore = levelDbHotDataStore;
        archiveStore = new KvinParquet("./target/archive/");
        archivalTaskfuture = scheduledExecutorService.scheduleAtFixedRate(new ArchivalHandler(this), archivalSize, archivalSize, timeUnit);
    }

    @Override
    public boolean addListener(KvinListener listener) {
        try {
            listeners.add(listener);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean removeListener(KvinListener listener) {
        try {
            listeners.remove(listener);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(KvinTuple... tuples) {
        this.put(Arrays.asList(tuples));
    }

    @Override
    public void put(Iterable<KvinTuple> tuples) {
        try {
            putInternal(tuples);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void putInternal(Iterable<KvinTuple> tuples) throws IOException {
        for (KvinTuple tuple : tuples) {
            levelDbHotDataStore.put(Collections.singletonList(tuple));
        }
    }

    public synchronized void createNewHotDataStore() {
        oldLevelDbHotDataStorePath = this.currentStorePath;
        this.currentStorePath = new File(currentStorePath.getPath() + System.currentTimeMillis());
        levelDbHotDataStore.close();
        oldLevelDbHotDataStore.close();
        levelDbHotDataStore = new KvinLevelDb(currentStorePath);
        oldLevelDbHotDataStore = new KvinLevelDb(oldLevelDbHotDataStorePath);
    }

    public KvinLevelDb getOldLevelDbHotDataStore() {
        return oldLevelDbHotDataStore;
    }

    public void deleteOldLevelDbHotDataStore() throws IOException {
        FileUtils.deleteDirectory(new File(oldLevelDbHotDataStorePath.getAbsolutePath()));
    }

    public NiceIterator<KvinTuple> readCurrentHotStore() {
        return new DatabaseArchiver(levelDbHotDataStore, archiveStore).getDatabaseIterator();
    }

    public boolean isArchivalInProcess() {
        return isArchivalInProcess.get();
    }

    public void setArchivalInProcess(boolean status) {
        isArchivalInProcess.set(status);
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long limit) {
        return fetchInternal(item, property, context, null, null, limit, null, null);
    }

    @Override
    public IExtendedIterator<KvinTuple> fetch(URI item, URI property, URI context, long end, long begin, long limit, long interval, String op) {
        return fetchInternal(item, property, context, end, begin, limit, interval, op);
    }

    private IExtendedIterator<KvinTuple> fetchInternal(URI item, URI property, URI context, Long end, Long begin, Long limit, Long interval, String op) {

        return new NiceIterator<>() {

            boolean isReadingFromHotStore = true;
            boolean isReadingFromArchiveStore = false;
            boolean isEndOfAllRead = false;
            IExtendedIterator<KvinTuple> hotStoreResult = null;
            IExtendedIterator<KvinTuple> archiveStoreResult = null;
            KvinTuple lazyReadInitialArchiveStoreTuple = null;


            @Override
            public boolean hasNext() {
                return !isEndOfAllRead && isItemRecordExist();
            }

            @Override
            public KvinTuple next() {
                KvinTuple tuple = null;
                if (isReadingFromHotStore) {
                    tuple = readFromHotStore();
                } else if (isReadingFromArchiveStore) {
                    tuple = readFromArchiveStore(false);
                }
                return tuple;
            }

            private KvinTuple readFromHotStore() {
                KvinTuple tuple;

                tuple = hotStoreResult.hasNext() ? hotStoreResult.next() : null;
                boolean isNextTupleAvailable = hotStoreResult.hasNext();

                if (!isNextTupleAvailable || tuple == null) {
                    isReadingFromHotStore = false;
                    lazyReadInitialArchiveStoreTuple = readFromArchiveStore(true);
                    if (lazyReadInitialArchiveStoreTuple == null) {
                        isEndOfAllRead = true;
                    }
                }

                return tuple;
            }

            private KvinTuple readFromArchiveStore(boolean lazyRead) {
                KvinTuple tuple;

                if (lazyRead) {
                    if (interval != null && op != null) {
                        archiveStoreResult = archiveStore.fetch(item, property, context, end, begin, limit, interval, op);
                    } else {
                        archiveStoreResult = archiveStore.fetch(item, property, context, limit);
                    }
                    tuple = archiveStoreResult.hasNext() ? archiveStoreResult.next() : null;
                } else {
                    if (lazyReadInitialArchiveStoreTuple == null) {
                        if (archiveStoreResult == null) {
                            if (interval != null && op != null) {
                                archiveStoreResult = archiveStore.fetch(item, property, context, end, begin, limit, interval, op);
                            } else {
                                archiveStoreResult = archiveStore.fetch(item, property, context, limit);
                            }
                        }

                        tuple = archiveStoreResult.next();
                        boolean isNextTupleExist = archiveStoreResult.hasNext();
                        if (tuple == null || !isNextTupleExist) {
                            isReadingFromArchiveStore = false;
                            isEndOfAllRead = true;
                        }
                    } else {
                        tuple = lazyReadInitialArchiveStoreTuple;
                        lazyReadInitialArchiveStoreTuple = null;
                    }
                }
                return tuple;
            }

            private boolean isItemRecordExist() {
                if (hotStoreResult == null) {
                    // reading from oldHotStore(read-only store) if archival is in process
                    if (isArchivalInProcess()) {
                        if (interval != null && op != null) {
                            hotStoreResult = oldLevelDbHotDataStore.fetch(item, property, context, end, begin, limit, interval, op);
                        } else {
                            hotStoreResult = oldLevelDbHotDataStore.fetch(item, property, context, limit);
                        }
                    }

                    // reading from hotStore when archival is not in process
                    if (hotStoreResult == null && interval != null && op != null) {
                        hotStoreResult = levelDbHotDataStore.fetch(item, property, context, end, begin, limit, interval, op);
                    } else if (hotStoreResult == null) {
                        hotStoreResult = levelDbHotDataStore.fetch(item, property, context, limit);
                    }
                }

                // fallback to archiveStore
                if (!hotStoreResult.hasNext()) {
                    if (interval != null && op != null) {
                        archiveStoreResult = archiveStore.fetch(item, property, context, end, begin, limit, interval, op);
                    } else {
                        archiveStoreResult = archiveStore.fetch(item, property, context, limit);
                    }
                    isReadingFromArchiveStore = archiveStoreResult.hasNext();
                    isReadingFromHotStore = false;
                } else {
                    isReadingFromHotStore = true;
                }
                return isReadingFromHotStore || isReadingFromArchiveStore;
            }

            @Override
            public void close() {
                super.close();
            }
        };
    }

    @Override
    public long delete(URI item, URI property, URI context, long end, long begin) {
        return 0;
    }

    @Override
    public boolean delete(URI item) {
        return false;
    }

    @Override
    public IExtendedIterator<URI> descendants(URI item) {
        return null;
    }

    @Override
    public IExtendedIterator<URI> descendants(URI item, long limit) {
        return null;
    }

    @Override
    public IExtendedIterator<URI> properties(URI item) {
        IExtendedIterator<URI> hotStoreResult = null;
        IExtendedIterator<URI> archiveStoreResult = null;

        // reading from oldHotStore(read-only store) if archival is in process
        if (isArchivalInProcess()) {
            hotStoreResult = oldLevelDbHotDataStore.properties(item);
        }
        // reading from hotStore when archival is not in process
        if (hotStoreResult == null) {
            hotStoreResult = levelDbHotDataStore.properties(item);
        }
        // fallback to archiveStore
        if (!hotStoreResult.hasNext()) {
            archiveStoreResult = archiveStore.properties(item);
        }

        return hotStoreResult.hasNext() ? hotStoreResult : archiveStoreResult;
    }

    @Override
    public long approximateSize(URI item, URI property, URI context, long end, long begin) {
        return 0;
    }

    @Override
    public void close() {
        levelDbHotDataStore.close();
        if (oldLevelDbHotDataStore != null) oldLevelDbHotDataStore.close();
        archivalTaskfuture.cancel(false);
    }
}
