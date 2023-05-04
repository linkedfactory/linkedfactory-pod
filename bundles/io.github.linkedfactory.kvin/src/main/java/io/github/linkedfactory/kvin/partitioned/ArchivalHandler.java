package io.github.linkedfactory.kvin.partitioned;

import io.github.linkedfactory.kvin.archive.DatabaseArchiver;
import io.github.linkedfactory.kvin.parquet.KvinParquet;

import java.io.IOException;

public class ArchivalHandler implements Runnable {
    KvinPartitioned kvinPartitioned;
    KvinParquet archiveStore;

    public ArchivalHandler(KvinPartitioned kvinPartitioned) {
        this.kvinPartitioned = kvinPartitioned;
        archiveStore = new KvinParquet("./target/archive/");
    }

    @Override
    public void run() {
        try {
            this.kvinPartitioned.setArchivalInProcess(true);
            this.kvinPartitioned.createNewHotDataStore();
            new DatabaseArchiver(this.kvinPartitioned.getOldLevelDbHotDataStore(), archiveStore).archive();
            this.kvinPartitioned.setArchivalInProcess(false);
            this.kvinPartitioned.deleteOldLevelDbHotDataStore();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
