package org.icatproject.ids.storage;

import javax.persistence.EntityManager;
import org.icatproject.ids.storage.local.LocalFileStorage;

public class StorageFactory {

    private static StorageFactory instance = null;

    private StorageFactory() {}

    public StorageInterface createStorageInterface(EntityManager em, String requestid) {
        return new LocalFileStorage();
    }

    public static StorageFactory getInstance() {
        if (instance == null) {
            instance = new StorageFactory();
        }
        return instance;
    }
}
