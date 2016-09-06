package org.neo4j.storageengine.api;

import java.io.File;
import java.util.Optional;

import org.neo4j.kernel.impl.store.StoreType;

public class StoreFileMetadata
{
    private final File file;
    private final Optional<StoreType> storeType;
    private final int recordSize;

    public StoreFileMetadata( File file, Optional<StoreType> storeType, int recordSize )
    {
        this.file = file;
        this.storeType = storeType;
        this.recordSize = recordSize;
    }

    public File file()
    {
        return file;
    }

    public Optional<StoreType> storeType()
    {
        return storeType;
    }

    public int recordSize()
    {
        return recordSize;
    }
}
