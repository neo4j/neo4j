/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormatPropertyConfigurator;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;

/**
 * Factory for Store implementations. Can also be used to create empty stores.
 */
public class StoreFactory
{
    public static final String LABELS_PART = ".labels";
    public static final String NAMES_PART = ".names";
    public static final String INDEX_PART = ".index";
    public static final String KEYS_PART = ".keys";
    public static final String ARRAYS_PART = ".arrays";
    public static final String STRINGS_PART = ".strings";
    public static final String NODE_STORE_NAME = ".nodestore.db";
    public static final String NODE_LABELS_STORE_NAME = NODE_STORE_NAME + LABELS_PART;
    public static final String PROPERTY_STORE_NAME = ".propertystore.db";
    public static final String PROPERTY_KEY_TOKEN_STORE_NAME = PROPERTY_STORE_NAME + INDEX_PART;
    public static final String PROPERTY_KEY_TOKEN_NAMES_STORE_NAME = PROPERTY_STORE_NAME + INDEX_PART + KEYS_PART;
    public static final String PROPERTY_STRINGS_STORE_NAME = PROPERTY_STORE_NAME + STRINGS_PART;
    public static final String PROPERTY_ARRAYS_STORE_NAME = PROPERTY_STORE_NAME + ARRAYS_PART;
    public static final String RELATIONSHIP_STORE_NAME = ".relationshipstore.db";
    public static final String RELATIONSHIP_TYPE_TOKEN_STORE_NAME = ".relationshiptypestore.db";
    public static final String RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME = RELATIONSHIP_TYPE_TOKEN_STORE_NAME +
                                                                          NAMES_PART;
    public static final String LABEL_TOKEN_STORE_NAME = ".labeltokenstore.db";
    public static final String LABEL_TOKEN_NAMES_STORE_NAME = LABEL_TOKEN_STORE_NAME + NAMES_PART;
    public static final String SCHEMA_STORE_NAME = ".schemastore.db";
    public static final String RELATIONSHIP_GROUP_STORE_NAME = ".relationshipgroupstore.db";
    public static final String COUNTS_STORE = ".counts.db";

    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final LogProvider logProvider;
    private final File neoStoreFileName;
    private final PageCache pageCache;
    private final RecordFormats recordFormats;
    private final OpenOption[] openOptions;
    private final VersionContextSupplier versionContextSupplier;

    public StoreFactory( File storeDir, Config config, IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction, LogProvider logProvider, VersionContextSupplier versionContextSupplier )
    {
        this( storeDir, config, idGeneratorFactory, pageCache, fileSystemAbstraction,
                selectForStoreOrConfig( config, storeDir, pageCache, logProvider ),
                logProvider, versionContextSupplier );
    }

    public StoreFactory( File storeDir, Config config, IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction, RecordFormats recordFormats, LogProvider logProvider,
            VersionContextSupplier versionContextSupplier )
    {
        this( storeDir, MetaDataStore.DEFAULT_NAME, config, idGeneratorFactory, pageCache, fileSystemAbstraction,
                recordFormats, logProvider, versionContextSupplier );
    }

    public StoreFactory( File storeDir, String storeName, Config config, IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache, FileSystemAbstraction fileSystemAbstraction, RecordFormats recordFormats,
            LogProvider logProvider, VersionContextSupplier versionContextSupplier, OpenOption... openOptions )
    {
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.versionContextSupplier = versionContextSupplier;
        this.recordFormats = recordFormats;
        this.openOptions = openOptions;
        new RecordFormatPropertyConfigurator( recordFormats, config ).configure();

        this.logProvider = logProvider;
        this.neoStoreFileName = new File( storeDir, storeName );
        this.pageCache = pageCache;
    }

    /**
     * Open {@link NeoStores} with all possible stores. If some store does not exist it will <b>not</b> be created.
     * @return container with all opened stores
     */
    public NeoStores openAllNeoStores()
    {
        return openNeoStores( false, StoreType.values() );
    }

    /**
     * Open {@link NeoStores} with all possible stores with a possibility to create store if it not exist.
     * @param createStoreIfNotExists - should store be created if it's not exist
     * @return container with all opened stores
     */
    public NeoStores openAllNeoStores( boolean createStoreIfNotExists )
    {
        return openNeoStores( createStoreIfNotExists, StoreType.values() );
    }

    /**
     * Open {@link NeoStores} for requested and store types. If requested store depend from non request store,
     * it will be automatically opened as well.
     * If some store does not exist it will <b>not</b> be created.
     * @param storeTypes - types of stores to be opened.
     * @return container with opened stores
     */
    public NeoStores openNeoStores( StoreType... storeTypes )
    {
        return openNeoStores( false, storeTypes );
    }

    /**
     * Open {@link NeoStores} for requested and store types. If requested store depend from non request store,
     * it will be automatically opened as well.
     * @param createStoreIfNotExists - should store be created if it's not exist
     * @param storeTypes - types of stores to be opened.
     * @return container with opened stores
     */
    public NeoStores openNeoStores( boolean createStoreIfNotExists, StoreType... storeTypes )
    {
        if ( createStoreIfNotExists )
        {
            try
            {
                fileSystemAbstraction.mkdirs( neoStoreFileName.getParentFile() );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException(
                        "Could not create store directory: " + neoStoreFileName.getParent(), e );
            }
        }
        return new NeoStores( neoStoreFileName, config, idGeneratorFactory, pageCache, logProvider,
                fileSystemAbstraction, versionContextSupplier, recordFormats, createStoreIfNotExists, storeTypes,
                openOptions );
    }
}
