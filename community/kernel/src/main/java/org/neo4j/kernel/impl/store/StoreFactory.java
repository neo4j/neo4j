/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

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

    private Config config;
    @SuppressWarnings( "deprecation" )
    private IdGeneratorFactory idGeneratorFactory;
    private FileSystemAbstraction fileSystemAbstraction;
    private LogProvider logProvider;
    private File neoStoreFileName;
    private PageCache pageCache;

    public StoreFactory()
    {
    }

    @SuppressWarnings( "deprecation" )
    public StoreFactory( FileSystemAbstraction fileSystem, File storeDir, PageCache pageCache, LogProvider logProvider )
    {
        this( storeDir, new Config(), new DefaultIdGeneratorFactory( fileSystem ), pageCache, fileSystem, logProvider );
    }

    public StoreFactory( File storeDir, Config config,
            @SuppressWarnings( "deprecation" ) IdGeneratorFactory idGeneratorFactory, PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction, LogProvider logProvider )
    {
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        setLogProvider( logProvider );
        setStoreDir( storeDir );
        this.pageCache = pageCache;
    }

    public void setConfig( Config config )
    {
        this.config = config;
    }

    public void setIdGeneratorFactory( IdGeneratorFactory idGeneratorFactory )
    {
        this.idGeneratorFactory = idGeneratorFactory;
    }

    public void setFileSystemAbstraction( FileSystemAbstraction fileSystemAbstraction )
    {
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    public void setLogProvider( LogProvider logProvider )
    {
        this.logProvider = logProvider;
    }

    public void setStoreDir( File storeDir )
    {
        this.neoStoreFileName = new File( storeDir, MetaDataStore.DEFAULT_NAME );
    }

    public void setPageCache( PageCache pageCache )
    {
        this.pageCache = pageCache;
    }

    /**
     * Open {@link NeoStores} with all possible stores. If some store does not exist it will <b>not</b> be created.
     * @return container with all opened stores
     */
    public NeoStores openAllNeoStores()
    {
        return openNeoStores( false, NeoStores.StoreType.values() );
    }

    /**
     * Open {@link NeoStores} with all possible stores with a possibility to create store if it not exist.
     * @param createStoreIfNotExists - should store be created if it's not exist
     * @return container with all opened stores
     */
    public NeoStores openAllNeoStores( boolean createStoreIfNotExists )
    {
        return openNeoStores( createStoreIfNotExists, NeoStores.StoreType.values() );
    }

    /**
     * Open {@link NeoStores} for requested and store types. If requested store depend from non request store,
     * it will be automatically opened as well.
     * If some store does not exist it will <b>not</b> be created.
     * @param storeTypes - types of stores to be opened.
     * @return container with opened stores
     */
    public NeoStores openNeoStores( NeoStores.StoreType... storeTypes )
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
    public NeoStores openNeoStores( boolean createStoreIfNotExists, NeoStores.StoreType... storeTypes )
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
                fileSystemAbstraction, createStoreIfNotExists, storeTypes );
    }

    public abstract static class Configuration
    {
        public static final Setting<Integer> string_block_size = GraphDatabaseSettings.string_block_size;
        public static final Setting<Integer> array_block_size = GraphDatabaseSettings.array_block_size;
    }
}
