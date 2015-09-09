/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.counts.ReadOnlyCountsTracker;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;

/**
 * This class contains the references to the "NodeStore,RelationshipStore,
 * PropertyStore and RelationshipTypeStore". NeoStores doesn't actually "store"
 * anything but extends the AbstractStore for the "type and version" validation
 * performed in there.
 */
public class NeoStores implements AutoCloseable
{
    public static boolean isStorePresent( PageCache pageCache, File storeDir )
    {
        File metaDataStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        try ( PagedFile ignore = pageCache.map( metaDataStore, MetaDataStore.getPageSize( pageCache ) ) )
        {
            return true;
        }
        catch ( IOException e )
        {
            return false;
        }
    }

    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final PageCache pageCache;
    private final LogProvider logProvider;
    private final boolean createIfNotExist;
    private final File storeDir;
    private final File neoStoreFileName;
    private final FileSystemAbstraction fileSystemAbstraction;

    private MetaDataStore metaDataStore;
    private NodeStore nodeStore;
    private DynamicArrayStore nodeLabelStore;
    private PropertyStore propStore;
    private PropertyKeyTokenStore propertyKeyTokenStore;
    private DynamicStringStore propertyKeyTokenNamesStore;
    private DynamicStringStore propertyStringStore;
    private DynamicArrayStore propertyArrayStore;
    private RelationshipStore relationshipStore;
    private RelationshipTypeTokenStore relTypeStore;
    private DynamicStringStore relTypeTokenNameStore;
    private LabelTokenStore labelTokenStore;
    private DynamicStringStore labelTokenNamesStore;
    private SchemaStore schemaStore;
    private RelationshipGroupStore relGroupStore;
    private CountsTracker counts;

    NeoStores(
            File neoStoreFileName,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            final LogProvider logProvider,
            FileSystemAbstraction fileSystemAbstraction,
            boolean createIfNotExist )
    {
        this.neoStoreFileName = neoStoreFileName;
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.pageCache = pageCache;
        this.logProvider = logProvider;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.createIfNotExist = createIfNotExist;
        this.storeDir = neoStoreFileName.getParentFile();
    }

    public StoreStatement acquireStatement()
    {
        return new StoreStatement( this );
    }

    public File getStoreDir()
    {
        return storeDir;
    }

    private File getStoreFileName( String substoreName )
    {
        return new File( neoStoreFileName.getPath() + substoreName );
    }

    /**
     * Closes the node,relationship,property and relationship type stores.
     */
    public void close()
    {
        if ( relTypeTokenNameStore != null )
        {
            relTypeTokenNameStore.close();
            relTypeTokenNameStore = null;
        }
        if ( relTypeStore != null )
        {
            relTypeStore.close();
            relTypeStore = null;
        }
        if ( labelTokenNamesStore != null )
        {
            labelTokenNamesStore.close();
            labelTokenNamesStore = null;
        }
        if ( labelTokenStore != null )
        {
            labelTokenStore.close();
            labelTokenStore = null;
        }
        if ( propertyKeyTokenNamesStore != null )
        {
            propertyKeyTokenNamesStore.close();
            propertyKeyTokenNamesStore = null;
        }
        if ( propertyKeyTokenStore != null )
        {
            propertyKeyTokenStore.close();
            propertyKeyTokenStore = null;
        }
        if ( propertyArrayStore != null )
        {
            propertyArrayStore.close();
            propertyArrayStore = null;
        }
        if ( propertyStringStore != null )
        {
            propertyStringStore.close();
            propertyStringStore = null;
        }
        if ( propStore != null )
        {
            propStore.close();
            propStore = null;
        }
        if ( relationshipStore != null )
        {
            relationshipStore.close();
            relationshipStore = null;
        }
        if ( nodeLabelStore != null )
        {
            nodeLabelStore.close();
            nodeLabelStore = null;
        }
        if ( nodeStore != null )
        {
            nodeStore.close();
            nodeStore = null;
        }
        if ( schemaStore != null )
        {
            schemaStore.close();
            schemaStore = null;
        }
        if ( relGroupStore != null )
        {
            relGroupStore.close();
            relGroupStore = null;
        }
        if ( counts != null )
        {
            try
            {
                counts.rotate( getMetaDataStore().getLastCommittedTransactionId() );
                counts.shutdown();
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
            finally
            {
                counts = null;
            }
        }
        if ( metaDataStore != null )
        {
            metaDataStore.close();
            metaDataStore = null;
        }
    }

    public void flush()
    {
        try
        {
            if ( counts != null )
            {
                counts.rotate( metaDataStore.getLastCommittedTransactionId() );
            }
            pageCache.flushAndForce();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to flush", e );
        }
    }

    /**
     * @return the NeoStore.
     */
    public synchronized MetaDataStore getMetaDataStore()
    {
        if ( metaDataStore == null )
        {
            metaDataStore = initialise(
                    new MetaDataStore( neoStoreFileName, config, idGeneratorFactory, pageCache, logProvider ) );
        }
        return metaDataStore;
    }

    private <T extends CommonAbstractStore> T initialise( T store )
    {
        store.initialise( createIfNotExist );
        return store;
    }

    /**
     * @return The node store
     */
    public synchronized NodeStore getNodeStore()
    {
        if ( nodeStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.NODE_STORE_NAME );
            DynamicArrayStore nodeLabelStore = getNodeLabelStore();
            nodeStore = initialise(
                    new NodeStore( fileName, config, idGeneratorFactory, pageCache, logProvider, nodeLabelStore ) );
        }
        return nodeStore;
    }

    private synchronized DynamicArrayStore getNodeLabelStore()
    {
        if ( nodeLabelStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.NODE_LABELS_STORE_NAME );
            int blockSizeFromConfiguration = config.get( GraphDatabaseSettings.label_block_size );
            nodeLabelStore = initialise(
                    new DynamicArrayStore( fileName, config, IdType.NODE_LABELS, idGeneratorFactory, pageCache,
                            logProvider, blockSizeFromConfiguration ) );
        }
        return nodeLabelStore;
    }

    /**
     * The relationship store.
     *
     * @return The relationship store
     */
    public synchronized RelationshipStore getRelationshipStore()
    {
        if ( relationshipStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.RELATIONSHIP_STORE_NAME );
            relationshipStore = initialise(
                    new RelationshipStore( fileName, config, idGeneratorFactory, pageCache, logProvider ) );
        }
        return relationshipStore;
    }

    /**
     * Returns the relationship type store.
     *
     * @return The relationship type store
     */
    public synchronized RelationshipTypeTokenStore getRelationshipTypeTokenStore()
    {
        if ( relTypeStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME );
            relTypeStore = initialise(
                    new RelationshipTypeTokenStore( fileName, config, idGeneratorFactory, pageCache, logProvider,
                    getRelationshipTypeTokenNamesStore() ) );
        }
        return relTypeStore;
    }

    private synchronized DynamicStringStore getRelationshipTypeTokenNamesStore()
    {
        if ( relTypeTokenNameStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME );
            relTypeTokenNameStore = initialise(
                    new DynamicStringStore( fileName, config, IdType.RELATIONSHIP_TYPE_TOKEN_NAME, idGeneratorFactory,
                            pageCache, logProvider, TokenStore.NAME_STORE_BLOCK_SIZE ) );
        }
        return relTypeTokenNameStore;
    }

    /**
     * Returns the label store.
     *
     * @return The label store
     */
    public synchronized LabelTokenStore getLabelTokenStore()
    {
        if ( labelTokenStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.LABEL_TOKEN_STORE_NAME );
            labelTokenStore =  initialise(
                    new LabelTokenStore( fileName, config, idGeneratorFactory, pageCache, logProvider,
                            getLabelTokenNamesStore() ) );
        }
        return labelTokenStore;
    }

    private synchronized DynamicStringStore getLabelTokenNamesStore()
    {
        if ( labelTokenNamesStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME );
            labelTokenNamesStore = initialise(
                    new DynamicStringStore( fileName, config, IdType.LABEL_TOKEN_NAME, idGeneratorFactory, pageCache,
                            logProvider, TokenStore.NAME_STORE_BLOCK_SIZE ) );
        }
        return labelTokenNamesStore;
    }

    /**
     * Returns the property store.
     *
     * @return The property store
     */
    public synchronized PropertyStore getPropertyStore()
    {
        if ( propStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.PROPERTY_STORE_NAME );
            propStore = initialise(
                    new PropertyStore( fileName, config, idGeneratorFactory, pageCache, logProvider,
                            getStringPropertyStore(), getPropertyKeyTokenStore(), getArrayPropertyStore() ) );
        }
        return propStore;
    }

    private synchronized DynamicStringStore getStringPropertyStore()
    {
        if ( propertyStringStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME );
            int blockSizeFromConfiguration = config.get( GraphDatabaseSettings.string_block_size );
            propertyStringStore = initialise(
                    new DynamicStringStore( fileName, config, IdType.STRING_BLOCK, idGeneratorFactory, pageCache,
                            logProvider, blockSizeFromConfiguration ) );
        }
        return propertyStringStore;
    }

    private synchronized DynamicArrayStore getArrayPropertyStore()
    {
        if ( propertyArrayStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME );
            int blockSizeFromConfiguration = config.get( GraphDatabaseSettings.array_block_size );
            propertyArrayStore = initialise(
                    new DynamicArrayStore( fileName, config, IdType.ARRAY_BLOCK, idGeneratorFactory, pageCache,
                            logProvider, blockSizeFromConfiguration ) );
        }
        return propertyArrayStore;
    }

    /**
     * @return the {@link PropertyKeyTokenStore}
     */
    public synchronized PropertyKeyTokenStore getPropertyKeyTokenStore()
    {
        if ( propertyKeyTokenStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME );
            propertyKeyTokenStore = initialise(
                    new PropertyKeyTokenStore( fileName, config, idGeneratorFactory, pageCache, logProvider,
                            getPropertyKeyTokenNamesStore() ) );
        }
        return propertyKeyTokenStore;
    }

    private synchronized DynamicStringStore getPropertyKeyTokenNamesStore()
    {
        if ( propertyKeyTokenNamesStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME );
            propertyKeyTokenNamesStore = initialise(
                    new DynamicStringStore( fileName, config, IdType.PROPERTY_KEY_TOKEN_NAME, idGeneratorFactory,
                            pageCache, logProvider, TokenStore.NAME_STORE_BLOCK_SIZE ) );
        }
        return propertyKeyTokenNamesStore;
    }

    /**
     * @return the {@link RelationshipGroupStore}
     */
    public synchronized RelationshipGroupStore getRelationshipGroupStore()
    {
        if ( relGroupStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.RELATIONSHIP_GROUP_STORE_NAME );
            relGroupStore = initialise(
                    new RelationshipGroupStore( fileName, config, idGeneratorFactory, pageCache, logProvider ) );
        }
        return relGroupStore;
    }

    /**
     * @return the schema store.
     */
    public synchronized SchemaStore getSchemaStore()
    {
        if ( schemaStore == null )
        {
            File fileName = getStoreFileName( StoreFactory.SCHEMA_STORE_NAME );
            schemaStore = initialise(
                    new SchemaStore( fileName, config, IdType.SCHEMA, idGeneratorFactory, pageCache, logProvider ) );
        }
        return schemaStore;
    }

    public synchronized CountsTracker getCounts()
    {
        if ( counts == null )
        {
            File fileName = getStoreFileName( StoreFactory.COUNTS_STORE );
            boolean readOnly = config.get( GraphDatabaseSettings.read_only );
            counts = readOnly ? createReadOnlyCountsTracker( fileName ) : createWritableCountsTracker( fileName );

            counts.setInitializer( new DataInitializer<CountsAccessor.Updater>()
            {
                private Log log = logProvider.getLog( MetaDataStore.class );

                @Override
                public void initialize( CountsAccessor.Updater updater )
                {
                    log.warn( "Missing counts store, rebuilding it." );
                    new CountsComputer( NeoStores.this ).initialize( updater );
                }

                @Override
                public long initialVersion()
                {
                    return getMetaDataStore().getLastCommittedTransactionId();
                }
            } );

            try
            {
                counts.init(); // TODO: move this to LifeCycle
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Failed to initialize counts store", e );
            }
        }
        return counts;
    }

    private CountsTracker createWritableCountsTracker( File fileName )
    {

        return new CountsTracker( logProvider, fileSystemAbstraction, pageCache, config, fileName );
    }

    private ReadOnlyCountsTracker createReadOnlyCountsTracker( File fileName )
    {
        return new ReadOnlyCountsTracker( logProvider, fileSystemAbstraction, pageCache, config, fileName );
    }

    public void makeStoreOk()
    {
        getRelationshipTypeTokenStore().makeStoreOk();
        getLabelTokenStore().makeStoreOk();
        getPropertyStore().makeStoreOk();
        getRelationshipStore().makeStoreOk();
        getNodeStore().makeStoreOk();
        getSchemaStore().makeStoreOk();
        getRelationshipGroupStore().makeStoreOk();
        getMetaDataStore().makeStoreOk();
    }

    public void rebuildIdGenerators()
    {
        getRelationshipTypeTokenStore().rebuildIdGenerator();
        getLabelTokenStore().rebuildIdGenerator();
        getPropertyStore().rebuildIdGenerator();
        getRelationshipStore().rebuildIdGenerator();
        getNodeStore().rebuildIdGenerator();
        getSchemaStore().rebuildIdGenerator();
        getRelationshipGroupStore().rebuildIdGenerator();
        getMetaDataStore().rebuildIdGenerator();
    }

    /**
     * Throws cause of store not being OK.
     */
    public void verifyStoreOk()
    {
        visitStore( new Visitor<CommonAbstractStore,RuntimeException>()
        {
            @Override
            public boolean visit( CommonAbstractStore element )
            {
                element.checkStoreOk();
                return false;
            }
        } );
    }

    public void logVersions( Logger msgLog )
    {
        msgLog.log( "Store versions:" );
        getMetaDataStore().logVersions( msgLog );
        getSchemaStore().logVersions( msgLog );
        getNodeStore().logVersions( msgLog );
        getRelationshipStore().logVersions( msgLog );
        getRelationshipTypeTokenStore().logVersions( msgLog );
        getLabelTokenStore().logVersions( msgLog );
        getPropertyStore().logVersions( msgLog );
        getRelationshipGroupStore().logVersions( msgLog );
    }

    public void logIdUsage( Logger msgLog )
    {
        msgLog.log( "Id usage:" );
        getSchemaStore().logIdUsage( msgLog );
        getNodeStore().logIdUsage( msgLog );
        getRelationshipStore().logIdUsage( msgLog );
        getRelationshipTypeTokenStore().logIdUsage( msgLog );
        getLabelTokenStore().logIdUsage( msgLog );
        getPropertyStore().logIdUsage( msgLog );
        getRelationshipGroupStore().logIdUsage( msgLog );
    }

    /**
     * Visits this store, and any other store managed by this store.
     * TODO this could, and probably should, replace all override-and-do-the-same-thing-to-all-my-managed-stores
     * methods like:
     * {@link #makeStoreOk()},
     * {@link #close()} (where that method could be deleted all together and do a visit in {@link #close()}),
     * {@link #logIdUsage(Logger)},
     * {@link #logVersions(Logger)},
     * For a good samaritan to pick up later.
     */
    public void visitStore( Visitor<CommonAbstractStore,RuntimeException> visitor )
    {
        getNodeStore().visitStore( visitor );
        getRelationshipStore().visitStore( visitor );
        getRelationshipGroupStore().visitStore( visitor );
        getRelationshipTypeTokenStore().visitStore( visitor );
        getLabelTokenStore().visitStore( visitor );
        getPropertyStore().visitStore( visitor );
        getSchemaStore().visitStore( visitor );
        getMetaDataStore().visitStore( visitor );
    }

    public void rebuildCountStoreIfNeeded() throws IOException
    {
        // TODO: move this to LifeCycle
        getCounts().start();
    }

    public void assertOpen()
    {
        if ( nodeStore == null )
        {
            throw new IllegalStateException( "Database has been shutdown" );
        }
    }
}
