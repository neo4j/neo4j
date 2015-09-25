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
import java.util.Iterator;

import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorWrapper;
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

import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

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

    private enum StoreType
    {
        NODE_LABEL
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.NODE_LABELS_STORE_NAME );
                int blockSizeFromConfiguration = me.config.get( GraphDatabaseSettings.label_block_size );
                return me.initialize( new DynamicArrayStore(
                        fileName, me.config, IdType.NODE_LABELS, me.idGeneratorFactory, me.pageCache, me.logProvider,
                        blockSizeFromConfiguration ) );
            }
        },
        NODE
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.NODE_STORE_NAME );
                return me.initialize( new NodeStore( fileName, me.config, me.idGeneratorFactory, me.pageCache,
                        me.logProvider, me.getNodeLabelStore() ) );
            }
        },
        PROPERTY_KEY_TOKEN_NAME
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME );
                return me.initialize( new DynamicStringStore( fileName, me.config, IdType.PROPERTY_KEY_TOKEN_NAME,
                        me.idGeneratorFactory, me.pageCache, me.logProvider, TokenStore.NAME_STORE_BLOCK_SIZE ) );
            }
        },
        PROPERTY_KEY_TOKEN
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME );
                return me.initialize( new PropertyKeyTokenStore( fileName, me.config, me.idGeneratorFactory,
                        me.pageCache, me.logProvider, me.getPropertyKeyTokenNamesStore() ) );
            }
        },
        PROPERTY_STRING
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.PROPERTY_STRINGS_STORE_NAME );
                int blockSizeFromConfiguration = me.config.get( GraphDatabaseSettings.string_block_size );
                return me.initialize( new DynamicStringStore( fileName, me.config, IdType.STRING_BLOCK,
                        me.idGeneratorFactory, me.pageCache, me.logProvider, blockSizeFromConfiguration ) );
            }
        },
        PROPERTY_ARRAY
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.PROPERTY_ARRAYS_STORE_NAME );
                int blockSizeFromConfiguration = me.config.get( GraphDatabaseSettings.array_block_size );
                return me.initialize( new DynamicArrayStore( fileName, me.config, IdType.ARRAY_BLOCK,
                        me.idGeneratorFactory, me.pageCache, me.logProvider, blockSizeFromConfiguration ) );
            }
        },
        PROPERTY
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.PROPERTY_STORE_NAME );
                return me.initialize( new PropertyStore(
                        fileName, me.config, me.idGeneratorFactory, me.pageCache, me.logProvider,
                        me.getStringPropertyStore(), me.getPropertyKeyTokenStore(), me.getArrayPropertyStore() ) );
            }
        },
        RELATIONSHIP
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.RELATIONSHIP_STORE_NAME );
                return me.initialize( new RelationshipStore( fileName, me.config, me.idGeneratorFactory, me.pageCache,
                        me.logProvider ) );

            }
        },
        RELATIONSHIP_TYPE_TOKEN_NAME
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME );
                return me.initialize( new DynamicStringStore(
                        fileName, me.config, IdType.RELATIONSHIP_TYPE_TOKEN_NAME, me.idGeneratorFactory, me.pageCache,
                        me.logProvider, TokenStore.NAME_STORE_BLOCK_SIZE ) );

            }
        },
        RELATIONSHIP_TYPE_TOKEN
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME );
                return me.initialize( new RelationshipTypeTokenStore( fileName, me.config, me.idGeneratorFactory,
                        me.pageCache, me.logProvider, me.getRelationshipTypeTokenNamesStore() ) );
            }
        },
        LABEL_TOKEN_NAME
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.LABEL_TOKEN_NAMES_STORE_NAME );
                return me.initialize( new DynamicStringStore( fileName, me.config, IdType.LABEL_TOKEN_NAME,
                        me.idGeneratorFactory, me.pageCache, me.logProvider, TokenStore.NAME_STORE_BLOCK_SIZE ) );
            }
        },
        LABEL_TOKEN
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.LABEL_TOKEN_STORE_NAME );
                return me.initialize( new LabelTokenStore( fileName, me.config, me.idGeneratorFactory, me.pageCache,
                        me.logProvider, me.getLabelTokenNamesStore() ) );
            }
        },
        SCHEMA
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.SCHEMA_STORE_NAME );
                return me.initialize( new SchemaStore( fileName, me.config, IdType.SCHEMA, me.idGeneratorFactory,
                        me.pageCache, me.logProvider ) );
            }
        },
        RELATIONSHIP_GROUP
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.RELATIONSHIP_GROUP_STORE_NAME );
                return me.initialize( new RelationshipGroupStore( fileName, me.config, me.idGeneratorFactory,
                        me.pageCache, me.logProvider ) );
            }
        },
        COUNTS( false )
        {
            @Override
            public CountsTracker open( final NeoStores me )
            {
                File fileName = me.getStoreFileName( StoreFactory.COUNTS_STORE );
                boolean readOnly = me.config.get( GraphDatabaseSettings.read_only );
                CountsTracker counts = readOnly
                        ? me.createReadOnlyCountsTracker( fileName )
                        : me.createWritableCountsTracker( fileName );

                counts.setInitializer( new DataInitializer<CountsAccessor.Updater>()
                {
                    private final Log log = me.logProvider.getLog( MetaDataStore.class );

                    @Override
                    public void initialize( CountsAccessor.Updater updater )
                    {
                        log.warn( "Missing counts store, rebuilding it." );
                        new CountsComputer( me ).initialize( updater );
                    }

                    @Override
                    public long initialVersion()
                    {
                        return me.getMetaDataStore().getLastCommittedTransactionId();
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
                return counts;
            }

            @Override
            void close( NeoStores me, Object object )
            {
                CountsTracker counts = (CountsTracker) object;
                try
                {
                    counts.rotate( me.getMetaDataStore().getLastCommittedTransactionId() );
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
        },
        META_DATA // Make sure this META store is last
        {
            @Override
            public CommonAbstractStore open( NeoStores me )
            {
                return me.initialize( new MetaDataStore( me.neoStoreFileName, me.config, me.idGeneratorFactory,
                        me.pageCache, me.logProvider ) );
            }
        };

        private final boolean recordStore;

        private StoreType()
        {
            this( true );
        }

        private StoreType( boolean recordStore )
        {
            this.recordStore = recordStore;
        }

        abstract Object open( NeoStores me );

        void close( NeoStores me, Object object )
        {
            ((CommonAbstractStore)object).close();
        }
    }

    private static final StoreType[] STORE_TYPES = StoreType.values();

    private final Predicate<StoreType> INSTANTIATED_RECORD_STORES = new Predicate<StoreType>()
    {
        @Override
        public boolean test( StoreType type )
        {
            return type.recordStore && stores[type.ordinal()] != null;
        }
    };

    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final PageCache pageCache;
    private final LogProvider logProvider;
    private final boolean createIfNotExist;
    private final File storeDir;
    private final File neoStoreFileName;
    private final FileSystemAbstraction fileSystemAbstraction;
    // All stores, as Object due to CountsTracker being different that all other stores.
    private final Object[] stores;
    // The way a store is retrieved. As a function since if eagerly initialized then no synchronization
    // is required for getting/initializing
    private final Function<StoreType,Object> storeGetter;

    NeoStores(
            File neoStoreFileName,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            final LogProvider logProvider,
            FileSystemAbstraction fileSystemAbstraction,
            boolean createIfNotExist,
            boolean eagerlyInitializedStores )
    {
        this.neoStoreFileName = neoStoreFileName;
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.pageCache = pageCache;
        this.logProvider = logProvider;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.createIfNotExist = createIfNotExist;
        this.storeDir = neoStoreFileName.getParentFile();

        final Object[] stores = new Object[STORE_TYPES.length];
        if ( eagerlyInitializedStores )
        {
            // Ensure they're all instantiated and initialized
            // So that we can just return from the array without fuzz
            this.storeGetter = new Function<StoreType,Object>()
            {
                @Override
                public Object apply( StoreType type )
                {
                    return stores[type.ordinal()];
                }
            };
            for ( StoreType type : STORE_TYPES )
            {
                getInitializedStore( type, stores );
            }
        }
        else
        {
            // Do synchronization on every call since the stores are opened lazily.
            this.storeGetter = new Function<StoreType,Object>()
            {
                @Override
                public Object apply( StoreType type )
                {
                    return getInitializedStore( type, stores );
                }
            };
        }

        this.stores = stores;
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
    @Override
    public void close()
    {
        for ( StoreType type : STORE_TYPES )
        {
            closeStore( type );
        }
    }

    private void closeStore( StoreType type )
    {
        int i = type.ordinal();
        if ( stores[i] != null )
        {
            type.close( this, stores[i] );
            stores[i] = null;
        }
    }

    public void flush()
    {
        try
        {
            CountsTracker counts = (CountsTracker) stores[StoreType.COUNTS.ordinal()];
            if ( counts != null )
            {
                counts.rotate( getMetaDataStore().getLastCommittedTransactionId() );
            }
            pageCache.flushAndForce();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Failed to flush", e );
        }
    }

    private synchronized Object getInitializedStore( StoreType type, Object[] stores )
    {
        int i = type.ordinal();
        if ( stores[i] == null )
        {
            stores[i] = type.open( this );
        }
        return stores[i];
    }

    <T extends CommonAbstractStore> T initialize( T store )
    {
        store.initialise( createIfNotExist );
        return store;
    }

    /**
     * @return the NeoStore.
     */
    public MetaDataStore getMetaDataStore()
    {
        return (MetaDataStore) storeGetter.apply( StoreType.META_DATA );
    }

    /**
     * @return The node store
     */
    public NodeStore getNodeStore()
    {
        return (NodeStore) storeGetter.apply( StoreType.NODE );
    }

    private DynamicArrayStore getNodeLabelStore()
    {
        return (DynamicArrayStore) storeGetter.apply( StoreType.NODE_LABEL );
    }

    /**
     * The relationship store.
     *
     * @return The relationship store
     */
    public RelationshipStore getRelationshipStore()
    {
        return (RelationshipStore) storeGetter.apply( StoreType.RELATIONSHIP );
    }

    /**
     * Returns the relationship type store.
     *
     * @return The relationship type store
     */
    public RelationshipTypeTokenStore getRelationshipTypeTokenStore()
    {
        return (RelationshipTypeTokenStore) storeGetter.apply( StoreType.RELATIONSHIP_TYPE_TOKEN );
    }

    private DynamicStringStore getRelationshipTypeTokenNamesStore()
    {
        return (DynamicStringStore) storeGetter.apply( StoreType.RELATIONSHIP_TYPE_TOKEN_NAME );
    }

    /**
     * Returns the label store.
     *
     * @return The label store
     */
    public LabelTokenStore getLabelTokenStore()
    {
        return (LabelTokenStore) storeGetter.apply( StoreType.LABEL_TOKEN );
    }

    private DynamicStringStore getLabelTokenNamesStore()
    {
        return (DynamicStringStore) storeGetter.apply( StoreType.LABEL_TOKEN_NAME );
    }

    /**
     * Returns the property store.
     *
     * @return The property store
     */
    public PropertyStore getPropertyStore()
    {
        return (PropertyStore) storeGetter.apply( StoreType.PROPERTY );
    }

    private DynamicStringStore getStringPropertyStore()
    {
        return (DynamicStringStore) storeGetter.apply( StoreType.PROPERTY_STRING );
    }

    private DynamicArrayStore getArrayPropertyStore()
    {
        return (DynamicArrayStore) storeGetter.apply( StoreType.PROPERTY_ARRAY );
    }

    /**
     * @return the {@link PropertyKeyTokenStore}
     */
    public PropertyKeyTokenStore getPropertyKeyTokenStore()
    {
        return (PropertyKeyTokenStore) storeGetter.apply( StoreType.PROPERTY_KEY_TOKEN );
    }

    private DynamicStringStore getPropertyKeyTokenNamesStore()
    {
        return (DynamicStringStore) storeGetter.apply( StoreType.PROPERTY_KEY_TOKEN_NAME );
    }

    /**
     * @return the {@link RelationshipGroupStore}
     */
    public RelationshipGroupStore getRelationshipGroupStore()
    {
        return (RelationshipGroupStore) storeGetter.apply( StoreType.RELATIONSHIP_GROUP );
    }

    /**
     * @return the schema store.
     */
    public SchemaStore getSchemaStore()
    {
        return (SchemaStore) storeGetter.apply( StoreType.SCHEMA );
    }

    public CountsTracker getCounts()
    {
        return (CountsTracker) storeGetter.apply( StoreType.COUNTS );
    }

    private CountsTracker createWritableCountsTracker( File fileName )
    {
        return new CountsTracker( logProvider, fileSystemAbstraction, pageCache, config, fileName );
    }

    private ReadOnlyCountsTracker createReadOnlyCountsTracker( File fileName )
    {
        return new ReadOnlyCountsTracker( logProvider, fileSystemAbstraction, pageCache, config, fileName );
    }

    private Iterable<CommonAbstractStore> instantiatedRecordStores()
    {
        Iterator<StoreType> storeTypes = new FilteringIterator<>( iterator( STORE_TYPES ), INSTANTIATED_RECORD_STORES );
        return loop( new IteratorWrapper<CommonAbstractStore,StoreType>( storeTypes )
        {
            @Override
            protected CommonAbstractStore underlyingObjectToObject( StoreType type )
            {
                return (CommonAbstractStore) stores[type.ordinal()];
            }
        } );
    }

    public void makeStoreOk()
    {
        for ( CommonAbstractStore store : instantiatedRecordStores() )
        {
            store.makeStoreOk();
        }
    }

    public void rebuildIdGenerators()
    {
        for ( CommonAbstractStore store : instantiatedRecordStores() )
        {
            store.rebuildIdGenerator();
        }
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
        for ( CommonAbstractStore store : instantiatedRecordStores() )
        {
            store.logVersions( msgLog );
        }
    }

    public void logIdUsage( Logger msgLog )
    {
        msgLog.log( "Id usage:" );
        for ( CommonAbstractStore store : instantiatedRecordStores() )
        {
            store.logIdUsage( msgLog );
        }
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
        for ( CommonAbstractStore store : instantiatedRecordStores() )
        {
            store.visitStore( visitor );
        }
    }

    public void rebuildCountStoreIfNeeded() throws IOException
    {
        // TODO: move this to LifeCycle
        getCounts().start();
    }

    public void assertOpen()
    {
        if ( stores[StoreType.NODE.ordinal()] == null )
        {
            throw new IllegalStateException( "Database has been shutdown" );
        }
    }
}
