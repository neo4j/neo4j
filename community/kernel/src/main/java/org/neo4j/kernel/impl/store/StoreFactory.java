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
import java.nio.ByteBuffer;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

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
    @SuppressWarnings( "deprecation" )
    private final IdGeneratorFactory idGeneratorFactory;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final StringLogger stringLogger;
    private final StoreVersionMismatchHandler versionMismatchHandler;
    private final File neoStoreFileName;
    private final Monitors monitors;
    private final PageCache pageCache;

    public StoreFactory( FileSystemAbstraction fileSystem, File storeDir, PageCache pageCache, StringLogger logger,
            Monitors monitors )
    {
        this( fileSystem, storeDir, pageCache, logger, monitors, StoreVersionMismatchHandler.FORCE_CURRENT_VERSION );
    }

    @SuppressWarnings( "deprecation" )
    public StoreFactory( FileSystemAbstraction fileSystem, File storeDir, PageCache pageCache, StringLogger logger,
                         Monitors monitors, StoreVersionMismatchHandler versionMismatchHandler )
    {
        this( configForStoreDir( new Config(), storeDir ),
                new DefaultIdGeneratorFactory(), pageCache, fileSystem,
                logger, monitors, versionMismatchHandler );
    }

    public StoreFactory( Config config, @SuppressWarnings( "deprecation" ) IdGeneratorFactory idGeneratorFactory,
                         PageCache pageCache, FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger,
                         Monitors monitors )
    {
        this( config, idGeneratorFactory, pageCache, fileSystemAbstraction, stringLogger,
                monitors, StoreVersionMismatchHandler.FORCE_CURRENT_VERSION );
    }

    public StoreFactory( Config config, @SuppressWarnings( "deprecation" ) IdGeneratorFactory idGeneratorFactory,
                         PageCache pageCache, FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger,
                         Monitors monitors, StoreVersionMismatchHandler versionMismatchHandler )
    {
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.stringLogger = stringLogger;
        this.versionMismatchHandler = versionMismatchHandler;
        this.neoStoreFileName = config.get( GraphDatabaseSettings.neo_store );
        assert neoStoreFileName != null;
        this.monitors = monitors;
        this.pageCache = pageCache;
    }

    public static String buildTypeDescriptorAndVersion( String typeDescriptor )
    {
        return typeDescriptor + " " + CommonAbstractStore.ALL_STORES_VERSION;
    }

    /**
     * Fills in neo_store and store_dir based on store dir.
     *
     * @return a new modified config, leaves this config unchanged.
     */
    public static Config configForStoreDir( Config config, File storeDir )
    {
        return config.with( stringMap(
                GraphDatabaseSettings.neo_store.name(), new File( storeDir, NeoStore.DEFAULT_NAME ).getAbsolutePath(),
                GraphDatabaseSettings.store_dir.name(), storeDir.getAbsolutePath() ) );
    }

    public File storeFileName( String toAppend )
    {
        return new File( neoStoreFileName.getPath() + toAppend );
    }

    public File storeFileName( StoreFile file, StoreFileType type )
    {
        return new File( neoStoreFileName.getParentFile(), file.fileName( type ) );
    }

    public NeoStore newNeoStore( boolean allowCreateEmpty )
    {
        boolean storeExists = storeExists();

        if ( !storeExists && allowCreateEmpty )
        {
            return createNeoStore();
        }

        // The store exists already, start it
        return new NeoStore( neoStoreFileName, config, idGeneratorFactory, pageCache, fileSystemAbstraction,
                stringLogger,
                newRelationshipTypeTokenStore(),
                newLabelTokenStore(),
                newPropertyStore(),
                newRelationshipStore(),
                newNodeStore(),
                newSchemaStore(),
                newRelationshipGroupStore(),
                newCountsStore(),
                versionMismatchHandler, monitors );
    }

    public boolean storeExists()
    {
        return fileSystemAbstraction.fileExists( neoStoreFileName );
    }

    public RelationshipGroupStore newRelationshipGroupStore()
    {
        return newRelationshipGroupStore( storeFileName( RELATIONSHIP_GROUP_STORE_NAME ) );
    }

    public RelationshipGroupStore newRelationshipGroupStore( File baseFile )
    {
        return new RelationshipGroupStore( baseFile, config,
                idGeneratorFactory, pageCache, fileSystemAbstraction, stringLogger, versionMismatchHandler, monitors );
    }

    public SchemaStore newSchemaStore()
    {
        return newSchemaStore( storeFileName( SCHEMA_STORE_NAME ) );
    }

    @SuppressWarnings( "deprecation" )
    public SchemaStore newSchemaStore( File baseFile )
    {
        return new SchemaStore( baseFile, config, IdType.SCHEMA,
                idGeneratorFactory, pageCache, fileSystemAbstraction, stringLogger, versionMismatchHandler, monitors );
    }

    public DynamicStringStore newDynamicStringStore( File fileName,
                                                     @SuppressWarnings( "deprecation" ) IdType nameIdType )
    {
        return new DynamicStringStore( fileName, config, nameIdType, idGeneratorFactory, pageCache,
                fileSystemAbstraction, stringLogger, versionMismatchHandler, monitors );
    }

    public RelationshipTypeTokenStore newRelationshipTypeTokenStore()
    {
        return newRelationshipTypeTokenStore( storeFileName( RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME ),
                storeFileName( RELATIONSHIP_TYPE_TOKEN_STORE_NAME ) );
    }

    public RelationshipTypeTokenStore newRelationshipTypeTokenStore( File baseFile )
    {
        return newRelationshipTypeTokenStore( new File( baseFile + NAMES_PART ), baseFile );
    }

    @SuppressWarnings( "deprecation" )
    private RelationshipTypeTokenStore newRelationshipTypeTokenStore( File relationshipTypeTokenNamesStore,
                                                                      File relationshipTypeTokenStore )
    {
        DynamicStringStore nameStore = newDynamicStringStore( relationshipTypeTokenNamesStore,
                IdType.RELATIONSHIP_TYPE_TOKEN_NAME );
        return new RelationshipTypeTokenStore( relationshipTypeTokenStore, config,
                idGeneratorFactory, pageCache, fileSystemAbstraction, stringLogger, nameStore,
                versionMismatchHandler, monitors );
    }

    public PropertyStore newPropertyStore()
    {
        return newPropertyStore( storeFileName( PROPERTY_STRINGS_STORE_NAME ),
                storeFileName( PROPERTY_ARRAYS_STORE_NAME ), storeFileName( PROPERTY_STORE_NAME ),
                storeFileName( PROPERTY_KEY_TOKEN_STORE_NAME ) );
    }

    public PropertyStore newPropertyStore( File baseFile )
    {
        return newPropertyStore( new File( baseFile.getPath() + STRINGS_PART ),
                new File( baseFile.getPath() + ARRAYS_PART ), baseFile,
                new File( baseFile.getPath() + INDEX_PART ) );
    }

    @SuppressWarnings( "deprecation" )
    private PropertyStore newPropertyStore( File propertyStringStore, File propertyArrayStore, File propertyStore,
                                            File propertyKeysStore )
    {
        PropertyKeyTokenStore propertyKeyTokenStore = newPropertyKeyTokenStore( propertyKeysStore );
        DynamicStringStore stringPropertyStore = newDynamicStringStore( propertyStringStore, IdType.STRING_BLOCK );
        DynamicArrayStore arrayPropertyStore = newDynamicArrayStore( propertyArrayStore, IdType.ARRAY_BLOCK );
        return new PropertyStore( propertyStore, config, idGeneratorFactory,
                pageCache, fileSystemAbstraction, stringLogger, stringPropertyStore, propertyKeyTokenStore,
                arrayPropertyStore, versionMismatchHandler, monitors );
    }

    public PropertyKeyTokenStore newPropertyKeyTokenStore()
    {
        return newPropertyKeyTokenStore( storeFileName( PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),
                storeFileName( PROPERTY_KEY_TOKEN_STORE_NAME ) );
    }

    public PropertyKeyTokenStore newPropertyKeyTokenStore( File baseFile )
    {
        return newPropertyKeyTokenStore( new File( baseFile.getPath() + KEYS_PART ), baseFile );
    }

    @SuppressWarnings( "deprecation" )
    private PropertyKeyTokenStore newPropertyKeyTokenStore( File propertyKeyTokenNamesStore,
                                                            File propertyKeyTokenStore )
    {
        DynamicStringStore nameStore = newDynamicStringStore( propertyKeyTokenNamesStore,
                IdType.PROPERTY_KEY_TOKEN_NAME );
        return new PropertyKeyTokenStore( propertyKeyTokenStore, config,
                idGeneratorFactory, pageCache, fileSystemAbstraction, stringLogger, nameStore,
                versionMismatchHandler, monitors );
    }

    public LabelTokenStore newLabelTokenStore()
    {
        return newLabelTokenStore( storeFileName( LABEL_TOKEN_NAMES_STORE_NAME ),
                storeFileName( LABEL_TOKEN_STORE_NAME ) );
    }

    public LabelTokenStore newLabelTokenStore( File baseFile )
    {
        return newLabelTokenStore( new File( baseFile + NAMES_PART ), baseFile );
    }

    private LabelTokenStore newLabelTokenStore( File labelTokenNamesStore, File labelTokenStore )
    {
        @SuppressWarnings( "deprecation" )
        DynamicStringStore nameStore = newDynamicStringStore( labelTokenNamesStore, IdType.LABEL_TOKEN_NAME );
        return new LabelTokenStore( labelTokenStore, config, idGeneratorFactory,
                pageCache, fileSystemAbstraction, stringLogger, nameStore, versionMismatchHandler, monitors );
    }

    public RelationshipStore newRelationshipStore()
    {
        return newRelationshipStore( storeFileName( RELATIONSHIP_STORE_NAME ) );
    }

    public RelationshipStore newRelationshipStore( File baseFile )
    {
        return new RelationshipStore( baseFile, config,
                idGeneratorFactory, pageCache, fileSystemAbstraction, stringLogger, versionMismatchHandler, monitors );
    }

    public DynamicArrayStore newDynamicArrayStore( File fileName, @SuppressWarnings( "deprecation" ) IdType idType )
    {
        return new DynamicArrayStore( fileName, config, idType, idGeneratorFactory, pageCache,
                fileSystemAbstraction, stringLogger, versionMismatchHandler, monitors );
    }

    public NodeStore newNodeStore()
    {
        return newNodeStore( storeFileName( NODE_LABELS_STORE_NAME ), storeFileName( NODE_STORE_NAME ) );
    }

    public NodeStore newNodeStore( File baseFile )
    {
        return newNodeStore( new File( baseFile.getPath() + LABELS_PART ), baseFile );
    }

    @SuppressWarnings( "deprecation" )
    private NodeStore newNodeStore( File labelStore, File nodeStore )
    {
        DynamicArrayStore dynamicLabelStore = new DynamicArrayStore( labelStore,
                config, IdType.NODE_LABELS, idGeneratorFactory, pageCache, fileSystemAbstraction, stringLogger,
                versionMismatchHandler, monitors );
        return new NodeStore( nodeStore, config, idGeneratorFactory, pageCache,
                fileSystemAbstraction, stringLogger, dynamicLabelStore, versionMismatchHandler, monitors );
    }

    public CountsTracker newCountsStore()
    {
        return new CountsTracker( stringLogger, fileSystemAbstraction, pageCache, storeFileName( COUNTS_STORE ) );
    }

    public NeoStore createNeoStore()
    {
        return createNeoStore( new StoreId() );
    }

    public NeoStore createNeoStore( StoreId storeId )
    {
        // Go ahead and create the store
        stringLogger.info( "Creating new db @ " + neoStoreFileName );
        try
        {
            fileSystemAbstraction.mkdirs( neoStoreFileName.getParentFile() );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to create directory " +
                                                  neoStoreFileName.getParentFile() + " for creating a neo store in",
                    e );
        }

        createEmptyStore( neoStoreFileName, buildTypeDescriptorAndVersion( NeoStore.TYPE_DESCRIPTOR ) );
        createNodeStore();
        createRelationshipStore();
        createPropertyStore();
        createRelationshipTypeStore();
        createLabelTokenStore();
        createSchemaStore();
        createRelationshipGroupStore( config.get( Configuration.dense_node_threshold ) );

        NeoStore neoStore = newNeoStore( false );
        /*
         * created time | random long | backup version | tx id | store version | next prop | latest constraint tx |
         * upgrade time | upgrade id
         */
        for ( int i = 0; i < NeoStore.META_DATA_RECORD_COUNT; i++ )
        {
            neoStore.nextId();
        }
        neoStore.setCreationTime( storeId.getCreationTime() );
        neoStore.setRandomNumber( storeId.getRandomId() );
        // If neoStore.creationTime == neoStore.upgradeTime && neoStore.upgradeTransactionId == BASE_TX_ID
        // then store has never been upgraded
        neoStore.setUpgradeTime( storeId.getCreationTime() );
        neoStore.setUpgradeTransaction( BASE_TX_ID, BASE_TX_CHECKSUM );
        neoStore.setCurrentLogVersion( 0 );
        neoStore.setLastCommittedAndClosedTransactionId( BASE_TX_ID, BASE_TX_CHECKSUM );
        neoStore.setStoreVersion( NeoStore.versionStringToLong( CommonAbstractStore.ALL_STORES_VERSION ) );
        neoStore.setGraphNextProp( -1 );
        neoStore.setLatestConstraintIntroducingTx( 0 );

        neoStore.flush();

        return neoStore;
    }

    /**
     * Creates a new node store contained in <CODE>fileName</CODE> If filename
     * is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     */
    public void createNodeStore()
    {
        createNodeLabelsStore();
        createEmptyStore( storeFileName( NODE_STORE_NAME ), buildTypeDescriptorAndVersion( NodeStore.TYPE_DESCRIPTOR
        ) );
    }

    @SuppressWarnings( "deprecation" )
    private void createNodeLabelsStore()
    {
        int labelStoreBlockSize = config.get( Configuration.label_block_size );
        createEmptyDynamicStore( storeFileName( NODE_LABELS_STORE_NAME ), labelStoreBlockSize,
                DynamicArrayStore.VERSION, IdType.NODE_LABELS );
    }

    /**
     * Creates a new relationship store contained in <CODE>fileName</CODE> If
     * filename is <CODE>null</CODE> or the file already exists an <CODE>IOException</CODE>
     * is thrown.
     */
    public void createRelationshipStore()
    {
        createEmptyStore( storeFileName( RELATIONSHIP_STORE_NAME ),
                buildTypeDescriptorAndVersion( RelationshipStore.TYPE_DESCRIPTOR ) );
    }

    /**
     * Creates a new property store contained in <CODE>fileName</CODE> If
     * filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     */
    @SuppressWarnings( "deprecation" )
    public void createPropertyStore()
    {
        createEmptyStore( storeFileName( PROPERTY_STORE_NAME ),
                buildTypeDescriptorAndVersion( PropertyStore.TYPE_DESCRIPTOR ) );
        int stringStoreBlockSize = config.get( Configuration.string_block_size );
        int arrayStoreBlockSize = config.get( Configuration.array_block_size );

        createPropertyKeyTokenStore();
        createDynamicStringStore( storeFileName( PROPERTY_STRINGS_STORE_NAME ), stringStoreBlockSize,
                IdType.STRING_BLOCK );
        createDynamicArrayStore( storeFileName( PROPERTY_ARRAYS_STORE_NAME ), arrayStoreBlockSize );
    }

    /**
     * Creates a new relationship type store contained in <CODE>fileName</CODE>
     * If filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     */
    @SuppressWarnings( "deprecation" )
    private void createRelationshipTypeStore()
    {
        createEmptyStore( storeFileName( RELATIONSHIP_TYPE_TOKEN_STORE_NAME ),
                buildTypeDescriptorAndVersion( RelationshipTypeTokenStore.TYPE_DESCRIPTOR ) );
        createDynamicStringStore( storeFileName( RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME ),
                TokenStore.NAME_STORE_BLOCK_SIZE, IdType.RELATIONSHIP_TYPE_TOKEN_NAME );
        RelationshipTypeTokenStore store = newRelationshipTypeTokenStore();
        store.close();
    }

    @SuppressWarnings( "deprecation" )
    private void createLabelTokenStore()
    {
        createEmptyStore( storeFileName( LABEL_TOKEN_STORE_NAME ),
                buildTypeDescriptorAndVersion( LabelTokenStore.TYPE_DESCRIPTOR ) );
        createDynamicStringStore( storeFileName( LABEL_TOKEN_NAMES_STORE_NAME ),
                TokenStore.NAME_STORE_BLOCK_SIZE, IdType.LABEL_TOKEN_NAME );
        LabelTokenStore store = newLabelTokenStore();
        store.close();
    }

    public void createDynamicStringStore( File fileName, int blockSize,
                                          @SuppressWarnings( "deprecation" ) IdType idType )
    {
        createEmptyDynamicStore( fileName, blockSize, DynamicStringStore.VERSION, idType );
    }

    @SuppressWarnings( "deprecation" )
    public void createPropertyKeyTokenStore()
    {
        createEmptyStore( storeFileName( PROPERTY_KEY_TOKEN_STORE_NAME ),
                buildTypeDescriptorAndVersion( PropertyKeyTokenStore.TYPE_DESCRIPTOR ) );
        createDynamicStringStore( storeFileName( PROPERTY_KEY_TOKEN_NAMES_STORE_NAME ),
                TokenStore.NAME_STORE_BLOCK_SIZE, IdType.PROPERTY_KEY_TOKEN_NAME );
    }

    @SuppressWarnings( "deprecation" )
    public void createDynamicArrayStore( File fileName, int blockSize )
    {
        createEmptyDynamicStore( fileName, blockSize, DynamicArrayStore.VERSION, IdType.ARRAY_BLOCK );
    }

    @SuppressWarnings( "deprecation" )
    public void createSchemaStore()
    {
        createEmptyDynamicStore( storeFileName( SCHEMA_STORE_NAME ), SchemaStore.BLOCK_SIZE,
                SchemaStore.VERSION, IdType.SCHEMA );
    }

    /**
     * Creates a new empty store. A factory method returning an implementation
     * should make use of this method to initialize an empty store. Block size
     * must be greater than zero. Not that the first block will be marked as
     * reserved (contains info about the block size). There will be an overhead
     * for each block of <CODE>AbstractDynamicStore.BLOCK_HEADER_SIZE</CODE>
     * bytes.
     * <p>
     * This method will create a empty store with descriptor returned by the
     * {@link CommonAbstractStore#getTypeDescriptor()}. The internal id generator used by
     * this store will also be created.
     *
     * @param fileName The file name of the store that will be created
     * @param baseBlockSize The number of bytes for each block
     * @param typeAndVersionDescriptor The type and version descriptor that identifies this store
     */
    public void createEmptyDynamicStore( File fileName, int baseBlockSize,
                                         String typeAndVersionDescriptor,
                                         @SuppressWarnings( "deprecation" ) IdType idType )
    {
        int blockSize = baseBlockSize;
        // sanity checks
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        if ( fileSystemAbstraction.fileExists( fileName ) )
        {
            throw new IllegalStateException( "Can't create store[" + fileName
                                             + "], file already exists" );
        }
        if ( blockSize < 1 )
        {
            throw new IllegalArgumentException( "Illegal block size["
                                                + blockSize + "]" );
        }
        if ( blockSize > 0xFFFF )
        {
            throw new IllegalArgumentException( "Illegal block size[" + blockSize + "], limit is 65535" );
        }
        blockSize += AbstractDynamicStore.BLOCK_HEADER_SIZE;

        // write the header
        try
        {
            StoreChannel channel = fileSystemAbstraction.create( fileName );
            int endHeaderSize = blockSize
                                + UTF8.encode( typeAndVersionDescriptor ).length;
            ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
            buffer.putInt( blockSize );
            buffer.position( endHeaderSize - typeAndVersionDescriptor.length() );
            buffer.put( UTF8.encode( typeAndVersionDescriptor ) ).flip();
            channel.write( buffer );
            channel.force( false );
            channel.close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to create store "
                                                  + fileName, e );
        }
        idGeneratorFactory.create( fileSystemAbstraction, new File( fileName.getPath() + ".id" ), 0 );
        // TODO highestIdInUse = 0 works now, but not when slave can create store files.
        IdGenerator idGenerator = idGeneratorFactory.open( fileSystemAbstraction,
                new File( fileName.getPath() + ".id" ),
                idType.getGrabSize(), idType, 0 );
        idGenerator.nextId(); // reserve first for blockSize
        idGenerator.close();
    }

    @SuppressWarnings( "deprecation" )
    public void createRelationshipGroupStore( int denseNodeThreshold )
    {
        ByteBuffer firstRecord = ByteBuffer.allocate( RelationshipGroupStore.RECORD_SIZE ).putInt( denseNodeThreshold );
        firstRecord.flip();
        firstRecord.limit( firstRecord.capacity() );
        createEmptyStore( storeFileName( RELATIONSHIP_GROUP_STORE_NAME ),
                buildTypeDescriptorAndVersion( RelationshipGroupStore.TYPE_DESCRIPTOR ),
                firstRecord, IdType.RELATIONSHIP_GROUP );
    }

    public void createEmptyStore( File fileName, String typeAndVersionDescriptor )
    {
        createEmptyStore( fileName, typeAndVersionDescriptor, null, null );
    }

    private void createEmptyStore( File fileName, String typeAndVersionDescriptor, ByteBuffer firstRecordData,
                                   @SuppressWarnings( "deprecation" ) IdType idType )
    {
        // sanity checks
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        if ( fileSystemAbstraction.fileExists( fileName ) )
        {
            throw new IllegalStateException( "Can't create store[" + fileName
                                             + "], file already exists" );
        }

        // write the header
        try
        {
            StoreChannel channel = fileSystemAbstraction.create( fileName );
            int endHeaderSize = UTF8.encode( typeAndVersionDescriptor ).length;
            if ( firstRecordData != null )
            {
                endHeaderSize += firstRecordData.limit();
            }
            ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
            if ( firstRecordData != null )
            {
                buffer.put( firstRecordData );
            }
            buffer.put( UTF8.encode( typeAndVersionDescriptor ) ).flip();
            channel.write( buffer );
            channel.force( false );
            channel.close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to create store " + fileName, e );
        }
        idGeneratorFactory.create( fileSystemAbstraction, new File( fileName.getPath() + ".id" ), 0 );
        if ( firstRecordData != null )
        {
            IdGenerator idGenerator = idGeneratorFactory.open( fileSystemAbstraction,
                    new File( fileName.getPath() + ".id" ), 1, idType, 0 );
            idGenerator.nextId(); // reserve first for blockSize
            idGenerator.close();
        }
    }

    /**
     * I.e. total number of used/unused records + 1
     * @param storeFile {@link StoreFile} to get the name from.
     * @param recordSize record size of that store.
     * @return highId, i.e. an id one greater than the highest id in the store.
     */
    public long getHighId( StoreFile storeFile, int recordSize ) throws IOException
    {
        return IdGeneratorImpl.readHighId( fileSystemAbstraction, storeFileName( storeFile, StoreFileType.ID ) );
    }

    public abstract static class Configuration
    {
        public static final Setting<Integer> string_block_size = GraphDatabaseSettings.string_block_size;
        public static final Setting<Integer> array_block_size = GraphDatabaseSettings.array_block_size;
        public static final Setting<Integer> label_block_size = GraphDatabaseSettings.label_block_size;
        public static final Setting<Integer> dense_node_threshold = GraphDatabaseSettings.dense_node_threshold;
    }
}
