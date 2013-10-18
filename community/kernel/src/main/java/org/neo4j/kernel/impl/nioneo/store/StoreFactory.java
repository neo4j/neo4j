/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.storemigration.ConfigMapUpgradeConfiguration;
import org.neo4j.kernel.impl.storemigration.DatabaseFiles;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.util.StringLogger;

/**
* Factory for Store implementations. Can also be used to create empty stores.
*/
public class StoreFactory
{
    public static abstract class Configuration
    {
        public static final Setting<Integer> string_block_size = GraphDatabaseSettings.string_block_size;
        public static final Setting<Integer> array_block_size = GraphDatabaseSettings.array_block_size;
        public static final Setting<Integer> label_block_size = GraphDatabaseSettings.label_block_size;
    }

    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final WindowPoolFactory windowPoolFactory;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final StringLogger stringLogger;
    private final TxHook txHook;

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
    public static final String RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME = RELATIONSHIP_TYPE_TOKEN_STORE_NAME + NAMES_PART;
    public static final String LABEL_TOKEN_STORE_NAME = ".labeltokenstore.db";
    public static final String LABEL_TOKEN_NAMES_STORE_NAME = LABEL_TOKEN_STORE_NAME + NAMES_PART;
    public static final String SCHEMA_STORE_NAME = ".schemastore.db";

    public StoreFactory( Config config, IdGeneratorFactory idGeneratorFactory, WindowPoolFactory windowPoolFactory,
                         FileSystemAbstraction fileSystemAbstraction, StringLogger stringLogger, TxHook txHook )
    {
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
        this.windowPoolFactory = windowPoolFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.stringLogger = stringLogger;
        this.txHook = txHook;
    }

    public boolean ensureStoreExists() throws IOException
    {
        boolean readOnly = config.get( GraphDatabaseSettings.read_only );

        File store = config.get( GraphDatabaseSettings.neo_store );
        boolean created = false;
        if ( !readOnly && !fileSystemAbstraction.fileExists( store ))
        {
            stringLogger.info( "Creating new db @ " + store );
            fileSystemAbstraction.mkdirs( store.getParentFile() );
            createNeoStore( store ).close();
            created = true;
        }
        return created;
    }

    public NeoStore newNeoStore(File fileName)
    {
        try
        {
            return attemptNewNeoStore( fileName );
        }
        catch ( NotCurrentStoreVersionException e )
        {
            tryToUpgradeStores( fileName );
            return attemptNewNeoStore( fileName );
        }
        catch ( StoreNotFoundException e )
        {
            tryToUpgradeStores( fileName );
            return attemptNewNeoStore( fileName );
        }
    }

    NeoStore attemptNewNeoStore( File fileName )
    {
        return new NeoStore( fileName, config, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction,
                stringLogger, txHook,
                newRelationshipTypeTokenStore( new File( fileName.getPath() + RELATIONSHIP_TYPE_TOKEN_STORE_NAME ) ),
                newLabelTokenStore( new File( fileName.getPath() + LABEL_TOKEN_STORE_NAME ) ),
                newPropertyStore(new File( fileName.getPath() + PROPERTY_STORE_NAME)),
                newRelationshipStore(new File( fileName.getPath() + RELATIONSHIP_STORE_NAME)),
                newNodeStore(new File( fileName.getPath() + NODE_STORE_NAME)),
                // We don't need any particular upgrade when we add the schema store
                newSchemaStore(new File( fileName.getPath() + SCHEMA_STORE_NAME)));
    }

    private void tryToUpgradeStores( File fileName )
    {
        new StoreUpgrader(config, new ConfigMapUpgradeConfiguration(config),
                new UpgradableDatabase( fileSystemAbstraction ),
                new StoreMigrator( new VisibleMigrationProgressMonitor( stringLogger, System.out ) ),
                new DatabaseFiles( fileSystemAbstraction ),
                idGeneratorFactory, fileSystemAbstraction ).attemptUpgrade( fileName );
    }

    public SchemaStore newSchemaStore( File file )
    {
        return new SchemaStore( file, config, IdType.SCHEMA, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger );
    }

    private DynamicStringStore newDynamicStringStore(File fileName, IdType nameIdType)
    {
        return new DynamicStringStore( fileName, config, nameIdType, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger);
    }

    private RelationshipTypeTokenStore newRelationshipTypeTokenStore( File baseFileName )
    {
        DynamicStringStore nameStore = newDynamicStringStore( new File( baseFileName.getPath() + NAMES_PART), IdType.RELATIONSHIP_TYPE_TOKEN_NAME );
        return new RelationshipTypeTokenStore( baseFileName, config, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger, nameStore );
    }

    public PropertyStore newPropertyStore( File baseFileName )
    {
        DynamicStringStore stringPropertyStore = newDynamicStringStore(
                new File( baseFileName.getPath() + STRINGS_PART ), IdType.STRING_BLOCK );
        PropertyKeyTokenStore propertyKeyTokenStore = newPropertyKeyTokenStore(
                new File( baseFileName.getPath() + INDEX_PART ) );
        DynamicArrayStore arrayPropertyStore = newDynamicArrayStore( new File( baseFileName.getPath() + ARRAYS_PART ) );
        return new PropertyStore( baseFileName, config, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction,
                stringLogger, stringPropertyStore, propertyKeyTokenStore, arrayPropertyStore );
    }

    public PropertyKeyTokenStore newPropertyKeyTokenStore( File baseFileName )
    {
        DynamicStringStore nameStore = newDynamicStringStore( new File( baseFileName.getPath() + KEYS_PART ),
                IdType.PROPERTY_KEY_TOKEN_NAME );
        return new PropertyKeyTokenStore( baseFileName, config, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger, nameStore );
    }

    private LabelTokenStore newLabelTokenStore( File baseFileName )
    {
        DynamicStringStore nameStore = newDynamicStringStore(new File( baseFileName.getPath() + NAMES_PART ),
                IdType.LABEL_TOKEN_NAME );
        return new LabelTokenStore( baseFileName, config, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger, nameStore );
    }

    private RelationshipStore newRelationshipStore(File baseFileName)
    {
        return new RelationshipStore( baseFileName, config, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger);
    }

    public DynamicArrayStore newDynamicArrayStore(File baseFileName)
    {
        return new DynamicArrayStore( baseFileName, config, IdType.ARRAY_BLOCK, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger);
    }

    public NodeStore newNodeStore(File baseFileName)
    {
        File labelsFileName = new File( baseFileName.getPath() + LABELS_PART );
        DynamicArrayStore dynamicLabelStore = new DynamicArrayStore( labelsFileName,
                config, IdType.NODE_LABELS, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction, stringLogger);
        return new NodeStore( baseFileName, config, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction,
                stringLogger, dynamicLabelStore );
    }

    public NeoStore createNeoStore(File fileName)
    {
        return createNeoStore( fileName, new StoreId() );
    }

    public NeoStore createNeoStore(File fileName, StoreId storeId)
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( NeoStore.TYPE_DESCRIPTOR ) );
        createNodeStore(new File( fileName.getPath() + NODE_STORE_NAME));
        createRelationshipStore(new File( fileName.getPath() + RELATIONSHIP_STORE_NAME));
        createPropertyStore(new File( fileName.getPath() + PROPERTY_STORE_NAME));
        createRelationshipTypeStore(new File( fileName.getPath() + RELATIONSHIP_TYPE_TOKEN_STORE_NAME ));
        createLabelTokenStore( new File( fileName.getPath() + LABEL_TOKEN_STORE_NAME ) );
        createSchemaStore(new File( fileName.getPath() + SCHEMA_STORE_NAME));

        NeoStore neoStore = newNeoStore( fileName );
        /*
        *  created time | random long | backup version | tx id | store version | next prop
        */
        for ( int i = 0; i < 6; i++ )
        {
            neoStore.nextId();
        }
        neoStore.setCreationTime( storeId.getCreationTime() );
        neoStore.setRandomNumber( storeId.getRandomId() );
        neoStore.setVersion( 0 );
        neoStore.setLastCommittedTx( 1 );
        neoStore.setStoreVersion( storeId.getStoreVersion() );
        neoStore.setGraphNextProp( -1 );
        return neoStore;
    }

    /**
     * Creates a new node store contained in <CODE>fileName</CODE> If filename
     * is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new node store
     */
    public void createNodeStore( File fileName )
    {
        createNodeLabelsStore( new File( fileName.getPath() + LABELS_PART ) );
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( NodeStore.TYPE_DESCRIPTOR ) );
    }

    private void createNodeLabelsStore( File fileName )
    {
        int labelStoreBlockSize = config.get( Configuration.label_block_size );
        createEmptyDynamicStore( fileName, labelStoreBlockSize,
                DynamicArrayStore.VERSION, IdType.NODE_LABELS );
    }

    /**
     * Creates a new relationship store contained in <CODE>fileName</CODE> If
     * filename is <CODE>null</CODE> or the file already exists an <CODE>IOException</CODE>
     * is thrown.
     *
     * @param fileName
     *            File name of the new relationship store
     */
    private void createRelationshipStore( File fileName)
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( RelationshipStore.TYPE_DESCRIPTOR ) );
    }

    /**
     * Creates a new property store contained in <CODE>fileName</CODE> If
     * filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new property store
     */
    public void createPropertyStore( File fileName )
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( PropertyStore.TYPE_DESCRIPTOR ));
        int stringStoreBlockSize = config.get( Configuration.string_block_size );
        int arrayStoreBlockSize = config.get( Configuration.array_block_size );

        createDynamicStringStore(new File( fileName.getPath() + STRINGS_PART), stringStoreBlockSize, IdType.STRING_BLOCK);
        createPropertyKeyTokenStore( new File( fileName.getPath() + INDEX_PART ) );
        createDynamicArrayStore( new File( fileName.getPath() + ARRAYS_PART ), arrayStoreBlockSize );
    }

    /**
     * Creates a new relationship type store contained in <CODE>fileName</CODE>
     * If filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new relationship type store
     */
    private void createRelationshipTypeStore( File fileName )
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( RelationshipTypeTokenStore.TYPE_DESCRIPTOR ));
        createDynamicStringStore( new File( fileName.getPath() + NAMES_PART), TokenStore.NAME_STORE_BLOCK_SIZE, IdType.RELATIONSHIP_TYPE_TOKEN_NAME );
        RelationshipTypeTokenStore store = newRelationshipTypeTokenStore( fileName );
        store.close();
    }

    private void createLabelTokenStore( File fileName )
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( LabelTokenStore.TYPE_DESCRIPTOR ));
        createDynamicStringStore( new File( fileName.getPath() + NAMES_PART), TokenStore.NAME_STORE_BLOCK_SIZE, IdType.LABEL_TOKEN_NAME );
        LabelTokenStore store = newLabelTokenStore( fileName );
        store.close();
    }

    private void createDynamicStringStore( File fileName, int blockSize, IdType idType )
    {
        createEmptyDynamicStore(fileName, blockSize, DynamicStringStore.VERSION, idType);
    }

    private void createPropertyKeyTokenStore( File fileName )
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( PropertyKeyTokenStore.TYPE_DESCRIPTOR ));
        createDynamicStringStore(new File( fileName.getPath() + KEYS_PART), TokenStore.NAME_STORE_BLOCK_SIZE, IdType.PROPERTY_KEY_TOKEN_NAME );
    }

    public void createDynamicArrayStore( File fileName, int blockSize)
    {
        createEmptyDynamicStore(fileName, blockSize, DynamicArrayStore.VERSION, IdType.ARRAY_BLOCK);
    }

    public void createSchemaStore( File fileName )
    {
        createEmptyDynamicStore( fileName, SchemaStore.BLOCK_SIZE, SchemaStore.VERSION, IdType.SCHEMA );
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
     * @param fileName
     *            The file name of the store that will be created
     * @param  baseBlockSize
     *            The number of bytes for each block
     * @param typeAndVersionDescriptor
     *            The type and version descriptor that identifies this store
     */
    public void createEmptyDynamicStore( File fileName, int baseBlockSize,
                                            String typeAndVersionDescriptor, IdType idType)
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
            FileChannel channel = fileSystemAbstraction.create(fileName);
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
        idGeneratorFactory.create( fileSystemAbstraction, new File( fileName.getPath() + ".id"), 0 );
        // TODO highestIdInUse = 0 works now, but not when slave can create store files.
        IdGenerator idGenerator = idGeneratorFactory.open(fileSystemAbstraction, new File( fileName.getPath() + ".id"),
                idType.getGrabSize(), idType, 0 );
        idGenerator.nextId(); // reserve first for blockSize
        idGenerator.close();
    }


    public void createEmptyStore( File fileName, String typeAndVersionDescriptor)
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
            FileChannel channel = fileSystemAbstraction.create(fileName);
            int endHeaderSize = UTF8.encode(typeAndVersionDescriptor).length;
            ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
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
        idGeneratorFactory.create( fileSystemAbstraction, new File( fileName.getPath() + ".id"), 0 );
    }

    public String buildTypeDescriptorAndVersion( String typeDescriptor )
    {
        return typeDescriptor + " " + CommonAbstractStore.ALL_STORES_VERSION;
    }
}
