/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Logger;

import org.neo4j.graphdb.factory.GraphDatabaseSetting;
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
        public static final GraphDatabaseSetting.IntegerSetting string_block_size = GraphDatabaseSettings.string_block_size;
        public static final GraphDatabaseSetting.IntegerSetting array_block_size = GraphDatabaseSettings.array_block_size;
    }
    
    protected static final Logger logger = Logger.getLogger(StoreFactory.class.getName());

    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;
    private final WindowPoolFactory windowPoolFactory;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final StringLogger stringLogger;
    private final TxHook txHook;

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

        String store = config.get( GraphDatabaseSettings.neo_store );
        boolean created = false;
        if ( !readOnly && !fileSystemAbstraction.fileExists( store ))
        {
            stringLogger.logMessage( "Creating new db @ " + store, true );
            fileSystemAbstraction.autoCreatePath( store );
            createNeoStore( store ).close();
            created = true;
        }
        return created;
    }

    public NeoStore newNeoStore(String fileName)
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
    }

    NeoStore attemptNewNeoStore( String fileName )
    {
        return new NeoStore( fileName, config, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction,
                stringLogger, txHook,
                newRelationshipTypeStore(fileName + ".relationshiptypestore.db"),
                newPropertyStore(fileName + ".propertystore.db"),
                newRelationshipStore(fileName + ".relationshipstore.db"),
                newNodeStore(fileName + ".nodestore.db"));
    }

    private void tryToUpgradeStores( String fileName )
    {
        new StoreUpgrader(config, stringLogger, new ConfigMapUpgradeConfiguration(config),
                new UpgradableDatabase(), new StoreMigrator( new VisibleMigrationProgressMonitor( System.out ) ),
                new DatabaseFiles(), idGeneratorFactory, fileSystemAbstraction ).attemptUpgrade( fileName );
    }

    private DynamicStringStore newDynamicStringStore(String s, IdType nameIdType)
    {
        return new DynamicStringStore( s, config, nameIdType, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger);
    }

    private RelationshipTypeStore newRelationshipTypeStore(String s)
    {
        DynamicStringStore nameStore = newDynamicStringStore( s + ".names", IdType.RELATIONSHIP_TYPE_BLOCK );
        return new RelationshipTypeStore( s, config, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger, nameStore );
    }

    private PropertyStore newPropertyStore(String s)
    {
        DynamicStringStore stringPropertyStore = newDynamicStringStore(s + ".strings", IdType.STRING_BLOCK);
        PropertyIndexStore propertyIndexStore = newPropertyIndexStore(s + ".index");
        DynamicArrayStore arrayPropertyStore = newDynamicArrayStore( s + ".arrays" );
        return new PropertyStore( s, config, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction, stringLogger,
                stringPropertyStore, propertyIndexStore, arrayPropertyStore);
    }

    private PropertyIndexStore newPropertyIndexStore(String s)
    {
        DynamicStringStore nameStore = newDynamicStringStore(s + ".keys", IdType.PROPERTY_INDEX_BLOCK);
        return new PropertyIndexStore( s, config, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger, nameStore );
    }

    private RelationshipStore newRelationshipStore(String s)
    {
        return new RelationshipStore( s, config, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger);
    }

    private DynamicArrayStore newDynamicArrayStore(String s)
    {
        return new DynamicArrayStore( s, config, IdType.ARRAY_BLOCK, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, stringLogger);
    }

    private NodeStore newNodeStore(String s)
    {
        return new NodeStore( s, config, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction, stringLogger );
    }

    public NeoStore createNeoStore(String fileName)
    {
        return createNeoStore( fileName, new StoreId() );
    }

    public NeoStore createNeoStore(String fileName, StoreId storeId)
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( NeoStore.TYPE_DESCRIPTOR ) );
        createNodeStore(fileName + ".nodestore.db");
        createRelationshipStore(fileName + ".relationshipstore.db");
        createPropertyStore(fileName + ".propertystore.db");
        createRelationshipTypeStore(fileName + ".relationshiptypestore.db");
/*
        if ( !config.containsKey( "neo_store" ) )
        {
            // TODO Ugly
            Map<String, Object> newConfig = new HashMap<String, Object>( config );
            newConfig.put( "neo_store", fileName );
            config = newConfig;
        }
*/
        NeoStore neoStore = newNeoStore( fileName );
        /*
        *  created time | random long | backup version | tx id | store version | next prop
        */
        for ( int i = 0; i < 6; i++ ) neoStore.nextId();
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
    private void createNodeStore( String fileName )
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( NodeStore.TYPE_DESCRIPTOR ) );
        NodeStore store = newNodeStore( fileName );
        NodeRecord nodeRecord = new NodeRecord( store.nextId(), Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue() );
        nodeRecord.setInUse( true );
        store.updateRecord( nodeRecord );
        store.close();
    }

    /**
     * Creates a new relationship store contained in <CODE>fileName</CODE> If
     * filename is <CODE>null</CODE> or the file already exists an <CODE>IOException</CODE>
     * is thrown.
     *
     * @param fileName
     *            File name of the new relationship store
     * @throws IOException
     *             If unable to create relationship store or name null
     */
    private void createRelationshipStore( String fileName)
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( RelationshipStore.TYPE_DESCRIPTOR )  );
    }

    /**
     * Creates a new property store contained in <CODE>fileName</CODE> If
     * filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new property store
     * @throws IOException
     *             If unable to create property store or name null
     */
    private void createPropertyStore( String fileName )
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( PropertyStore.TYPE_DESCRIPTOR ));
        int stringStoreBlockSize = config.get( Configuration.string_block_size );
        int arrayStoreBlockSize = config.get( Configuration.array_block_size );

        createDynamicStringStore(fileName + ".strings", stringStoreBlockSize, IdType.STRING_BLOCK);
        createPropertyIndexStore(fileName + ".index");
        createDynamicArrayStore(fileName + ".arrays", arrayStoreBlockSize);
    }

    /**
     * Creates a new relationship type store contained in <CODE>fileName</CODE>
     * If filename is <CODE>null</CODE> or the file already exists an
     * <CODE>IOException</CODE> is thrown.
     *
     * @param fileName
     *            File name of the new relationship type store
     * @throws IOException
     *             If unable to create store or name null
     */
    private void createRelationshipTypeStore( String fileName )
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( RelationshipTypeStore.TYPE_DESCRIPTOR ));
        createDynamicStringStore( fileName + ".names", AbstractNameStore.NAME_STORE_BLOCK_SIZE, IdType.RELATIONSHIP_TYPE_BLOCK);
        RelationshipTypeStore store = newRelationshipTypeStore( fileName );
        store.close();
    }

    private void createDynamicStringStore( String fileName, int blockSize, IdType idType )
    {
        createEmptyDynamicStore(fileName, blockSize, DynamicStringStore.VERSION, idType);
    }

    private void createPropertyIndexStore( String fileName)
    {
        createEmptyStore( fileName, buildTypeDescriptorAndVersion( PropertyIndexStore.TYPE_DESCRIPTOR ));
        createDynamicStringStore(fileName + ".keys", PropertyIndexStore.NAME_STORE_BLOCK_SIZE, IdType.PROPERTY_INDEX_BLOCK);
    }

    public void createDynamicArrayStore( String fileName, int blockSize)
    {
        createEmptyDynamicStore(fileName, blockSize, DynamicArrayStore.VERSION, IdType.ARRAY_BLOCK);
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
     *
     * @throws IOException
     *             If fileName is null or if file exists or illegal block size
     */
    public void createEmptyDynamicStore( String fileName, int baseBlockSize,
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
        idGeneratorFactory.create( fileSystemAbstraction, fileName + ".id", 0 );
        // TODO highestIdInUse = 0 works now, but not when slave can create store files.
        IdGenerator idGenerator = idGeneratorFactory.open(fileSystemAbstraction, fileName + ".id", 1, idType );
        idGenerator.nextId(); // reserv first for blockSize
        idGenerator.close();
    }


    public void createEmptyStore( String fileName, String typeAndVersionDescriptor)
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
        idGeneratorFactory.create( fileSystemAbstraction, fileName + ".id", 0 );
    }

    public String buildTypeDescriptorAndVersion( String typeDescriptor )
    {
        return typeDescriptor + " " + CommonAbstractStore.ALL_STORES_VERSION;
    }
}
