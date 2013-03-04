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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.LOGICAL_LOG_DEFAULT_NAME;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.TransactionInterceptorProviders;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaConnection;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.PlaceboTm;
import org.neo4j.kernel.impl.transaction.TransactionStateFactory;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class TestXa
{
    private final EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private NeoStoreXaDataSource ds;
    private File logBaseFileName;
    private NeoStoreXaConnection xaCon;
    private Logger log;
    private Level level;
    private Map<String, PropertyIndex> propertyIndexes;

    private static class MyPropertyIndex extends org.neo4j.kernel.impl.core.PropertyIndex
    {
        protected MyPropertyIndex( String key, int keyId )
        {
            super( key, keyId );
        }
    }

    private LockManager lockManager;

    private File path()
    {
        String path = "xatest";
        File file = new File( path );
        fileSystem.mkdirs( file );
        return file;
    }

    private File file( String name )
    {
        return new File( path(), name);
    }

    @Before
    public void setUpNeoStore() throws Exception
    {
        log = Logger
                .getLogger( "org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog/"
                        + "nioneo_logical.log" );
        level = log.getLevel();
        log.setLevel( Level.OFF );
        log = Logger
                .getLogger( "org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource" );
        log.setLevel( Level.OFF );
        propertyIndexes = new HashMap<String, PropertyIndex>();

        StoreFactory sf = new StoreFactory( new Config( Collections.<String, String>emptyMap(),
                GraphDatabaseSettings.class ), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fileSystem, StringLogger.DEV_NULL, null );
        sf.createNeoStore( file( "neo" ) ).close();

        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        logBaseFileName = ds.getXaContainer().getLogicalLog().getBaseFileName();
    }

    @After
    public void tearDownNeoStore()
    {
        ds.stop();
        log.setLevel( level );
        log = Logger
                .getLogger( "org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog/"
                        + "nioneo_logical.log" );
        log.setLevel( level );
        log = Logger
                .getLogger( "org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource" );
        log.setLevel( level );

        for ( String file : new String[] {
                "neo",
                "neo.nodestore.db",
                "neo.nodestore.db.labels",
                "neo.propertystore.db",
                "neo.propertystore.db.index",
                "neo.propertystore.db.index.keys",
                "neo.propertystore.db.strings",
                "neo.propertystore.db.arrays",
                "neo.relationshipstore.db",
                "neo.relationshiptypestore.db",
                "neo.relationshiptypestore.db.names",
                "neo.schemastore.db",
        } )
        {
            fileSystem.deleteFile( file( file ) );
            fileSystem.deleteFile( file( file + ".id" ) );
        }
        
        File file = new File( "." );
        for ( File nioFile : fileSystem.listFiles( file ) )
        {
            if ( nioFile.getName().startsWith( "nioneo_logical.log" ) )
            {
                assertTrue( "Couldn't delete '" + nioFile.getPath() + "'", fileSystem.deleteFile( nioFile ) );
            }
        }
    }

    private void deleteLogicalLogIfExist()
    {
        File file = new File( logBaseFileName.getPath() + ".1" );
        if ( fileSystem.fileExists( file ) )
        {
            assertTrue( fileSystem.deleteFile( file ) );
        }
        file = new File( logBaseFileName.getPath() + ".2" );
        if ( fileSystem.fileExists( file ) )
        {
            assertTrue( fileSystem.deleteFile( file ) );
        }
        file = new File( logBaseFileName.getPath() + ".active" );
        assertTrue( fileSystem.deleteFile( file ) );
        
        // Delete the last .v file
        for ( int i = 5; i >= 0; i-- )
            if ( fileSystem.deleteFile( new File( logBaseFileName.getPath() + ".v" + i ) ) )
                break;
    }

    public static void renameCopiedLogicalLog( FileSystemAbstraction fileSystem,
            Pair<Pair<File, File>, Pair<File, File>> files ) throws IOException
    {
        fileSystem.deleteFile( files.first().first() );
        fileSystem.renameFile( files.first().other(), files.first().first() );
        
        fileSystem.deleteFile( files.other().first() );
        fileSystem.renameFile( files.other().other(), files.other().first() );
    }

    private void truncateLogicalLog( int size ) throws IOException
    {
        char active = '1';
        FileChannel af = fileSystem.open( new File( logBaseFileName.getPath() + ".active" ), "r" );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        af.read( buffer );
        af.close();
        buffer.flip();
        active = buffer.asCharBuffer().get();
        buffer.clear();
        FileChannel fileChannel = fileSystem.open( new File( logBaseFileName.getPath() + "." + active ), "rw" );
//        System.out.println( fileChannel.size() );
        if ( fileChannel.size() > size )
        {
            fileChannel.truncate( size );
        }
        else
        {
            fileChannel.position( size );
            ByteBuffer buf = ByteBuffer.allocate( 1 );
            buf.put( (byte) 0 ).flip();
            fileChannel.write( buf );
        }
        fileChannel.force( false );
        fileChannel.close();
    }

    public static Pair<Pair<File, File>, Pair<File, File>> copyLogicalLog( FileSystemAbstraction fileSystem,
            File logBaseFileName ) throws IOException
    {
        char active = '1';
        File activeLog = new File( logBaseFileName.getPath() + ".active" );
        FileChannel af = fileSystem.open( activeLog, "r" );
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        af.read( buffer );
        buffer.flip();
        File activeLogBackup = new File( logBaseFileName.getPath() + ".bak.active" );
        FileChannel activeCopy = fileSystem.open( activeLogBackup, "rw" );
        activeCopy.write( buffer );
        activeCopy.close();
        af.close();
        buffer.flip();
        active = buffer.asCharBuffer().get();
        buffer.clear();
        File currentLog = new File( logBaseFileName.getPath() + "." + active );
        FileChannel source = fileSystem.open( currentLog, "r" );
        File currentLogBackup = new File( logBaseFileName.getPath() + ".bak." + active );
        FileChannel dest = fileSystem.open( currentLogBackup, "rw" );
        int read = -1;
        do
        {
            read = source.read( buffer );
            buffer.flip();
            dest.write( buffer );
            buffer.clear();
        }
        while ( read == 1024 );
        source.close();
        dest.close();
        return Pair.of( Pair.of( activeLog, activeLogBackup ), Pair.of( currentLog, currentLogBackup ) );
    }

    private PropertyIndex index( String key )
    {
        PropertyIndex result = propertyIndexes.get( key );
        if ( result != null )
        {
            return result;
        }

        int id = (int) ds.nextId( PropertyIndex.class );
        PropertyIndex index = new MyPropertyIndex( key, id );
        propertyIndexes.put( key, index );
        xaCon.getWriteTransaction().createPropertyIndex( key, id );
        return index;
    }

    @Test
    public void testLogicalLog() throws Exception
    {
        Xid xid = new XidImpl( new byte[1], new byte[1] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node2 );
        PropertyData n1prop1 = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop1" ), "string1" );
        xaCon.getWriteTransaction().nodeLoadProperties( node1, false );
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getWriteTransaction().createRelationshipType( relType1,
                "relationshiptype1" );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel1, relType1, node1, node2 );
        PropertyData r1prop1 = xaCon.getWriteTransaction().relAddProperty(
                rel1, index( "prop1" ), "string1" );
        n1prop1 = xaCon.getWriteTransaction().nodeChangeProperty( node1,
                n1prop1, "string2" );
        r1prop1 = xaCon.getWriteTransaction().relChangeProperty( rel1, r1prop1,
                "string2" );
        xaCon.getWriteTransaction().nodeRemoveProperty( node1, n1prop1 );
        xaCon.getWriteTransaction().relRemoveProperty( rel1, r1prop1 );
        xaCon.getWriteTransaction().relDelete( rel1 );
        xaCon.getWriteTransaction().nodeDelete( node1 );
        xaCon.getWriteTransaction().nodeDelete( node2 );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.commit( xid, true );
        Pair<Pair<File, File>, Pair<File, File>> copies = copyLogicalLog( fileSystem, logBaseFileName );
        xaCon.clearAllTransactions();
        ds.stop();
        deleteLogicalLogIfExist();
        renameCopiedLogicalLog( fileSystem, copies );
        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaCon.clearAllTransactions();
    }

    private NeoStoreXaDataSource newNeoStore() throws InstantiationException,
            IOException
    {
        final Config config = new Config( MapUtil.stringMap(
                        InternalAbstractGraphDatabase.Configuration.store_dir.name(), path().getPath(),
                        InternalAbstractGraphDatabase.Configuration.neo_store.name(), file( "neo" ).getPath(),
                        InternalAbstractGraphDatabase.Configuration.logical_log.name(),
                        file( LOGICAL_LOG_DEFAULT_NAME ).getPath() ), GraphDatabaseSettings.class );

        StoreFactory sf = new StoreFactory( config, new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(),
                fileSystem, StringLogger.DEV_NULL, null );

        PlaceboTm txManager = new PlaceboTm( lockManager, TxIdGenerator.DEFAULT );
        LogBufferFactory logBufferFactory = new DefaultLogBufferFactory();

        // Since these tests fiddle with copying logical logs and such themselves
        // make sure all history logs are removed before opening the store
        for ( File file : fileSystem.listFiles( path() ) )
        {
            if ( file.isFile() && file.getName().startsWith( LOGICAL_LOG_DEFAULT_NAME + ".v" ) )
            {
                fileSystem.deleteFile( file );
            }
        }

        NeoStoreXaDataSource neoStoreXaDataSource = new NeoStoreXaDataSource( config, sf, lockManager,
                StringLogger.DEV_NULL,
                new XaFactory( config, TxIdGenerator.DEFAULT, txManager,
                        logBufferFactory, fileSystem, new DevNullLoggingService(), RecoveryVerifier.ALWAYS_VALID,
                        LogPruneStrategies.NO_PRUNING ), TransactionStateFactory.noStateFactory( new DevNullLoggingService() ),
                        noCacheAccess(), mock( SchemaIndexProvider.class), new TransactionInterceptorProviders( Collections.<TransactionInterceptorProvider>emptyList(),
                        new DependencyResolver()

                        {
                            @Override
                            public <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException
                            {
                                return (T) config;
                            }
                        } ), null, new SingleLoggingService( DEV_NULL ) );
        neoStoreXaDataSource.init();
        neoStoreXaDataSource.start();
        return neoStoreXaDataSource;
    }

    private CacheAccessBackDoor noCacheAccess()
    {
        return new CacheAccessBackDoor()
        {
            @Override
            public void removeSchemaRuleFromCache( long id )
            {
            }
            
            @Override
            public void removeRelationshipTypeFromCache( int id )
            {
            }
            
            @Override
            public void removeRelationshipFromCache( long id )
            {
            }
            
            @Override
            public void removeNodeFromCache( long nodeId )
            {
            }
            
            @Override
            public void removeGraphPropertiesFromCache()
            {
            }
            
            @Override
            public void addSchemaRule( SchemaRule schemaRule )
            {
            }
            
            @Override
            public void addRelationshipType( NameData type )
            {
            }
            
            @Override
            public void addPropertyIndex( NameData index )
            {
            }

            @Override
            public void patchDeletedRelationshipNodes( long relId, long firstNodeId, long firstNodeNextRelId,
                    long secondNodeId, long secondNodeNextRelId )
            {
            }
        };
    }

    @Test
    public void testLogicalLogPrepared() throws Exception
    {
        Xid xid = new XidImpl( new byte[2], new byte[2] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node2 );
        PropertyData n1prop1 = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop1" ), "string1" );
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getWriteTransaction().createRelationshipType( relType1,
                "relationshiptype1" );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel1, relType1, node1, node2 );
        PropertyData r1prop1 = xaCon.getWriteTransaction().relAddProperty(
                rel1, index( "prop1" ), "string1" );
        xaCon.getWriteTransaction().nodeChangeProperty( node1, n1prop1, "string2" );
        xaCon.getWriteTransaction().relChangeProperty( rel1, r1prop1,
                "string2" );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        ds.rotateLogicalLog();
        Pair<Pair<File, File>, Pair<File, File>> copies = copyLogicalLog( fileSystem, logBaseFileName );
        xaCon.clearAllTransactions();
        ds.stop();
        deleteLogicalLogIfExist();
        renameCopiedLogicalLog( fileSystem, copies );
        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaRes.commit( xid, true );
        xaCon.clearAllTransactions();
    }

    @Test
    public void testLogicalLogPreparedPropertyBlocks() throws Exception
    {
        Xid xid = new XidImpl( new byte[2], new byte[2] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        PropertyData n1prop1 = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop1" ),
                new long[]{1 << 23, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1} );
        PropertyData n1prop2 = xaCon.getWriteTransaction().nodeAddProperty(
                node1,
                index( "prop2" ),
                new long[]{1 << 23, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1} );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        ds.rotateLogicalLog();
        copyClearRename();

        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaRes.commit( xid, true );
        xaCon.clearAllTransactions();

        xid = new XidImpl( new byte[2], new byte[2] );
        xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        xaCon.getWriteTransaction().nodeRemoveProperty( node1, n1prop1 );
        xaCon.getWriteTransaction().nodeAddProperty(
                node1,
                index( "prop3" ),
                new long[]{1 << 23, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1} );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        ds.rotateLogicalLog();
        copyClearRename();
        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaRes.commit( xid, true );
        xaCon.clearAllTransactions();
    }

    private void copyClearRename() throws IOException
    {
        copyClearRename( true );
    }
    
    private void copyClearRename( boolean clearTransactions ) throws IOException
    {
        Pair<Pair<File, File>, Pair<File, File>> copies = copyLogicalLog( fileSystem, logBaseFileName );
        if ( clearTransactions )
            xaCon.clearAllTransactions();
        ds.stop();
        deleteLogicalLogIfExist();
        renameCopiedLogicalLog( fileSystem, copies );
    }

    @Test
    public void makeSureRecordsAreCreated() throws Exception
    {
        Xid xid = new XidImpl( new byte[2], new byte[2] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        xaCon.getWriteTransaction().nodeAddProperty( node1, index( "prop1" ),
                new long[]{1 << 63, 1, 1} );
        xaCon.getWriteTransaction().nodeAddProperty( node1, index( "prop2" ),
                new long[]{1 << 63, 1, 1} );
        PropertyData toRead = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop3" ),
                new long[]{1 << 63, 1, 1} );
        PropertyData toDelete = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop4" ),
                new long[]{1 << 63, 1, 1} );
        xaRes.end( xid, XAResource.TMSUCCESS );
        // xaRes.prepare( xid );
        xaRes.commit( xid, true );
        ds.rotateLogicalLog();
        copyClearRename();

        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();

        xid = new XidImpl( new byte[2], new byte[2] );
        xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );

        xaCon.getWriteTransaction().nodeRemoveProperty( node1, toDelete );

        xaRes.end( xid, XAResource.TMSUCCESS );
        // xaRes.prepare( xid );
        xaRes.commit( xid, true );
        ds.rotateLogicalLog();
        copyClearRename();

        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        xid = new XidImpl( new byte[2], new byte[2] );
        xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );

        assertTrue( Arrays.equals(
                (long[]) toRead.getValue(),
                (long[]) xaCon.getWriteTransaction().nodeLoadProperties( node1,
                        false ).get( toRead.getIndex() ).getValue() ) );

        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        xaCon.clearAllTransactions();
        ds.stop();
        deleteLogicalLogIfExist();
//        renameCopiedLogicalLog( path() );
    }

    @Test
    public void testDynamicRecordsInLog() throws Exception
    {
        Xid xid = new XidImpl( new byte[2], new byte[2] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        PropertyData toChange = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop1" ), "hi" );
        PropertyData toRead = xaCon.getWriteTransaction().nodeAddProperty(
                node1,
                index( "prop2" ),
                new long[]{1 << 23, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1} );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        ds.rotateLogicalLog();
        copyClearRename();

        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaRes.commit( xid, true );
        xaCon.clearAllTransactions();
        xid = new XidImpl( new byte[2], new byte[2] );
        xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        xaCon.getWriteTransaction().nodeChangeProperty( node1, toChange, "hI" );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        ds.rotateLogicalLog();
        copyClearRename();

        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaRes.commit( xid, true );
        xaCon.clearAllTransactions();

        assertTrue(
                Arrays.equals( new long[]{1 << 23, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
                        (long[]) xaCon.getWriteTransaction().loadPropertyValue( toRead ) ) );

    }

    @Test
    public void testLogicalLogPrePrepared() throws Exception
    {
        Xid xid = new XidImpl( new byte[3], new byte[3] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node2 );
        PropertyData n1prop1 = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop1" ), "string1" );
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getWriteTransaction().createRelationshipType( relType1,
                "relationshiptype1" );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel1, relType1, node1, node2 );
        PropertyData r1prop1 = xaCon.getWriteTransaction().relAddProperty(
                rel1, index( "prop1" ), "string1" );
        xaCon.getWriteTransaction().nodeChangeProperty( node1, n1prop1, "string2" );
        xaCon.getWriteTransaction().relChangeProperty( rel1, r1prop1,
                "string2" );
        xaRes.end( xid, XAResource.TMSUCCESS );
        copyClearRename();
        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
    }

    @Test
    public void testBrokenNodeCommand() throws Exception
    {
        Xid xid = new XidImpl( new byte[4], new byte[4] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        xaCon.clearAllTransactions();
        copyClearRename();
        truncateLogicalLog( 94 );
        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaCon.clearAllTransactions();
    }

    @Test
    public void testBrokenCommand() throws Exception
    {
        Xid xid = new XidImpl( new byte[4], new byte[4] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        copyClearRename();
        truncateLogicalLog( 94 );
        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaCon.clearAllTransactions();
    }

    @Test
    public void testBrokenPrepare() throws Exception
    {
        Xid xid = new XidImpl( new byte[4], new byte[4] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node2 );
        /*PropertyData n1prop1 = */
        xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop1" ), "string value 1" );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        copyClearRename();
        truncateLogicalLog( 243 );
        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaCon.clearAllTransactions();
    }

    @Test
    public void testBrokenDone() throws Exception
    {
        Xid xid = new XidImpl( new byte[4], new byte[4] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node2 );
        /*PropertyData n1prop1 = */
        xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop1" ), "string value 1" );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        xaRes.commit( xid, false );
        copyClearRename( false );
        truncateLogicalLog( 318 );
        ds = newNeoStore();
        xaCon = ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaCon.clearAllTransactions();
    }

    @Test
    public void testLogVersion()
    {
        long creationTime = ds.getCreationTime();
        long randomIdentifier = ds.getRandomIdentifier();
        long currentVersion = ds.getCurrentLogVersion();
        assertEquals( currentVersion, ds.incrementAndGetLogVersion() );
        assertEquals( currentVersion + 1, ds.incrementAndGetLogVersion() );
        assertEquals( creationTime, ds.getCreationTime() );
        assertEquals( randomIdentifier, ds.getRandomIdentifier() );
    }

    @Test
    public void testLogicalLogRotation() throws Exception
    {
        // TODO fix somehow
//        ds.keepLogicalLogs( true );
        Xid xid = new XidImpl( new byte[1], new byte[1] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getWriteTransaction().nodeCreate( node2 );
        PropertyData n1prop1 = xaCon.getWriteTransaction().nodeAddProperty(
                node1, index( "prop1" ), "string1" );
        xaCon.getWriteTransaction().nodeLoadProperties( node1, false );
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getWriteTransaction().createRelationshipType( relType1, "relationshiptype1" );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getWriteTransaction().relationshipCreate( rel1, relType1, node1, node2 );
        PropertyData r1prop1 = xaCon.getWriteTransaction().relAddProperty(
                rel1, index( "prop1" ), "string1" );
        n1prop1 = xaCon.getWriteTransaction().nodeChangeProperty( node1,
                n1prop1, "string2" );
        r1prop1 = xaCon.getWriteTransaction().relChangeProperty( rel1, r1prop1,
                "string2" );
        xaCon.getWriteTransaction().nodeRemoveProperty( node1, n1prop1 );
        xaCon.getWriteTransaction().relRemoveProperty( rel1, r1prop1 );
        xaCon.getWriteTransaction().relDelete( rel1 );
        xaCon.getWriteTransaction().nodeDelete( node1 );
        xaCon.getWriteTransaction().nodeDelete( node2 );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.commit( xid, true );
        long currentVersion = ds.getCurrentLogVersion();
        ds.rotateLogicalLog();
        assertTrue( logicalLogExists( currentVersion ) );
        ds.rotateLogicalLog();
        assertTrue( logicalLogExists( currentVersion ) );
        assertTrue( logicalLogExists( currentVersion + 1 ) );
    }

    private boolean logicalLogExists( long version ) throws IOException
    {
        ReadableByteChannel log = ds.getLogicalLog( version );
        try
        {
            return log != null;
        }
        finally
        {
            log.close();
        }
    }
}
