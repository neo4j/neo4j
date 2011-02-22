/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaConnection;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;

public class TestXa extends AbstractNeo4jTestCase
{
    public static IdGeneratorFactory ID_GENERATOR_FACTORY =
        CommonFactories.defaultIdGeneratorFactory();
    
    private NeoStoreXaDataSource ds;
    private NeoStoreXaConnection xaCon;
    private Logger log;
    private Level level;

    private static class MyPropertyIndex extends
        org.neo4j.kernel.impl.core.PropertyIndex
    {
        private static Map<String,PropertyIndex> stringToIndex = new HashMap<String,PropertyIndex>();
        private static Map<Integer,PropertyIndex> intToIndex = new HashMap<Integer,PropertyIndex>();

        protected MyPropertyIndex( String key, int keyId )
        {
            super( key, keyId );
        }

        public static Iterable<PropertyIndex> index( String key )
        {
            if ( stringToIndex.containsKey( key ) )
            {
                return Arrays.asList( new PropertyIndex[] { stringToIndex
                    .get( key ) } );
            }
            return Collections.emptyList();
        }

//        public static PropertyIndex getIndexFor( int index )
//        {
//            return intToIndex.get( index );
//        }

        public static void add( MyPropertyIndex index )
        {
            // TODO Auto-generated method stub
            stringToIndex.put( index.getKey(), index );
            intToIndex.put( index.getKeyId(), index );
        }
    }
    
    @Override
    protected boolean restartGraphDbBetweenTests()
    {
        return true;
    }

    private PropertyIndex createDummyIndex( int id, String key )
    {
        MyPropertyIndex index = new MyPropertyIndex( key, id );
        MyPropertyIndex.add( index );
        return index;
    }

    private LockManager lockManager;
    private LockReleaser lockReleaser;
    
    private String path()
    {
        String path = getStorePath( "xatest" );
        new File( path ).mkdirs();
        return path;
    }
    
    private String file( String name )
    {
        return path() + File.separator + name;
    }
    
    @BeforeClass
    public static void deleteFiles()
    {
        deleteFileOrDirectory( getStorePath( "xatest" ) );
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
        NeoStore.createStore( file( "neo" ), MapUtil.map(
                IdGeneratorFactory.class, ID_GENERATOR_FACTORY ) );
        lockManager = getEmbeddedGraphDb().getConfig().getLockManager();
        lockReleaser = getEmbeddedGraphDb().getConfig().getLockReleaser();
        ds = newNeoStore();
//        ds = new NeoStoreXaDataSource( file( "neo" ), file( "nioneo_logical.log" ),
//            lockManager, lockReleaser );
        xaCon = (NeoStoreXaConnection) ds.getXaConnection();
    }

    @After
    public void tearDownNeoStore()
    {
        ds.close();
        log.setLevel( level );
        log = Logger
            .getLogger( "org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog/"
                + "nioneo_logical.log" );
        log.setLevel( level );
        log = Logger
            .getLogger( "org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource" );
        log.setLevel( level );
        File file = new File( file( "neo" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.id" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.nodestore.db" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.nodestore.db.id" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.propertystore.db" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.propertystore.db.id" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.propertystore.db.index" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.propertystore.db.index.id" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.propertystore.db.index.keys" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.propertystore.db.index.keys.id" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.propertystore.db.strings" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.propertystore.db.strings.id" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.propertystore.db.arrays" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.propertystore.db.arrays.id" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.relationshipstore.db" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.relationshipstore.db.id" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.relationshiptypestore.db" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.relationshiptypestore.db.id" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.relationshiptypestore.db.names" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "neo.relationshiptypestore.db.names.id" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( path() );
        for ( File nioFile : file.listFiles() )
        {
            if ( nioFile.getName().startsWith( "nioneo_logical.log" ) )
            {
                assertTrue( nioFile.delete() );
            }
        }
    }

    private void deleteLogicalLogIfExist()
    {
        File file = new File( file( "nioneo_logical.log.1" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "nioneo_logical.log.2" ) );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( file( "nioneo_logical.log.active" ) );
        assertTrue( file.delete() );
    }

    public static void renameCopiedLogicalLog( Pair<Pair<File, File>, Pair<File, File>> backedUpLogFiles )
    {
        Pair<File, File> active = backedUpLogFiles.first();
        assertTrue( active.other().renameTo( active.first() ) );
        
        Pair<File, File> current = backedUpLogFiles.other();
        assertTrue( current.other().renameTo( current.first() ) );
    }

    private void truncateLogicalLog( int size ) throws IOException
    {
        char active = '1';
        FileChannel af = new RandomAccessFile( file( "nioneo_logical.log.active" ), 
            "r" ).getChannel();
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        af.read( buffer );
        af.close();
        buffer.flip();
        active = buffer.asCharBuffer().get();
        buffer.clear();
        FileChannel fileChannel = new RandomAccessFile( file( "nioneo_logical.log." + 
            active ), "rw" ).getChannel();
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
    
    public static Pair<Pair<File, File>, Pair<File, File>> copyLogicalLog( String storeDir ) throws IOException
    {
        char active = '1';
        File activeLog = new File( storeDir, "nioneo_logical.log.active" );
        FileChannel af = new RandomAccessFile( activeLog, 
            "r" ).getChannel();
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        af.read( buffer );
        buffer.flip();
        File activeLogBackup = new File( storeDir, "nioneo_logical.log.bak.active" );
        FileChannel activeCopy = new RandomAccessFile( 
                activeLogBackup, "rw" ).getChannel();
        activeCopy.write( buffer );
        activeCopy.close();
        af.close();
        buffer.flip();
        active = buffer.asCharBuffer().get();
        buffer.clear();
        File currentLog = new File( storeDir, "nioneo_logical.log." + 
            active );
        FileChannel source = new RandomAccessFile( currentLog, "r" ).getChannel();
        File currentLogBackup = new File( storeDir, "nioneo_logical.log.bak." + 
            active );
        FileChannel dest = new RandomAccessFile( currentLogBackup, "rw" ).getChannel();
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
        Iterator<PropertyIndex> itr = MyPropertyIndex.index( key ).iterator();
        if ( !itr.hasNext() )
        {
            int id = (int) ds.nextId( PropertyIndex.class );
            PropertyIndex index = createDummyIndex( id, key );
            xaCon.getPropertyIndexConsumer().createPropertyIndex( id, key );
            return index;
        }
        return itr.next();
    }

    @Test
    public void testLogicalLog() throws Exception
    {
        Xid xid = new XidImpl( new byte[1], new byte[1] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node2 );
        long n1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getNodeConsumer().addProperty( node1, n1prop1,
            index( "prop1" ), "string1" );
        xaCon.getNodeConsumer().getProperties( node1, false );
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getRelationshipTypeConsumer().addRelationshipType( relType1,
            "relationshiptype1" );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getRelationshipConsumer().createRelationship( rel1, node1,
            node2, relType1 );
        long r1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getRelationshipConsumer().addProperty( rel1, r1prop1,
            index( "prop1" ), "string1" );
        xaCon.getNodeConsumer().changeProperty( node1, n1prop1, "string2" );
        xaCon.getRelationshipConsumer().changeProperty( rel1, r1prop1,
            "string2" );
        xaCon.getNodeConsumer().removeProperty( node1, n1prop1 );
        xaCon.getRelationshipConsumer().removeProperty( rel1, r1prop1 );
        xaCon.getRelationshipConsumer().deleteRelationship( rel1 );
        xaCon.getNodeConsumer().deleteNode( node1 );
        xaCon.getNodeConsumer().deleteNode( node2 );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.commit( xid, true );
        Pair<Pair<File, File>, Pair<File, File>> copy = copyLogicalLog( path() );
        xaCon.clearAllTransactions();
        ds.close();
        deleteLogicalLogIfExist();
        renameCopiedLogicalLog( copy );
        ds = newNeoStore();
//        ds = new NeoStoreXaDataSource( file( "neo" ), file( "nioneo_logical.log" ),
//            lockManager, lockReleaser );
        xaCon = (NeoStoreXaConnection) ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaCon.clearAllTransactions();
    }
    
    private NeoStoreXaDataSource newNeoStore() throws InstantiationException,
            IOException
    {
        return new NeoStoreXaDataSource( MapUtil.genericMap(
                LockManager.class, lockManager,
                LockReleaser.class, lockReleaser,
                IdGeneratorFactory.class, ID_GENERATOR_FACTORY,
                TxIdGenerator.class, TxIdGenerator.DEFAULT,
                "store_dir", path(),
                "neo_store", file( "neo" ),
                "logical_log", file( "nioneo_logical.log" ) ) );
    }

    @Test
    public void testLogicalLogPrepared() throws Exception
    {
        Xid xid = new XidImpl( new byte[2], new byte[2] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node2 );
        long n1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getNodeConsumer().addProperty( node1, n1prop1,
            index( "prop1" ), "string1" );
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getRelationshipTypeConsumer().addRelationshipType( relType1,
            "relationshiptype1" );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getRelationshipConsumer().createRelationship( rel1, node1,
            node2, relType1 );
        long r1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getRelationshipConsumer().addProperty( rel1, r1prop1,
            index( "prop1" ), "string1" );
        xaCon.getNodeConsumer().changeProperty( node1, n1prop1, "string2" );
        xaCon.getRelationshipConsumer().changeProperty( rel1, r1prop1,
            "string2" );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        Pair<Pair<File, File>, Pair<File, File>> copy = copyLogicalLog( path() );
        xaCon.clearAllTransactions();
        ds.close();
        deleteLogicalLogIfExist();
        renameCopiedLogicalLog( copy );
        ds = newNeoStore();
//        ds = new NeoStoreXaDataSource( file( "neo" ), file( "nioneo_logical.log" ),
//            lockManager, lockReleaser );
        xaCon = (NeoStoreXaConnection) ds.getXaConnection();
        xaRes = xaCon.getXaResource();
        assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
        xaRes.commit( xid, true );
        xaCon.clearAllTransactions();
    }

    @Test
    public void testLogicalLogPrePrepared() throws Exception
    {
        Xid xid = new XidImpl( new byte[3], new byte[3] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node2 );
        long n1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getNodeConsumer().addProperty( node1, n1prop1,
            index( "prop1" ), "string1" );
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getRelationshipTypeConsumer().addRelationshipType( relType1,
            "relationshiptype1" );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getRelationshipConsumer().createRelationship( rel1, node1,
            node2, relType1 );
        long r1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getRelationshipConsumer().addProperty( rel1, r1prop1,
            index( "prop1" ), "string1" );
        xaCon.getNodeConsumer().changeProperty( node1, n1prop1, "string2" );
        xaCon.getRelationshipConsumer().changeProperty( rel1, r1prop1,
            "string2" );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaCon.clearAllTransactions();
        Pair<Pair<File, File>, Pair<File, File>> copy = copyLogicalLog( path() );
        ds.close();
        deleteLogicalLogIfExist();
        renameCopiedLogicalLog( copy );
        ds = newNeoStore();
//        ds = new NeoStoreXaDataSource( file( "neo" ), file( "nioneo_logical.log" ),
//            lockManager, lockReleaser );
        xaCon = (NeoStoreXaConnection) ds.getXaConnection();
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
        xaCon.getNodeConsumer().createNode( node1 );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        xaCon.clearAllTransactions();
        Pair<Pair<File, File>, Pair<File, File>> copy = copyLogicalLog( path() );
        xaCon.clearAllTransactions();
        ds.close();
        deleteLogicalLogIfExist();
        renameCopiedLogicalLog( copy );
        truncateLogicalLog( 40 );
        ds = newNeoStore();
//        ds = new NeoStoreXaDataSource( file( "neo" ), file( "nioneo_logical.log" ),
//            lockManager, lockReleaser );
        xaCon = (NeoStoreXaConnection) ds.getXaConnection();
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
        xaCon.getNodeConsumer().createNode( node1 );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        xaCon.clearAllTransactions();
        Pair<Pair<File, File>, Pair<File, File>> copy = copyLogicalLog( path() );
        xaCon.clearAllTransactions();
        ds.close();
        deleteLogicalLogIfExist();
        renameCopiedLogicalLog( copy );
        truncateLogicalLog( 37 );
        ds = newNeoStore();
//        ds = new NeoStoreXaDataSource( file( "neo" ), file( "nioneo_logical.log" ),
//            lockManager, lockReleaser );
        xaCon = (NeoStoreXaConnection) ds.getXaConnection();
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
        xaCon.getNodeConsumer().createNode( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node2 );
        long n1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getNodeConsumer().addProperty( node1, n1prop1,
            index( "prop1" ), "string value 1" );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        Pair<Pair<File, File>, Pair<File, File>> copy = copyLogicalLog( path() );
        xaCon.clearAllTransactions();
        ds.close();
        deleteLogicalLogIfExist();
        renameCopiedLogicalLog( copy );
        truncateLogicalLog( 188 ); // master (w/ shortstring) says 155
        ds = newNeoStore();
//        ds = new NeoStoreXaDataSource( file( "neo" ), file( "nioneo_logical.log" ),
//            lockManager, lockReleaser );
        xaCon = (NeoStoreXaConnection) ds.getXaConnection();
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
        xaCon.getNodeConsumer().createNode( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node2 );
        long n1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getNodeConsumer().addProperty( node1, n1prop1,
            index( "prop1" ), "string value 1" );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.prepare( xid );
        xaRes.commit( xid, false );
        Pair<Pair<File, File>, Pair<File, File>> copy = copyLogicalLog( path() );
        ds.close();
        deleteLogicalLogIfExist();
        renameCopiedLogicalLog( copy );
        truncateLogicalLog( 224 ); // master (w/ shortstring) says 171
        ds = newNeoStore();
//        ds = new NeoStoreXaDataSource( file( "neo" ), file( "nioneo_logical.log" ),
//             lockManager, lockReleaser );
        xaCon = (NeoStoreXaConnection) ds.getXaConnection();
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
        ds.keepLogicalLogs( true );
        Xid xid = new XidImpl( new byte[1], new byte[1] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node2 );
        long n1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getNodeConsumer().addProperty( node1, n1prop1,
            index( "prop1" ), "string1" );
        xaCon.getNodeConsumer().getProperties( node1, false );
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getRelationshipTypeConsumer().addRelationshipType( relType1,
            "relationshiptype1" );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getRelationshipConsumer().createRelationship( rel1, node1,
            node2, relType1 );
        long r1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getRelationshipConsumer().addProperty( rel1, r1prop1,
            index( "prop1" ), "string1" );
        xaCon.getNodeConsumer().changeProperty( node1, n1prop1, "string2" );
        xaCon.getRelationshipConsumer().changeProperty( rel1, r1prop1,
            "string2" );
        xaCon.getNodeConsumer().removeProperty( node1, n1prop1 );
        xaCon.getRelationshipConsumer().removeProperty( rel1, r1prop1 );
        xaCon.getRelationshipConsumer().deleteRelationship( rel1 );
        xaCon.getNodeConsumer().deleteNode( node1 );
        xaCon.getNodeConsumer().deleteNode( node2 );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.commit( xid, true );
        long currentVersion = ds.getCurrentLogVersion();
        ds.rotateLogicalLog();
        assertTrue( ds.getLogicalLog( currentVersion ) != null );
        ds.rotateLogicalLog();
        assertTrue( ds.getLogicalLog( currentVersion ) != null );
        assertTrue( ds.getLogicalLog( currentVersion + 1 ) != null );
    }


    @Test
    public void testApplyLogicalLog() throws Exception
    {
        ds.keepLogicalLogs( true );
        Xid xid = new XidImpl( new byte[1], new byte[1] );
        XAResource xaRes = xaCon.getXaResource();
        xaRes.start( xid, XAResource.TMNOFLAGS );
        long node1 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node1 );
        long node2 = ds.nextId( Node.class );
        xaCon.getNodeConsumer().createNode( node2 );
        long n1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getNodeConsumer().addProperty( node1, n1prop1,
            index( "prop1" ), "string1" );
        xaCon.getNodeConsumer().getProperties( node1, false );
        int relType1 = (int) ds.nextId( RelationshipType.class );
        xaCon.getRelationshipTypeConsumer().addRelationshipType( relType1,
            "relationshiptype1" );
        long rel1 = ds.nextId( Relationship.class );
        xaCon.getRelationshipConsumer().createRelationship( rel1, node1,
            node2, relType1 );
        long r1prop1 = ds.nextId( PropertyStore.class );
        xaCon.getRelationshipConsumer().addProperty( rel1, r1prop1,
            index( "prop1" ), "string1" );
        xaCon.getNodeConsumer().changeProperty( node1, n1prop1, "string2" );
        xaCon.getRelationshipConsumer().changeProperty( rel1, r1prop1,
            "string2" );
        xaCon.getNodeConsumer().removeProperty( node1, n1prop1 );
        xaCon.getRelationshipConsumer().removeProperty( rel1, r1prop1 );
        xaCon.getRelationshipConsumer().deleteRelationship( rel1 );
        xaCon.getNodeConsumer().deleteNode( node1 );
        xaCon.getNodeConsumer().deleteNode( node2 );
        xaRes.end( xid, XAResource.TMSUCCESS );
        xaRes.commit( xid, true );
        long currentVersion = ds.getCurrentLogVersion();
        ds.keepLogicalLogs( true );
        ds.rotateLogicalLog();
        ds.rotateLogicalLog();
        ds.rotateLogicalLog();
        ds.setCurrentLogVersion( currentVersion );
        ds.setLastCommittedTxId( 0 );
        ds.makeBackupSlave();
        ds.applyLog( ds.getLogicalLog( currentVersion ) );
        ds.applyLog( ds.getLogicalLog( currentVersion + 1 ) );
        ds.applyLog( ds.getLogicalLog( currentVersion + 2 ) );
        ds.keepLogicalLogs( false );
    }
}
