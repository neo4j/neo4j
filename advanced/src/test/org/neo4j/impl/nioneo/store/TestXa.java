/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.nioneo.store;

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

import junit.framework.Test;
import junit.framework.TestSuite;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.AbstractNeoTestCase;
import org.neo4j.impl.core.LockReleaser;
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.xa.NeoStoreXaConnection;
import org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.XidImpl;

public class TestXa extends AbstractNeoTestCase
{

    public TestXa( String testName )
    {
        super( testName );
    }

    public static void main( java.lang.String[] args )
    {
        junit.textui.TestRunner.run( suite() );
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite( TestXa.class );
        return suite;
    }

    private NeoStoreXaDataSource ds;
    private NeoStoreXaConnection xaCon;
    private Logger log;
    private Level level;

    private static class MyPropertyIndex extends
        org.neo4j.impl.core.PropertyIndex
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
            return Collections.EMPTY_LIST;
        }

        public static PropertyIndex getIndexFor( int index )
        {
            return intToIndex.get( index );
        }

        public static void add( MyPropertyIndex index )
        {
            // TODO Auto-generated method stub
            stringToIndex.put( index.getKey(), index );
            intToIndex.put( index.getKeyId(), index );
        }
    }

    private PropertyIndex createDummyIndex( int id, String key )
    {
        MyPropertyIndex index = new MyPropertyIndex( key, id );
        MyPropertyIndex.add( index );
        return index;
    }

    private LockManager lockManager;
    private LockReleaser lockReleaser;

    public void setUp()
    {
        super.setUp();
        log = Logger
            .getLogger( "org.neo4j.impl.transaction.xaframework.XaLogicalLog/"
                + "nioneo_logical.log" );
        level = log.getLevel();
        log.setLevel( Level.OFF );
        log = Logger
            .getLogger( "org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource" );
        log.setLevel( Level.OFF );
        try
        {
            NeoStore.createStore( "neo" );
            lockManager = getEmbeddedNeo().getConfig().getLockManager();
            lockReleaser = getEmbeddedNeo().getConfig().getLockReleaser();
            ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log",
                lockManager, lockReleaser );
            xaCon = (NeoStoreXaConnection) ds.getXaConnection();
        }
        catch ( Exception e )
        {
            fail( "" + e );
        }
    }

    public void tearDown()
    {
        super.tearDown();
        ds.close();
        log.setLevel( level );
        log = Logger
            .getLogger( "org.neo4j.impl.transaction.xaframework.XaLogicalLog/"
                + "nioneo_logical.log" );
        log.setLevel( level );
        log = Logger
            .getLogger( "org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource" );
        log.setLevel( level );
        File file = new File( "neo" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.id" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.nodestore.db" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.nodestore.db.id" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.propertystore.db" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.propertystore.db.id" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.propertystore.db.index" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.propertystore.db.index.id" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.propertystore.db.index.keys" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.propertystore.db.index.keys.id" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.propertystore.db.strings" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.propertystore.db.strings.id" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.propertystore.db.arrays" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.propertystore.db.arrays.id" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.relationshipstore.db" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.relationshipstore.db.id" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.relationshiptypestore.db" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.relationshiptypestore.db.id" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.relationshiptypestore.db.names" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "neo.relationshiptypestore.db.names.id" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "." );
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
        File file = new File( "nioneo_logical.log.1" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "nioneo_logical.log.2" );
        if ( file.exists() )
        {
            assertTrue( file.delete() );
        }
        file = new File( "nioneo_logical.log.active" );
        assertTrue( file.delete() );
    }

    private void renameCopiedLogicalLog() throws IOException
    {
        File file = new File( "nioneo_logical.log.bak.1" );
        if ( file.exists() )
        {
            assertTrue( file.renameTo( new File( "nioneo_logical.log.1" ) ) );
        }
        else
        {
            file = new File( "nioneo_logical.log.bak.2" );
            assertTrue( file.renameTo( new File( "nioneo_logical.log.2" ) ) );
        }
        file = new File( "nioneo_logical.log.bak.active" );
        assertTrue( file.renameTo( new File( "nioneo_logical.log.active" ) ) );
    }

    private void truncateLogicalLog( int size ) throws IOException
    {
        char active = '1';
        FileChannel af = new RandomAccessFile( "nioneo_logical.log.active", 
            "r" ).getChannel();
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        af.read( buffer );
        af.close();
        buffer.flip();
        active = buffer.asCharBuffer().get();
        buffer.clear();
        FileChannel fileChannel = new RandomAccessFile( "nioneo_logical.log." + 
            active, "rw" ).getChannel();
        if ( size < 0 )
        {
            fileChannel.truncate( fileChannel.size() - 3 );
        }
        else
        {
            fileChannel.truncate( size );
        }
        fileChannel.force( false );
        fileChannel.close();
    }
    
    private void copyLogicalLog() throws IOException
    {
        char active = '1';
        FileChannel af = new RandomAccessFile( "nioneo_logical.log.active", 
            "r" ).getChannel();
        ByteBuffer buffer = ByteBuffer.allocate( 1024 );
        af.read( buffer );
        buffer.flip();
        FileChannel activeCopy = new RandomAccessFile( 
            "nioneo_logical.log.bak.active", "rw" ).getChannel();
        activeCopy.write( buffer );
        activeCopy.close();
        af.close();
        buffer.flip();
        active = buffer.asCharBuffer().get();
        buffer.clear();
        FileChannel source = new RandomAccessFile( "nioneo_logical.log." + 
            active, "r" ).getChannel();
        FileChannel dest = new RandomAccessFile( "nioneo_logical.log.bak." + 
            active, "rw" ).getChannel();
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
    }

    private PropertyIndex index( String key ) throws IOException
    {
        Iterator<PropertyIndex> itr = MyPropertyIndex.index( key ).iterator();
        if ( !itr.hasNext() )
        {
            int id = ds.nextId( PropertyIndex.class );
            PropertyIndex index = createDummyIndex( id, key );
            xaCon.getPropertyIndexConsumer().createPropertyIndex( id, key );
            return index;
        }
        return itr.next();
    }

    public void testLogicalLog()
    {
        try
        {
            Xid xid = new XidImpl( new byte[1], new byte[1] );
            XAResource xaRes = xaCon.getXaResource();
            xaRes.start( xid, XAResource.TMNOFLAGS );
            int node1 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node1 );
            int node2 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node2 );
            int n1prop1 = ds.nextId( PropertyStore.class );
            xaCon.getNodeConsumer().addProperty( node1, n1prop1,
                index( "prop1" ), "string1" );
            xaCon.getNodeConsumer().getProperties( node1 );
            int relType1 = ds.nextId( RelationshipType.class );
            xaCon.getRelationshipTypeConsumer().addRelationshipType( relType1,
                "relationshiptype1" );
            int rel1 = ds.nextId( Relationship.class );
            xaCon.getRelationshipConsumer().createRelationship( rel1, node1,
                node2, relType1 );
            int r1prop1 = ds.nextId( PropertyStore.class );
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
            copyLogicalLog();
            xaCon.clearAllTransactions();
            ds.close();
            deleteLogicalLogIfExist();
            renameCopiedLogicalLog();
            ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log",
                lockManager, lockReleaser );
            xaCon = (NeoStoreXaConnection) ds.getXaConnection();
            xaRes = xaCon.getXaResource();
            assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
            xaCon.clearAllTransactions();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
    }

    public void testLogicalLogPrepared()
    {
        try
        {
            Xid xid = new XidImpl( new byte[2], new byte[2] );
            XAResource xaRes = xaCon.getXaResource();
            xaRes.start( xid, XAResource.TMNOFLAGS );
            int node1 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node1 );
            int node2 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node2 );
            int n1prop1 = ds.nextId( PropertyStore.class );
            xaCon.getNodeConsumer().addProperty( node1, n1prop1,
                index( "prop1" ), "string1" );
            int relType1 = ds.nextId( RelationshipType.class );
            xaCon.getRelationshipTypeConsumer().addRelationshipType( relType1,
                "relationshiptype1" );
            int rel1 = ds.nextId( Relationship.class );
            xaCon.getRelationshipConsumer().createRelationship( rel1, node1,
                node2, relType1 );
            int r1prop1 = ds.nextId( PropertyStore.class );
            xaCon.getRelationshipConsumer().addProperty( rel1, r1prop1,
                index( "prop1" ), "string1" );
            xaCon.getNodeConsumer().changeProperty( node1, n1prop1, "string2" );
            xaCon.getRelationshipConsumer().changeProperty( rel1, r1prop1,
                "string2" );
            xaRes.end( xid, XAResource.TMSUCCESS );
            xaRes.prepare( xid );
            copyLogicalLog();
            xaCon.clearAllTransactions();
            ds.close();
            deleteLogicalLogIfExist();
            renameCopiedLogicalLog();
            ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log",
                lockManager, lockReleaser );
            xaCon = (NeoStoreXaConnection) ds.getXaConnection();
            xaRes = xaCon.getXaResource();
            assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
            xaRes.commit( xid, true );
            xaCon.clearAllTransactions();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
    }

    public void testLogicalLogPrePrepared()
    {
        try
        {
            Xid xid = new XidImpl( new byte[3], new byte[3] );
            XAResource xaRes = xaCon.getXaResource();
            xaRes.start( xid, XAResource.TMNOFLAGS );
            int node1 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node1 );
            int node2 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node2 );
            int n1prop1 = ds.nextId( PropertyStore.class );
            xaCon.getNodeConsumer().addProperty( node1, n1prop1,
                index( "prop1" ), "string1" );
            int relType1 = ds.nextId( RelationshipType.class );
            xaCon.getRelationshipTypeConsumer().addRelationshipType( relType1,
                "relationshiptype1" );
            int rel1 = ds.nextId( Relationship.class );
            xaCon.getRelationshipConsumer().createRelationship( rel1, node1,
                node2, relType1 );
            int r1prop1 = ds.nextId( PropertyStore.class );
            xaCon.getRelationshipConsumer().addProperty( rel1, r1prop1,
                index( "prop1" ), "string1" );
            xaCon.getNodeConsumer().changeProperty( node1, n1prop1, "string2" );
            xaCon.getRelationshipConsumer().changeProperty( rel1, r1prop1,
                "string2" );
            xaRes.end( xid, XAResource.TMSUCCESS );
            xaCon.clearAllTransactions();
            copyLogicalLog();
            ds.close();
            deleteLogicalLogIfExist();
            renameCopiedLogicalLog();
            ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log",
                lockManager, lockReleaser );
            xaCon = (NeoStoreXaConnection) ds.getXaConnection();
            xaRes = xaCon.getXaResource();
            assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
    }

    public void testBrokenCommand()
    {
        try
        {
            Xid xid = new XidImpl( new byte[4], new byte[4] );
            XAResource xaRes = xaCon.getXaResource();
            xaRes.start( xid, XAResource.TMNOFLAGS );
            int node1 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node1 );
            int node2 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node2 );
            int n1prop1 = ds.nextId( PropertyStore.class );
            xaCon.getNodeConsumer().addProperty( node1, n1prop1,
                index( "prop1" ), "string1" );
            copyLogicalLog();
            xaCon.clearAllTransactions();
            ds.close();
            deleteLogicalLogIfExist();
            renameCopiedLogicalLog();
            truncateLogicalLog( -3 );
            ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log",
                lockManager, lockReleaser );
            xaCon = (NeoStoreXaConnection) ds.getXaConnection();
            xaRes = xaCon.getXaResource();
            assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
            xaCon.clearAllTransactions();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
    }

    public void testBrokenPrepare()
    {
        try
        {
            Xid xid = new XidImpl( new byte[4], new byte[4] );
            XAResource xaRes = xaCon.getXaResource();
            xaRes.start( xid, XAResource.TMNOFLAGS );
            int node1 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node1 );
            int node2 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node2 );
            int n1prop1 = ds.nextId( PropertyStore.class );
            xaCon.getNodeConsumer().addProperty( node1, n1prop1,
                index( "prop1" ), "string1" );
            xaRes.end( xid, XAResource.TMSUCCESS );
            xaRes.prepare( xid );
            copyLogicalLog();
            xaCon.clearAllTransactions();
            ds.close();
            deleteLogicalLogIfExist();
            renameCopiedLogicalLog();
            truncateLogicalLog( 141 );
            ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log",
                lockManager, lockReleaser );
            xaCon = (NeoStoreXaConnection) ds.getXaConnection();
            xaRes = xaCon.getXaResource();
            assertEquals( 0, xaRes.recover( XAResource.TMNOFLAGS ).length );
            xaCon.clearAllTransactions();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
    }

    public void testBrokenDone()
    {
        try
        {
            Xid xid = new XidImpl( new byte[4], new byte[4] );
            XAResource xaRes = xaCon.getXaResource();
            xaRes.start( xid, XAResource.TMNOFLAGS );
            int node1 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node1 );
            int node2 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node2 );
            int n1prop1 = ds.nextId( PropertyStore.class );
            xaCon.getNodeConsumer().addProperty( node1, n1prop1,
                index( "prop1" ), "string1" );
            xaRes.end( xid, XAResource.TMSUCCESS );
            xaRes.prepare( xid );
            xaRes.commit( xid, false );
            copyLogicalLog();
            ds.close();
            deleteLogicalLogIfExist();
            renameCopiedLogicalLog();
            truncateLogicalLog( 145 );
            ds = new NeoStoreXaDataSource( "neo", "nioneo_logical.log",
                lockManager, lockReleaser );
            xaCon = (NeoStoreXaConnection) ds.getXaConnection();
            xaRes = xaCon.getXaResource();
            assertEquals( 1, xaRes.recover( XAResource.TMNOFLAGS ).length );
            xaCon.clearAllTransactions();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
    }
    
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

    public void testLogicalLogRotation()
    {
        try
        {
            ds.keepLogicalLogs( true );
            Xid xid = new XidImpl( new byte[1], new byte[1] );
            XAResource xaRes = xaCon.getXaResource();
            xaRes.start( xid, XAResource.TMNOFLAGS );
            int node1 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node1 );
            int node2 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node2 );
            int n1prop1 = ds.nextId( PropertyStore.class );
            xaCon.getNodeConsumer().addProperty( node1, n1prop1,
                index( "prop1" ), "string1" );
            xaCon.getNodeConsumer().getProperties( node1 );
            int relType1 = ds.nextId( RelationshipType.class );
            xaCon.getRelationshipTypeConsumer().addRelationshipType( relType1,
                "relationshiptype1" );
            int rel1 = ds.nextId( Relationship.class );
            xaCon.getRelationshipConsumer().createRelationship( rel1, node1,
                node2, relType1 );
            int r1prop1 = ds.nextId( PropertyStore.class );
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
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
    }


    public void testApplyLogicalLog()
    {
        try
        {
            ds.keepLogicalLogs( true );
            Xid xid = new XidImpl( new byte[1], new byte[1] );
            XAResource xaRes = xaCon.getXaResource();
            xaRes.start( xid, XAResource.TMNOFLAGS );
            int node1 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node1 );
            int node2 = ds.nextId( Node.class );
            xaCon.getNodeConsumer().createNode( node2 );
            int n1prop1 = ds.nextId( PropertyStore.class );
            xaCon.getNodeConsumer().addProperty( node1, n1prop1,
                index( "prop1" ), "string1" );
            xaCon.getNodeConsumer().getProperties( node1 );
            int relType1 = ds.nextId( RelationshipType.class );
            xaCon.getRelationshipTypeConsumer().addRelationshipType( relType1,
                "relationshiptype1" );
            int rel1 = ds.nextId( Relationship.class );
            xaCon.getRelationshipConsumer().createRelationship( rel1, node1,
                node2, relType1 );
            int r1prop1 = ds.nextId( PropertyStore.class );
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
            ds.makeBackupSlave();
            ds.applyLog( ds.getLogicalLog( currentVersion ) );
            ds.applyLog( ds.getLogicalLog( currentVersion + 1 ) );
            ds.applyLog( ds.getLogicalLog( currentVersion + 2 ) );
            ds.keepLogicalLogs( false );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
    }
}