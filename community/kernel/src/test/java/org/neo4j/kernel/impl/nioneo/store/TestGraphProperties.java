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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Arrays.*;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.IteratorUtil.*;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.*;
import static org.neo4j.test.TargetDirectory.*;

public class TestGraphProperties
{
    private GraphDatabaseAPI db;
    private Transaction tx;

    @Before
    public void doBefore() throws Exception
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
    }
    
    @After
    public void doAfter() throws Exception
    {
        db.shutdown();
    }
    
    private void restartDb()
    {
        db.shutdown();
        db = new ImpermanentGraphDatabase();
    }
    
    @Test
    public void basicProperties() throws Exception
    {
        assertNull( properties().getProperty( "test", null ) );
        beginTx();
        properties().setProperty( "test", "yo" );
        assertEquals( "yo", properties().getProperty( "test" ) );
        finishTx( true );
        assertEquals( "yo", properties().getProperty( "test" ) );
        beginTx();
        assertNull( properties().removeProperty( "something non existent" ) );
        assertEquals( "yo", properties().removeProperty( "test" ) );
        assertNull( properties().getProperty( "test", null ) );
        properties().setProperty( "other", 10 );
        assertEquals( 10, properties().getProperty( "other" ) );
        properties().setProperty( "new", "third" );
        finishTx( true );
        assertNull( properties().getProperty( "test", null ) );
        assertEquals( 10, properties().getProperty( "other" ) );
        assertEquals( asSet( asCollection( properties().getPropertyKeys() ) ), asSet( asList( "other", "new" ) ) );
        
        beginTx();
        properties().setProperty( "rollback", true );
        assertEquals( true, properties().getProperty( "rollback" ) );
        finishTx( false );
        assertNull( properties().getProperty( "rollback", null ) );
    }
    
    @Test
    public void setManyGraphProperties() throws Exception
    {
        beginTx();
        Object[] values = new Object[] { 10, "A string value", new float[] { 1234.567F, 7654.321F},
                "A rather longer string which wouldn't fit inlined #!)(&¤" };
        int count = 200;
        for ( int i = 0; i < count; i++ ) properties().setProperty( "key" + i, values[i%values.length] );
        finishTx( true );
        
        for ( int i = 0; i < count; i++ ) assertPropertyEquals( values[i%values.length], properties().getProperty( "key" + i ) ); 
        db.getNodeManager().clearCache();
        for ( int i = 0; i < count; i++ ) assertPropertyEquals( values[i%values.length], properties().getProperty( "key" + i ) ); 
    }
    
    private void assertPropertyEquals( Object expected, Object readValue )
    {
        if ( expected.getClass().isArray() ) assertEquals( arrayAsCollection( expected ), arrayAsCollection( readValue ) );
        else assertEquals( expected, readValue );
    }

    @Test
    public void setBigArrayGraphProperty() throws Exception
    {
        long[] array = new long[1000];
        for ( int i = 0; i < 10; i++ ) array[array.length/10*i] = i;
        String key = "big long array";
        beginTx();
        properties().setProperty( key, array );
        assertPropertyEquals( array, properties().getProperty( key ) );
        finishTx( true );
        assertPropertyEquals( array, properties().getProperty( key ) );
        db.getNodeManager().clearCache();
        assertPropertyEquals( array, properties().getProperty( key ) );
    }

    private <T> Set<T> asSet( Collection<T> asCollection )
    {
        Set<T> set = new HashSet<T>();
        set.addAll( asCollection );
        return set;
    }

    private void finishTx( boolean success )
    {
        if ( tx == null ) throw new IllegalStateException( "Transaction not started" );
        if ( success ) tx.success();
        tx.finish();
        tx = null;
    }
    
    private void beginTx()
    {
        if ( tx != null ) throw new IllegalStateException( "Transaction already started" );
        tx = db.beginTx();
    }

    private PropertyContainer properties()
    {
        return db.getNodeManager().getGraphProperties();
    }
    
    @Test
    public void firstRecordOtherThanZeroIfNotFirst() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( forTest( getClass() ).directory( "zero", true ).getAbsolutePath() ).newGraphDatabase();
        String storeDir = db.getStoreDir();
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Yo" );
        tx.success();
        tx.finish();
        db.shutdown();
        
        db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( forTest( getClass() ).directory( "zero", false ).getAbsolutePath() ).newGraphDatabase();
        tx = db.beginTx();
        db.getNodeManager().getGraphProperties().setProperty( "test", "something" );
        tx.success();
        tx.finish();
        db.shutdown();

        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        NeoStore neoStore = new StoreFactory(new Config( StringLogger.DEV_NULL, fileSystem, Collections.<String,String>emptyMap()), new DefaultIdGeneratorFactory(), fileSystem, null, StringLogger.DEV_NULL, null).newNeoStore(new File( storeDir, NeoStore.DEFAULT_NAME ).getAbsolutePath());
        long prop = neoStore.getGraphNextProp();
        assertTrue( prop != 0 );
        neoStore.close();
    }
    
    @Test
    public void graphPropertiesAreLockedPerTx() throws Exception
    {
        Worker worker1 = new Worker( new State( db ) );
        Worker worker2 = new Worker( new State( db ) );
        
        PropertyContainer properties = getGraphProperties( db );
        worker1.beginTx();
        worker2.beginTx();
        
        String key1 = "name";
        String value1 = "Value 1";
        String key2 = "some other property";
        String value2 = "Value 2";
        String key3 = "say";
        String value3 = "hello";
        worker1.setProperty( key1, value1 ).get();
        assertFalse( properties.hasProperty( key1 ) );
        assertFalse( worker2.hasProperty( key1 ) );
        Future<Void> blockedSetProperty = worker2.setProperty( key2, value2 );
        assertFalse( properties.hasProperty( key1 ) );
        assertFalse( properties.hasProperty( key2 ) );
        worker1.setProperty( key3, value3 ).get();
        assertFalse( blockedSetProperty.isDone() );
        assertFalse( properties.hasProperty( key1 ) );
        assertFalse( properties.hasProperty( key2 ) );
        assertFalse( properties.hasProperty( key3 ) );
        worker1.commitTx();
        assertTrue( properties.hasProperty( key1 ) );
        assertFalse( properties.hasProperty( key2 ) );
        assertTrue( properties.hasProperty( key3 ) );
        blockedSetProperty.get();
        assertTrue( blockedSetProperty.isDone() );
        worker2.commitTx();
        assertTrue( properties.hasProperty( key1 ) );
        assertTrue( properties.hasProperty( key2 ) );
        assertTrue( properties.hasProperty( key3 ) );
        
        assertEquals( value1, properties.getProperty( key1 ) );
        assertEquals( value3, properties.getProperty( key3 ) );
        assertEquals( value2, properties.getProperty( key2 ) );
        
        worker1.shutdown();
        worker2.shutdown();
    }
    
    @Test
    public void upgradeDoesntAccidentallyAssignPropertyChainZero() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( TargetDirectory.forTest(
                        TestGraphProperties.class ).directory( "upgrade", true ).getAbsolutePath() ).newGraphDatabase();
        String storeDir = db.getStoreDir();
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Something" );
        tx.success();
        tx.finish();
        db.shutdown();
        
        removeLastNeoStoreRecord( storeDir );
        
        db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).newGraphDatabase();
        PropertyContainer properties = db.getNodeManager().getGraphProperties();
        assertFalse( properties.getPropertyKeys().iterator().hasNext() );
        tx = db.beginTx();
        properties.setProperty( "a property", "a value" );
        tx.success();
        tx.finish();
        db.getNodeManager().clearCache();
        assertEquals( "a value", properties.getProperty( "a property" ) );
        db.shutdown();
        
        db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).newGraphDatabase();
        properties = db.getNodeManager().getGraphProperties();
        assertEquals( "a value", properties.getProperty( "a property" ) );
        db.shutdown();
    }

    private void removeLastNeoStoreRecord( String storeDir ) throws IOException
    {
        // Remove the last record, next startup will look like as if we're upgrading an old store
        File neoStoreFile = new File( storeDir, NeoStore.DEFAULT_NAME );
        RandomAccessFile raFile = new RandomAccessFile( neoStoreFile, "rw" );
        FileChannel channel = raFile.getChannel();
        channel.position( NeoStore.RECORD_SIZE*6/*position of "next prop"*/ );
        int trail = (int) (channel.size()-channel.position());
        ByteBuffer trailBuffer = null;
        if ( trail > 0 )
        {
            trailBuffer = ByteBuffer.allocate( trail );
            channel.read( trailBuffer );
            trailBuffer.flip();
        }
        channel.position( NeoStore.RECORD_SIZE*5 );
        if ( trail > 0 ) channel.write( trailBuffer );
        channel.truncate( channel.position() );
        raFile.close();
    }
    
    @Test
    public void upgradeWorksEvenOnUncleanShutdown() throws Exception
    {
        String storeDir = TargetDirectory.forTest( TestGraphProperties.class ).directory( "nonclean", true ).getAbsolutePath();
        assertEquals( 0, Runtime.getRuntime().exec( new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                ProduceUncleanStore.class.getName(), storeDir } ).waitFor() );
        removeLastNeoStoreRecord( storeDir );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).newGraphDatabase();
        PropertyContainer properties = db.getNodeManager().getGraphProperties();
        assertFalse( properties.getPropertyKeys().iterator().hasNext() ); 
        Transaction tx = db.beginTx();
        properties.setProperty( "a property", "a value" );
        tx.success();
        tx.finish();
        db.getNodeManager().clearCache();
        assertEquals( "a value", properties.getProperty( "a property" ) );
        db.shutdown();
    }
    
    @Test
    public void twoUncleanInARow() throws Exception
    {
        String storeDir = TargetDirectory.forTest( TestGraphProperties.class ).directory( "nonclean", true ).getAbsolutePath();
        assertEquals( 0, Runtime.getRuntime().exec( new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                ProduceUncleanStore.class.getName(), storeDir, "true" } ).waitFor() );
        assertEquals( 0, Runtime.getRuntime().exec( new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                ProduceUncleanStore.class.getName(), storeDir, "true" } ).waitFor() );
        assertEquals( 0, Runtime.getRuntime().exec( new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                ProduceUncleanStore.class.getName(), storeDir, "true" } ).waitFor() );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).newGraphDatabase();
        assertEquals( "Some value", db.getNodeManager().getGraphProperties().getProperty( "prop" ) );
        db.shutdown();
    }
    
    @Test
    public void testEquals()
    {
        GraphProperties graphProperties = db.getNodeManager().getGraphProperties();
        tx = db.beginTx();
        try
        {
            graphProperties.setProperty( "test", "test" );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        
        assertEquals( graphProperties, db.getNodeManager().getGraphProperties() );
        restartDb();
        assertFalse( graphProperties.equals( db.getNodeManager().getGraphProperties() ) );
    }
    
    private static class State
    {
        private final GraphDatabaseAPI db;
        private final PropertyContainer properties;
        private Transaction tx;

        State( GraphDatabaseAPI db )
        {
            this.db = db;
            this.properties = getGraphProperties( db );
        }
    }
    
    private static GraphProperties getGraphProperties( GraphDatabaseAPI db )
    {
        return db.getNodeManager().getGraphProperties();
    }
    
    private static class Worker extends OtherThreadExecutor<State>
    {
        public Worker( State initialState )
        {
            super( initialState );
        }
        
        public boolean hasProperty( final String key ) throws Exception
        {
            return execute( new WorkerCommand<State, Boolean>()
            {
                @Override
                public Boolean doWork( State state )
                {
                    return Boolean.valueOf( state.properties.hasProperty( key ) );
                }
            } ).booleanValue();
        }

        public void commitTx() throws Exception
        {
            execute( new WorkerCommand<State, Void>()
            {
                @Override
                public Void doWork( State state )
                {
                    state.tx.success();
                    state.tx.finish();
                    return null;
                }
            } );
        }

        void beginTx() throws Exception
        {
            execute( new WorkerCommand<State, Void>()
            {
                @Override
                public Void doWork( State state )
                {
                    state.tx = state.db.beginTx();
                    return null;
                }
            } );
        }
        
        Future<Void> setProperty( final String key, final Object value ) throws Exception
        {
            return executeDontWait( new WorkerCommand<State, Void>()
            {
                @Override
                public Void doWork( State state )
                {
                    state.properties.setProperty( key, value );
                    return null;
                }
            } );
        }
    }
}
