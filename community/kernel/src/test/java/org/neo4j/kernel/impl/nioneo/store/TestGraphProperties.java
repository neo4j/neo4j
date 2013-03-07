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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.arrayAsCollection;
import static org.neo4j.test.TargetDirectory.forTest;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class TestGraphProperties
{
    private EphemeralFileSystemAbstraction fileSystem;
    private TestGraphDatabaseFactory factory;
    
    @Before
    public void before() throws Exception
    {
        fileSystem = new EphemeralFileSystemAbstraction();
        factory = new TestGraphDatabaseFactory().setFileSystem( fileSystem );
    }

    @After
    public void after() throws Exception
    {
        fileSystem.shutdown();
    }
    
    @Test
    public void basicProperties() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        assertNull( properties( db ).getProperty( "test", null ) );
        Transaction tx = db.beginTx();
        properties( db ).setProperty( "test", "yo" );
        assertEquals( "yo", properties( db ).getProperty( "test" ) );
        tx.success();
        tx.finish();
        assertEquals( "yo", properties( db ).getProperty( "test" ) );
        tx = db.beginTx();
        assertNull( properties( db ).removeProperty( "something non existent" ) );
        assertEquals( "yo", properties( db ).removeProperty( "test" ) );
        assertNull( properties( db ).getProperty( "test", null ) );
        properties( db ).setProperty( "other", 10 );
        assertEquals( 10, properties( db ).getProperty( "other" ) );
        properties( db ).setProperty( "new", "third" );
        tx.success();
        tx.finish();
        assertNull( properties( db ).getProperty( "test", null ) );
        assertEquals( 10, properties( db ).getProperty( "other" ) );
        assertEquals( asSet( asCollection( properties( db ).getPropertyKeys() ) ), asSet( asList( "other", "new" ) ) );

        tx = db.beginTx();
        properties( db ).setProperty( "rollback", true );
        assertEquals( true, properties( db ).getProperty( "rollback" ) );
        tx.finish();
        assertNull( properties( db ).getProperty( "rollback", null ) );
        db.shutdown();
    }

    @Test
    public void setManyGraphProperties() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        
        Transaction tx = db.beginTx();
        Object[] values = new Object[]{10, "A string value", new float[]{1234.567F, 7654.321F},
                "A rather longer string which wouldn't fit inlined #!)(&¤"};
        int count = 200;
        for ( int i = 0; i < count; i++ )
        {
            properties( db ).setProperty( "key" + i, values[i % values.length] );
        }
        tx.success();
        tx.finish();

        for ( int i = 0; i < count; i++ )
        {
            assertPropertyEquals( values[i % values.length], properties( db ).getProperty( "key" + i ) );
        }
        db.getNodeManager().clearCache();
        for ( int i = 0; i < count; i++ )
        {
            assertPropertyEquals( values[i % values.length], properties( db ).getProperty( "key" + i ) );
        }
        db.shutdown();
    }

    private void assertPropertyEquals( Object expected, Object readValue )
    {
        if ( expected.getClass().isArray() )
        {
            assertEquals( arrayAsCollection( expected ), arrayAsCollection( readValue ) );
        }
        else
        {
            assertEquals( expected, readValue );
        }
    }

    @Test
    public void setBigArrayGraphProperty() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        long[] array = new long[1000];
        for ( int i = 0; i < 10; i++ )
        {
            array[array.length / 10 * i] = i;
        }
        String key = "big long array";
        Transaction tx = db.beginTx();
        properties( db ).setProperty( key, array );
        assertPropertyEquals( array, properties( db ).getProperty( key ) );
        tx.success();
        tx.finish();
        assertPropertyEquals( array, properties( db ).getProperty( key ) );
        db.getNodeManager().clearCache();
        assertPropertyEquals( array, properties( db ).getProperty( key ) );
        db.shutdown();
    }

    private PropertyContainer properties( GraphDatabaseAPI db )
    {
        return db.getNodeManager().getGraphProperties();
    }

    @Test
    public void firstRecordOtherThanZeroIfNotFirst() throws Exception
    {
        String storeDir = forTest( getClass()).directory( "zero", true ).getAbsolutePath();
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase( storeDir );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Yo" );
        tx.success();
        tx.finish();
        db.shutdown();

        db = (GraphDatabaseAPI) factory.newImpermanentDatabase( storeDir );
        tx = db.beginTx();
        db.getNodeManager().getGraphProperties().setProperty( "test", "something" );
        tx.success();
        tx.finish();
        db.shutdown();

        NeoStore neoStore = new StoreFactory( new Config( Collections.<String, String>emptyMap(),
                GraphDatabaseSettings.class ),
                new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(), fileSystem, StringLogger.DEV_NULL,
                null ).newNeoStore( new File( storeDir, NeoStore.DEFAULT_NAME ) );
        long prop = neoStore.getGraphNextProp();
        assertTrue( prop != 0 );
        neoStore.close();
    }

    @Test
    public void graphPropertiesAreLockedPerTx() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        
        Worker worker1 = new Worker( "W1", new State( db ) );
        Worker worker2 = new Worker( "W2", new State( db ) );

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
        
        db.shutdown();
    }

    @Test
    public void upgradeDoesntAccidentallyAssignPropertyChainZero() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory().setFileSystem( fileSystem );
        String storeDir = new File( "test" ).getAbsolutePath();
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase( storeDir );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Something" );
        tx.success();
        tx.finish();
        db.shutdown();

        removeLastNeoStoreRecord( fileSystem, storeDir );

        db = (GraphDatabaseAPI) factory.newImpermanentDatabase( storeDir );
        PropertyContainer properties = db.getNodeManager().getGraphProperties();
        assertFalse( properties.getPropertyKeys().iterator().hasNext() );
        tx = db.beginTx();
        properties.setProperty( "a property", "a value" );
        tx.success();
        tx.finish();
        db.getNodeManager().clearCache();
        assertEquals( "a value", properties.getProperty( "a property" ) );
        db.shutdown();

        db = (GraphDatabaseAPI) factory.newImpermanentDatabase( storeDir );
        properties = db.getNodeManager().getGraphProperties();
        assertEquals( "a value", properties.getProperty( "a property" ) );
        db.shutdown();
    }

    private void removeLastNeoStoreRecord( FileSystemAbstraction fileSystem, String storeDir ) throws IOException
    {
        // Remove the last record, next startup will look like as if we're upgrading an old store
        File neoStoreFile = new File( storeDir, NeoStore.DEFAULT_NAME );
        FileChannel channel = fileSystem.open( neoStoreFile, "rw" );
        channel.position( NeoStore.RECORD_SIZE * 6/*position of "next prop"*/ );
        int trail = (int) (channel.size() - channel.position());
        ByteBuffer trailBuffer = null;
        if ( trail > 0 )
        {
            trailBuffer = ByteBuffer.allocate( trail );
            channel.read( trailBuffer );
            trailBuffer.flip();
        }
        channel.position( NeoStore.RECORD_SIZE * 5 );
        if ( trail > 0 )
        {
            channel.write( trailBuffer );
        }
        channel.truncate( channel.position() );
    }

    @Ignore("TODO: Why should the assertFalse work??")
    @Test
    public void upgradeWorksEvenOnUncleanShutdown() throws Exception
    {
        EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
        String storeDir = TargetDirectory.forTest( TestGraphProperties.class ).directory( "nonclean",
                true ).getAbsolutePath();

        Process process = Runtime.getRuntime().exec( new String[]{
                "java", "-cp", System.getProperty( "java.class.path" ),
                ProduceUncleanStore.class.getName(), storeDir, "true"
        } );

        assertEquals( 0, new ProcessStreamHandler( process, true ).waitForResult() );

        removeLastNeoStoreRecord( fileSystem, storeDir );
        GraphDatabaseAPI db = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
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
        String storeDir = "dir";
        EphemeralFileSystemAbstraction snapshot = produceUncleanStore( fileSystem, storeDir );
        snapshot = produceUncleanStore( snapshot, storeDir );
        snapshot = produceUncleanStore( snapshot, storeDir );
        
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( snapshot ).newImpermanentDatabase( storeDir );
        assertEquals( "Some value", db.getNodeManager().getGraphProperties().getProperty( "prop" ) );
        db.shutdown();
    }

    @Test
    public void testEquals()
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        GraphProperties graphProperties = db.getNodeManager().getGraphProperties();
        Transaction tx = db.beginTx();
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
        db.shutdown();
        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
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
        public Worker( String name, State initialState )
        {
            super( name, initialState );
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
    
    private EphemeralFileSystemAbstraction produceUncleanStore( EphemeralFileSystemAbstraction fileSystem,
            String storeDir )
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Something" );
        ((GraphDatabaseAPI)db).getNodeManager().getGraphProperties().setProperty( "prop", "Some value" );
        tx.success();
        tx.finish();
        EphemeralFileSystemAbstraction snapshot = fileSystem.snapshot();
        db.shutdown();
        return snapshot;
    }
}
