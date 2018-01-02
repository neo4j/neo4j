/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.Future;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.graphdb.Neo4jMatchers.getPropertyKeys;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;

public class TestGraphProperties
{
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private TestGraphDatabaseFactory factory;

    @Before
    public void before() throws Exception
    {
        factory = new TestGraphDatabaseFactory().setFileSystem( fs.get() );
    }

    @Test
    public void basicProperties() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        PropertyContainer graphProperties = properties( db );
        assertThat( graphProperties, inTx( db, not( hasProperty( "test" ) ) ) );

        Transaction tx = db.beginTx();
        graphProperties.setProperty( "test", "yo" );
        assertEquals( "yo", graphProperties.getProperty( "test" ) );
        tx.success();
        tx.close();
        assertThat( graphProperties, inTx( db, hasProperty( "test" ).withValue( "yo" ) ) );
        tx = db.beginTx();
        assertNull( graphProperties.removeProperty( "something non existent" ) );
        assertEquals( "yo", graphProperties.removeProperty( "test" ) );
        assertNull( graphProperties.getProperty( "test", null ) );
        graphProperties.setProperty( "other", 10 );
        assertEquals( 10, graphProperties.getProperty( "other" ) );
        graphProperties.setProperty( "new", "third" );
        tx.success();
        tx.close();
        assertThat( graphProperties, inTx( db, not( hasProperty( "test" ) ) ) );
        assertThat( graphProperties, inTx( db, hasProperty( "other" ).withValue( 10 ) ) );
        assertThat( getPropertyKeys( db, graphProperties ), containsOnly( "other", "new" ) );

        tx = db.beginTx();
        graphProperties.setProperty( "rollback", true );
        assertEquals( true, graphProperties.getProperty( "rollback" ) );
        tx.close();
        assertThat( graphProperties, inTx( db, not( hasProperty( "rollback" ) ) ) );
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
        tx.close();

        for ( int i = 0; i < count; i++ )
        {
            assertThat( properties( db ), inTx( db, hasProperty( "key" + i ).withValue( values[i % values.length] ) ) );
        }
        for ( int i = 0; i < count; i++ )
        {
            assertThat( properties( db ), inTx( db, hasProperty( "key" + i ).withValue( values[i % values.length] ) ) );
        }
        db.shutdown();
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
        assertThat( properties( db ), hasProperty( key ).withValue( array ) );
        tx.success();
        tx.close();

        assertThat( properties( db ), inTx( db, hasProperty( key ).withValue( array ) ) );
        assertThat( properties( db ), inTx( db, hasProperty( key ).withValue( array ) ) );
        db.shutdown();
    }

    private static PropertyContainer properties( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( NodeManager.class ).newGraphProperties();
    }

    @Test
    public void firstRecordOtherThanZeroIfNotFirst() throws Exception
    {
        File storeDir = new File( "/store/dir" ).getAbsoluteFile();
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase( storeDir );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Yo" );
        tx.success();
        tx.close();
        db.shutdown();

        db = (GraphDatabaseAPI) factory.newImpermanentDatabase( storeDir );
        tx = db.beginTx();
        properties( db ).setProperty( "test", "something" );
        tx.success();
        tx.close();
        db.shutdown();

        Config config = new Config( Collections.<String, String>emptyMap(), GraphDatabaseSettings.class );
        StoreFactory storeFactory = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fs.get() ),
                pageCacheRule.getPageCache( fs.get() ), fs.get(), NullLogProvider.getInstance() );
        NeoStores neoStores = storeFactory.openAllNeoStores();
        long prop = neoStores.getMetaDataStore().getGraphNextProp();
        assertTrue( prop != 0 );
        neoStores.close();
    }

    @Test
    public void graphPropertiesAreLockedPerTx() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase();

        Worker worker1 = new Worker( "W1", new State( db ) );
        Worker worker2 = new Worker( "W2", new State( db ) );

        PropertyContainer properties = properties( db );
        worker1.beginTx();
        worker2.beginTx();

        String key1 = "name";
        String value1 = "Value 1";
        String key2 = "some other property";
        String value2 = "Value 2";
        String key3 = "say";
        String value3 = "hello";
        worker1.setProperty( key1, value1 ).get();
        assertThat( properties, inTx( db, not( hasProperty( key1 ) ) ) );
        assertFalse( worker2.hasProperty( key1 ) );
        Future<Void> blockedSetProperty = worker2.setProperty( key2, value2 );
        assertThat( properties, inTx( db, not( hasProperty( key1 ) ) ) );
        assertThat( properties, inTx( db, not( hasProperty( key2 ) ) ) );
        worker1.setProperty( key3, value3 ).get();
        assertFalse( blockedSetProperty.isDone() );
        assertThat( properties, inTx( db, not( hasProperty( key1 ) ) ) );
        assertThat( properties, inTx( db, not( hasProperty( key2 ) ) ) );
        assertThat( properties, inTx( db, not( hasProperty( key3 ) ) ) );
        worker1.commitTx();
        assertThat( properties, inTx( db, hasProperty( key1 ) ) );
        assertThat( properties, inTx( db, not( hasProperty( key2 ) ) ) );
        assertThat( properties, inTx( db, hasProperty( key3 ) ) );
        blockedSetProperty.get();
        assertTrue( blockedSetProperty.isDone() );
        worker2.commitTx();
        assertThat( properties, inTx( db, hasProperty( key1 ).withValue( value1 ) ) );
        assertThat( properties, inTx( db, hasProperty( key2 ).withValue( value2 ) ) );
        assertThat( properties, inTx( db, hasProperty( key3 ).withValue( value3 ) ) );

        worker1.close();
        worker2.close();
        db.shutdown();
   }

    @Test
    public void twoUncleanInARow() throws Exception
    {
        File storeDir = new File("dir");
        EphemeralFileSystemAbstraction snapshot = produceUncleanStore( fs.get(), storeDir );
        snapshot = produceUncleanStore( snapshot, storeDir );
        snapshot = produceUncleanStore( snapshot, storeDir );

        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( snapshot ).newImpermanentDatabase( storeDir );
        assertThat( properties( db ), inTx( db, hasProperty( "prop" ).withValue( "Some value" ) ) );
        db.shutdown();
    }

    @Test
    public void testEquals()
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        PropertyContainer graphProperties = properties( db );
        try ( Transaction tx = db.beginTx() )
        {
            graphProperties.setProperty( "test", "test" );
            tx.success();
        }

        assertEquals( graphProperties, properties( db ) );
        db.shutdown();
        db = (GraphDatabaseAPI) factory.newImpermanentDatabase();
        assertFalse( graphProperties.equals( properties( db ) ) );
        db.shutdown();
    }

    @Test
    public void shouldBeAbleToCreateLongGraphPropertyChainsAndReadTheCorrectNextPointerFromTheStore()
    {
        GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabase();

        PropertyContainer graphProperties = properties( (GraphDatabaseAPI) database );

        try ( Transaction tx = database.beginTx() )
        {
            graphProperties.setProperty( "a", new String[]{"A", "B", "C", "D", "E"} );
            graphProperties.setProperty( "b", true );
            graphProperties.setProperty( "c", "C" );
            tx.success();
        }

        try ( Transaction tx = database.beginTx() )
        {
            graphProperties.setProperty( "d", new String[]{"A", "F"} );
            graphProperties.setProperty( "e", true );
            graphProperties.setProperty( "f", "F" );
            tx.success();
        }

        try ( Transaction tx = database.beginTx() )
        {
            graphProperties.setProperty( "g", new String[]{"F"} );
            graphProperties.setProperty( "h", false );
            graphProperties.setProperty( "i", "I" );
            tx.success();
        }

        try ( Transaction tx = database.beginTx() )
        {
            assertArrayEquals( new String[]{"A", "B", "C", "D", "E"}, (String[]) graphProperties.getProperty( "a" ) );
            assertTrue( (boolean) graphProperties.getProperty( "b" ) );
            assertEquals( "C", graphProperties.getProperty( "c" ) );

            assertArrayEquals( new String[]{"A", "F"}, (String[]) graphProperties.getProperty( "d" ) );
            assertTrue( (boolean) graphProperties.getProperty( "e" ) );
            assertEquals( "F", graphProperties.getProperty( "f" ) );

            assertArrayEquals( new String[]{"F"}, (String[]) graphProperties.getProperty( "g" ) );
            assertFalse( (boolean) graphProperties.getProperty( "h" ) );
            assertEquals( "I", graphProperties.getProperty( "i" ) );
            tx.success();
        }
    }

    private static class State
    {
        private final GraphDatabaseAPI db;
        private final PropertyContainer properties;
        private Transaction tx;

        State( GraphDatabaseAPI db )
        {
            this.db = db;
            this.properties = properties( db );
        }
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
                    return state.properties.hasProperty( key );
                }
            } );
        }

        public void commitTx() throws Exception
        {
            execute( new WorkerCommand<State, Void>()
            {
                @Override
                public Void doWork( State state )
                {
                    state.tx.success();
                    state.tx.close();
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
            File storeDir )
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fileSystem ).newImpermanentDatabase( storeDir );
        Transaction tx = db.beginTx();
        Node node = db.createNode();
        node.setProperty( "name", "Something" );
        properties( (GraphDatabaseAPI) db ).setProperty( "prop", "Some value" );
        tx.success();
        tx.close();
        EphemeralFileSystemAbstraction snapshot = fileSystem.snapshot();
        db.shutdown();
        return snapshot;
    }

    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule();
}
