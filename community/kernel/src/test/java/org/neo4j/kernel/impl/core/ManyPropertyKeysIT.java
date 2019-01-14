/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.core;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.Collection;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;

/**
 * Tests for handling many property keys (even after restart of database)
 * as well as concurrent creation of property keys.
 */
public class ManyPropertyKeysIT
{
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( testDirectory )
            .around( fileSystemRule ).around( pageCacheRule );

    private File storeDir;

    @Before
    public void setup()
    {
        storeDir  = testDirectory.graphDbDir();
    }

    @Test
    public void creating_many_property_keys_should_have_all_loaded_the_next_restart() throws Exception
    {
        // GIVEN
        // The previous limit to load was 2500, so go some above that
        GraphDatabaseAPI db = databaseWithManyPropertyKeys( 3000 );
        int countBefore = propertyKeyCount( db );

        // WHEN
        db.shutdown();
        db = database();
        createNodeWithProperty( db, key( 2800 ), true );

        // THEN
        assertEquals( countBefore, propertyKeyCount( db ) );
        db.shutdown();
    }

    @Test
    public void concurrently_creating_same_property_key_in_different_transactions_should_end_up_with_same_key_id()
            throws Exception
    {
        // GIVEN
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
        OtherThreadExecutor<WorkerState> worker1 = new OtherThreadExecutor<>( "w1", new WorkerState( db ) );
        OtherThreadExecutor<WorkerState> worker2 = new OtherThreadExecutor<>( "w2", new WorkerState( db ) );
        worker1.execute( new BeginTx() );
        worker2.execute( new BeginTx() );

        // WHEN
        String key = "mykey";
        worker1.execute( new CreateNodeAndSetProperty( key ) );
        worker2.execute( new CreateNodeAndSetProperty( key ) );
        worker1.execute( new FinishTx() );
        worker2.execute( new FinishTx() );
        worker1.close();
        worker2.close();

        // THEN
        assertEquals( 1, propertyKeyCount( db ) );
        db.shutdown();
    }

    private GraphDatabaseAPI database()
    {
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsoluteFile() );
    }

    private GraphDatabaseAPI databaseWithManyPropertyKeys( int propertyKeyCount )
    {

        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
        StoreFactory storeFactory = new StoreFactory(
                storeDir, Config.defaults(), new DefaultIdGeneratorFactory( fileSystemRule.get() ), pageCache,
                fileSystemRule.get(),
                NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        NeoStores neoStores = storeFactory.openAllNeoStores( true );
        PropertyKeyTokenStore store = neoStores.getPropertyKeyTokenStore();
        for ( int i = 0; i < propertyKeyCount; i++ )
        {
            PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( (int) store.nextId() );
            record.setInUse( true );
            Collection<DynamicRecord> nameRecords = store.allocateNameRecords( PropertyStore.encodeString( key( i ) ) );
            record.addNameRecords( nameRecords );
            record.setNameId( (int) Iterables.first( nameRecords ).getId() );
            store.updateRecord( record );
        }
        neoStores.close();

        return database();
    }

    private String key( int i )
    {
        return "key" + i;
    }

    private Node createNodeWithProperty( GraphDatabaseService db, String key, Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( key, value );
            tx.success();
            return node;
        }
    }

    private int propertyKeyCount( GraphDatabaseAPI db ) throws TransactionFailureException
    {
        InwardKernel kernelAPI = db.getDependencyResolver().resolveDependency( InwardKernel.class );
        try ( KernelTransaction tx = kernelAPI.newTransaction( KernelTransaction.Type.implicit, AnonymousContext.read() ) )
        {
            return tx.tokenRead().propertyKeyCount();
        }
    }

    private static class WorkerState
    {
        protected final GraphDatabaseService db;
        protected Transaction tx;

        WorkerState( GraphDatabaseService db )
        {
            this.db = db;
        }
    }

    private static class BeginTx implements WorkerCommand<WorkerState, Void>
    {
        @Override
        public Void doWork( WorkerState state )
        {
            state.tx = state.db.beginTx();
            return null;
        }
    }

    private static class CreateNodeAndSetProperty implements WorkerCommand<WorkerState, Void>
    {
        private final String key;

        CreateNodeAndSetProperty( String key )
        {
            this.key = key;
        }

        @Override
        public Void doWork( WorkerState state )
        {
            Node node = state.db.createNode();
            node.setProperty( key, true );
            return null;
        }
    }

    private static class FinishTx implements WorkerCommand<WorkerState, Void>
    {
        @Override
        public Void doWork( WorkerState state )
        {
            state.tx.success();
            state.tx.close();
            return null;
        }
    }
}
