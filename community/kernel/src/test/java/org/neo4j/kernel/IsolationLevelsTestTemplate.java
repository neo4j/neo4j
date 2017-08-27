/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.IsolationLevel;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.test.rule.ReuseDatabaseClassRule;
import org.neo4j.test.rule.concurrent.ThreadRepository;
import org.neo4j.test.rule.concurrent.ThreadRepository.ThreadInfo;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith( Theories.class )
public abstract class IsolationLevelsTestTemplate
{
    @DataPoints
    public static final IsolationLevel[] LEVELS = IsolationLevel.values();

    // This rule means we'll only create a single database instance, and share it between all of the tests:
    @ClassRule
    public static final ReuseDatabaseClassRule reuse = new ReuseDatabaseClassRule();

    private static final RelationshipType REL = RelationshipType.withName( "REL" );
    private static final Label LABEL_A = Label.label( "A" );
    private static final Label LABEL_B = Label.label( "B" );
    private static final Label LABEL_C = Label.label( "C" );

    public DatabaseRule db = reuse.getOrCreate( this::createDatabaseRule );
    public ThreadRepository threads = new ThreadRepository( 10, TimeUnit.SECONDS );

    @Rule
    public RuleChain rules = RuleChain.outerRule( reuse.getRule() )
                                      .around( threads )
                                      .around( new RepeatRule( defaultTestRepetitions() ) );

    protected abstract DatabaseRule createDatabaseRule();

    protected int defaultTestRepetitions()
    {
        return 100;
    }

    protected static class DbTask
    {
        private final List<ThrowingBiConsumer<GraphDatabaseService,Transaction,Exception>> actions;
        private final GraphDatabaseService db;
        private boolean terminal;

        protected DbTask( GraphDatabaseService db )
        {
            this.db = db;
            this.actions = new ArrayList<>();
        }

        private void perform() throws Exception
        {
            try
            {
                Transaction tx = db.beginTx();
                for ( ThrowingBiConsumer<GraphDatabaseService,Transaction,Exception> action : actions )
                {
                    action.accept( db, tx );
                }
                if ( !terminal )
                {
                    tx.close();
                }
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
                throw e;
            }
        }

        public DbTask then( ThrowingBiConsumer<GraphDatabaseService,Transaction,Exception> action )
        {
            actions.add( action );
            return this;
        }

        public DbTask then( Consumer<GraphDatabaseService> action )
        {
            actions.add( ( db, tx ) -> action.accept( db ) );
            return this;
        }

        public DbTask then( ThreadRepository.Task task )
        {
            actions.add( ( db, tx ) -> task.perform() );
            return this;
        }

        public ThreadRepository.Task commit()
        {
            actions.add( ( db, tx ) -> tx.success() );
            actions.add( ( db, tx ) -> tx.close() );
            terminal = true;
            return this::perform;
        }

        public ThreadRepository.Task rollback()
        {
            actions.add( ( db, tx ) -> tx.close() );
            terminal = true;
            return this::perform;
        }
    }

    protected DbTask beginForked()
    {
        return new DbTask( db );
    }

    private void setIsolationLevel( Transaction tx, IsolationLevel level )
    {
        InternalTransaction internalTransaction = (InternalTransaction) tx;
        internalTransaction.setIsolationLevel( level );
    }

    private boolean shouldIgnoreTestForLevel( IsolationLevel level, IsolationLevel.Anomaly anomaly )
    {
        return !level.isSupported() || level.allows( anomaly );
    }

    private long txCreateNode( Label... labels )
    {
        return db.executeAndCommit( db ->
        {
            return db.createNode( labels ).getId();
        } );
    }

    private Long txCreateRelationship()
    {
        return db.executeAndCommit( db ->
        {
            Node node = db.createNode();
            Relationship rel = node.createRelationshipTo( node, REL );
            return rel.getId();
        } );
    }

    @Theory
    public void settingUnsupportedIsolationLevelMustThrow( IsolationLevel level )
    {
        if ( level.isSupported() )
        {
            return;
        }

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );
            fail( "setting an unsupported isolation level should have thrown" );
        }
        catch ( IllegalStateException ex )
        {
            // good!
        }
    }

    @Theory
    public void settingSupportedIsolationLevelMustNotThrow( IsolationLevel level )
    {
        if ( !level.isSupported() )
        {
            return;
        }

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );
            // that should not throw
        }
    }

    @Theory
    public void settingIsolationLevelAfterIsolationLevelHasAlreadyBeenSetMustThrow( IsolationLevel level )
    {

        if ( !level.isSupported() )
        {
            return;
        }

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );
            setIsolationLevel( tx, level );
            fail( "setting isolation level a second time should have thrown" );
        }
        catch ( IllegalStateException ex )
        {
            // good!
        }
    }

    @Theory
    public void settingIsolationLevelOnDataWriteTransactionMustThrow( IsolationLevel level )
    {
        if ( !level.isSupported() )
        {
            return;
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode(); // upgrade transaction mode from NONE to DATA WRITE
            setIsolationLevel( tx, level );
            fail( "setting isolation level in a data write transaction should have thrown" );
        }
        catch ( IllegalStateException ex )
        {
            // good!
        }
    }

    @Theory
    public void settingIsolationLevelOnSchemaWriteTransactionMustThrow( IsolationLevel level )
    {
        if ( !level.isSupported() )
        {
            return;
        }

        try ( Transaction tx = db.beginTx() )
        {
            // upgrade transaction mode from NONE to SCHEMA WRITE
            db.schema().indexFor( LABEL_C ).on( "random property" ).create();
            setIsolationLevel( tx, level );
            fail( "setting isolation level in a schema write transaction should have thrown" );
        }
        catch ( IllegalStateException ex )
        {
            // good!
        }
    }
    // todo perhaps change the thrown exception to some kind of KernelException?

    @Theory
    public void preventDirtyWriteOnNodeProperty( IsolationLevel level )
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.DirtyWrite ) )
        {
            return;
        }

        long nodeId = txCreateNode();
        ThreadRepository.Await await = threads.await();

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );
            Node node = db.getNodeById( nodeId );
            node.setProperty( "a", 1 );

            ThreadInfo thread = beginForked()
                    .then( db -> db.getNodeById( nodeId ).setProperty( "a", 2 ) )
                    .then( await )
                    .commit()
                    .run( threads );

            thread.waitUntilBlockedOrWaiting( 10_000 );

            // A dirty write would have overwritten our setProperty
            assertThat( node.getProperty( "a" ), is( 1 ) );
            tx.success();
        }
        finally
        {
            await.release();
        }
    }

    @Theory
    public void preventDirtyWriteOnNodeLabel( IsolationLevel level )
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.DirtyWrite ) )
        {
            return;
        }

        long nodeId = txCreateNode( LABEL_A );
        ThreadRepository.Await await = threads.await();

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );
            Node node = db.getNodeById( nodeId );
            node.removeLabel( LABEL_A );
            node.addLabel( LABEL_B );

            ThreadInfo thread = beginForked()
                    .then( db -> db.getNodeById( nodeId ).removeLabel( LABEL_A ) )
                    .then( db -> db.getNodeById( nodeId ).addLabel( LABEL_C ) )
                    .then( await )
                    .commit()
                    .run( threads );

            thread.waitUntilBlockedOrWaiting( 10_000 );

            // A dirty write would have overwritten our label updates
            assertFalse( node.hasLabel( LABEL_A ) );
            assertTrue( node.hasLabel( LABEL_B ) );
            assertFalse( node.hasLabel( LABEL_C ) );
            tx.success();
        }
        finally
        {
            await.release();
        }
    }

    @Theory
    public void preventDirtyWriteOnRelationshipProperty( IsolationLevel level )
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.DirtyWrite ) )
        {
            return;
        }

        long relId = txCreateRelationship();
        ThreadRepository.Await await = threads.await();

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );
            Relationship rel = db.getRelationshipById( relId );
            rel.setProperty( "a", 1 );

            ThreadInfo thread = beginForked()
                    .then( db -> db.getRelationshipById( relId ).setProperty( "a", 2 ) )
                    .then( await )
                    .commit()
                    .run( threads );

            thread.waitUntilBlockedOrWaiting( 10_000 );

            // A dirty write would have overwritten our setProperty
            assertThat( rel.getProperty( "a" ), is( 1 ) );
            tx.success();
        }
        finally
        {
            await.release();
        }
    }

    @Theory
    public void preventDirtyReadOfNode( IsolationLevel level ) throws Exception
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.DirtyRead ) )
        {
            return;
        }

        ThreadRepository.Await await = threads.await();
        AtomicLong nodeId = new AtomicLong( -1 );

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );

            beginForked()
                    .then( db -> nodeId.set( db.createNode().getId() ) )
                    .then( await )
                    .commit()
                    .run( threads );

            long id;
            do
            {
                id = nodeId.get();
            }
            while ( id == -1 );

            try
            {
                db.getNodeById( id );
                fail( "getNodeById should have thrown" );
            }
            catch ( NotFoundException e )
            {
                // good
            }
        }
        finally
        {
            await.release();
        }
    }

    @Theory
    public void preventDirtyReadOfRelationship( IsolationLevel level ) throws Exception
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.DirtyRead ) )
        {
            return;
        }

        long nodeId = txCreateNode();
        ThreadRepository.Await await = threads.await();
        AtomicLong relId = new AtomicLong( -1 );

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );

            beginForked()
                    .then( db -> relId.set( db.getNodeById( nodeId )
                                              .createRelationshipTo( db.getNodeById( nodeId ), REL )
                                              .getId() ) )
                    .then( await )
                    .commit()
                    .run( threads );

            long id;
            do
            {
                id = relId.get();
            }
            while ( id == -1 );

            try
            {
                db.getRelationshipById( id );
                fail( "getNodeById should have thrown" );
            }
            catch ( NotFoundException e )
            {
                // good
            }
        }
        finally
        {
            await.release();
        }
    }

    @Theory
    public void preventDirtyReadOfNodeProperty( IsolationLevel level ) throws InterruptedException
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.DirtyRead ) )
        {
            return;
        }

        long nodeId = txCreateNode();
        ThreadRepository.Await await = threads.await();
        ThreadRepository.Signal signal = threads.signal();

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );

            beginForked()
                    .then( db -> db.getNodeById( nodeId ).setProperty( "a", 1 ) )
                    .then( signal )
                    .then( await )
                    .commit()
                    .run( threads );

            signal.awaitNow();

            Node node = db.getNodeById( nodeId );
            // 'null' is the default value returned if there is no property by that key:
            Object a = node.getProperty( "a", null );
            assertThat( a, is( nullValue() ) );
        }
        finally
        {
            await.release();
        }
    }

    @Theory
    public void preventDirtyReadOfRelationshipProperty( IsolationLevel level ) throws InterruptedException
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.DirtyRead ) )
        {
            return;
        }

        long relId = txCreateRelationship();
        ThreadRepository.Await await = threads.await();
        ThreadRepository.Signal signal = threads.signal();

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );

            beginForked()
                    .then( db -> db.getRelationshipById( relId ).setProperty( "a", 1 ) )
                    .then( signal )
                    .then( await )
                    .commit()
                    .run( threads );

            signal.awaitNow();

            Relationship rel = db.getRelationshipById( relId );
            // 'null' is the default value returned if there is no property by that key:
            Object a = rel.getProperty( "a", null );
            assertThat( a, is( nullValue() ) );
        }
        finally
        {
            await.release();
        }
    }

    @Theory
    public void preventDirtyReadOfNodeLabel( IsolationLevel level ) throws InterruptedException
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.DirtyRead ) )
        {
            return;
        }

        long nodeId = txCreateNode();
        ThreadRepository.Await await = threads.await();
        ThreadRepository.Signal signal = threads.signal();

        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );

            beginForked()
                    .then( db -> db.getNodeById( nodeId ).addLabel( LABEL_A ) )
                    .then( signal )
                    .then( await )
                    .commit()
                    .run( threads );

            signal.awaitNow();

            Node node = db.getNodeById( nodeId );
            // 'null' is the default value returned if there is no property by that key:
            Collection<Label> labels = Iterables.asCollection( node.getLabels() );
            assertTrue( labels.isEmpty() );
        }
        finally
        {
            await.release();
        }
    }

    // todo prevent cursor lost update of node property from getAllNodes
    // todo prevent cursor lost update of node label from getAllNodes
    // todo prevent cursor lost update of relationship property from getAllRelationships
    // todo prevent cursor lost update of relationship property from Node getRelationships
    // todo prevent cursor lost update of node property from findNodes by lookup
    // todo prevent cursor lost update of node label from findNodes by lookup
    // todo prevent cursor lost update of node property from findNodes by label
    // todo prevent cursor lost update of node label from findNodes by label

    // todo prevent lost update of node property
    // todo prevent lost update of node label
    // todo prevent lost update of relationship property

    @Ignore( "Not implemented at this time, since it currently conflicts with iteration of relationships" )
    @Theory
    public void preventUnstableIteratorOfNodeProperties( IsolationLevel level )
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.UnstableIterator ) )
        {
            return;
        }

        String[] propertyNames = buildPropertyNames();

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            for ( String name : propertyNames )
            {
                node.setProperty( name, 1 );
            }
            tx.success();
        }

        int threadCount = 10;
        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );
            beginForked()
                    .then( db -> setAllProperties( db, nodeId, propertyNames ) )
                    .commit()
                    .run( threadCount, threads );

            while ( !threads.allDone() )
            {
                Node node = db.getNodeById( nodeId );
                Map<String,Object> allProperties = node.getAllProperties();
                Object valueToCompare = allProperties.get( propertyNames[0] );
                for ( Object value : allProperties.values() )
                {
                    assertThat( value, is( equalTo( valueToCompare ) ) );
                }
            }
        }
    }

    private String[] buildPropertyNames()
    {
        char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
        String[] propertyNames = new String[chars.length];
        int i = 0;
        for ( char x : chars )
        {
            propertyNames[i] = "" + x;
            i++;
        }
        return propertyNames;
    }

    private void setAllProperties( GraphDatabaseService db, long nodeId, String[] propertyNames )
    {
        int value = ThreadLocalRandom.current().nextInt();
        Node node = db.getNodeById( nodeId );
        for ( String name : propertyNames )
        {
            node.setProperty( name, value );
        }
    }

    @Theory
    public void preventUnstableIteratorOfNodeRelationships( IsolationLevel level )
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.UnstableIterator ) )
        {
            return;
        }
        RecordStorageEngine.takeRelationshipChainReadLocks = true;

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.createRelationshipTo( node, REL );
            tx.success();
        }

        int threadCount = 10;
        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );

            beginForked()
                    .then( (db,innerTx) -> setIsolationLevel( innerTx, level ) )
                    .then( (db,innerTx) -> deleteCreateRelationship( db, nodeId ) )
                    .commit()
                    .run( threadCount, threads );

            while ( !threads.allDone() )
            {
                Node node = db.getNodeById( nodeId );
                assertThat( Iterables.count( node.getRelationships() ), is( 1L ) );
            }
        }
        finally
        {
            RecordStorageEngine.takeRelationshipChainReadLocks = false;
        }
    }

    @Theory
    public void preventUnstableIteratorOfNodeRelationshipsWithTypes( IsolationLevel level )
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.UnstableIterator ) )
        {
            return;
        }
        RecordStorageEngine.takeRelationshipChainReadLocks = true;

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.createRelationshipTo( node, REL );
            tx.success();
        }

        int threadCount = 10;
        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );

            beginForked()
                    .then( (db,innerTx) -> setIsolationLevel( innerTx, level ) )
                    .then( db -> deleteCreateRelationship( db, nodeId ) )
                    .commit()
                    .run( threadCount, threads );

            while ( !threads.allDone() )
            {
                Node node = db.getNodeById( nodeId );
                assertThat( Iterables.count( node.getRelationships( REL ) ), is( 1L ) );
            }
        }
        finally
        {
            RecordStorageEngine.takeRelationshipChainReadLocks = false;
        }
    }

    @Theory
    public void doNotPreventUnstableIteratorOfMultipleNodeRelationshipsWithTypes( IsolationLevel level )
    {
        if ( shouldIgnoreTestForLevel( level, IsolationLevel.Anomaly.UnstableIterator ) )
        {
            return;
        }
        RecordStorageEngine.takeRelationshipChainReadLocks = true;

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            nodeId = node.getId();
            node.createRelationshipTo( node, REL );
            node.createRelationshipTo( node, REL );
            tx.success();
        }

        int threadCount = 10;
        try ( Transaction tx = db.beginTx() )
        {
            setIsolationLevel( tx, level );

            beginForked()
                    .then( (db,innerTx) -> setIsolationLevel( innerTx, level ) )
                    .then( db -> deleteCreateTwoRelationship( db, nodeId ) )
                    .commit()
                    .run( threadCount, threads );

            while ( !threads.allDone() )
            {
                Node node = db.getNodeById( nodeId );
                assertThat( Iterables.count( node.getRelationships( REL ) ), is( 2L ) );
            }
        }
        finally
        {
            RecordStorageEngine.takeRelationshipChainReadLocks = false;
        }
    }

    private void deleteCreateRelationship( GraphDatabaseService db, long nodeId )
    {
        Node node = db.getNodeById( nodeId );
        try
        {
            node.getRelationships().forEach( Relationship::delete );
            node.createRelationshipTo( node, REL );
        }
        catch ( DeadlockDetectedException | NotFoundException ignore )
        {
            // this is fine
        }
    }

    private void deleteCreateTwoRelationship( GraphDatabaseService db, long nodeId )
    {
        Node node = db.getNodeById( nodeId );
        try
        {
            node.getRelationships().forEach( Relationship::delete );
            node.createRelationshipTo( node, REL );
            node.createRelationshipTo( node, REL );
        }
        catch ( DeadlockDetectedException | NotFoundException ignore )
        {
            // this is fine
        }
    }
    // todo prevent unstable iterator of node labels
    // todo prevent unstable iterator of relationship properties
    // todo prevent unstable iterator of multiple node relationships
    // todo prevent unstable iterator of multiple node relationships with types

    // todo prevent fuzzy read of node property
    // todo prevent fuzzy read of node label
    // todo prevent fuzzy read of relationship property
    // todo prevent fuzzy read of node relationships

    // todo prevent phantom read of node that is removed from index
    // todo prevent phantom read of node that is added to index
    // todo prevent phantom read of node that is removed from constraint index
    // todo prevent phantom read of node that is added to constraint index
    // todo prevent phantom read of node that is removed from node key index
    // todo prevent phantom read of node that is added to node key index

    // todo read skew
    // todo write skew
    // todo read only serialisation anomaly
}
