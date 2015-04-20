/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.api.store.PersistenceCache;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.test.Barrier;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.RepeatRule;
import org.neo4j.test.RepeatRule.Repeat;
import org.neo4j.test.ThreadingRule;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.Iterables.toList;

public class CacheRaceTest
{
    private static final Predicate<StackTraceElement[]> PersistenceCache_apply = new Predicate<StackTraceElement[]>()
    {
        @Override
        public boolean accept( StackTraceElement[] trace )
        {
            for ( StackTraceElement element : trace )
            {
                if ( "apply".equals( element.getMethodName() ) &&
                     PersistenceCache.class.getName().equals( element.getClassName() ) )
                {
                    return true;
                }
            }
            return false;
        }
    };
    private final NodeCache nodeCache = new NodeCache();
    private final long seed = currentTimeMillis();
    private final Random random = new Random( seed );
    public final @Rule DatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseFactory factory )
        {
            factory.setCacheProviders( asList( cacheProvider() ) );
        }

        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.cache_type, StrongCacheProvider.NAME );
        }

        private CacheProvider cacheProvider()
        {
            return new CustomCacheProvider( StrongCacheProvider.NAME, new Callable<Cache<NodeImpl>>()
            {
                @Override
                public Cache<NodeImpl> call() throws Exception
                {
                    return nodeCache;
                }
            }, new Callable<Cache<RelationshipImpl>>()
            {
                @Override
                public Cache<RelationshipImpl> call() throws Exception
                {
                    return new StrongReferenceCache<>( CacheProvider.RELATIONSHIP_CACHE_NAME );
                }
            } );
        }
    };
    public final @Rule ThreadingRule threading = new ThreadingRule();
    public final @Rule RepeatRule repeater = new RepeatRule();

    @Test
    public void shouldNotGetDuplicateRelationshipsForNewNode() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        Barrier.Control committing = new Barrier.Control();
        Future<Node> node = threading.execute( createNode( committing ), graphDb );
        committing.await();
        Barrier.Control updateCache = nodeCache.blockThread( "create-node", PersistenceCache_apply );
        threading.threadBlockMonitor( currentThread(), new Release( updateCache ) );
        committing.release();
        updateCache.await(); // the 'create-node' thread is awaiting permission to update the cache
        nodeCache.clear();
        List<String> before = countRelationshipsOfAllNodes( graphDb ); // read the store to re-populate the cache

        // when
        updateCache.release();
        node.get();

        // then
        List<String> after = countRelationshipsOfAllNodes( graphDb ); // read resulting state
        // if before > after: we have encountered the race condition.
        // if before < after: we have managed to read the state before the transaction was written,
        //    and the cache needs to be cleared later.
        assertEquals( join( "\n\t", after ), before.size(), after.size() );
    }

    @Test
    public void shouldNotGetDuplicateRelationshipsForUpdatedNode() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        Node node = createNode( Barrier.NONE ).apply( graphDb );
        Barrier.Control committing = new Barrier.Control();
        Future<Relationship> rel = threading.execute( addRelationship( committing ), node );
        committing.await();
        Barrier.Control updateCache = nodeCache.blockThread( "add-relationship", PersistenceCache_apply );
        threading.threadBlockMonitor( currentThread(), new Release( updateCache ) );
        committing.release();
        updateCache.await(); // the 'create-node' thread is awaiting permission to update the cache
        nodeCache.clear();
        List<String> before = countRelationshipsOfAllNodes( graphDb ); // read the store to re-populate the cache

        // when
        updateCache.release();
        rel.get();

        // then
        List<String> after = countRelationshipsOfAllNodes( graphDb ); // read resulting state
        // if before > after: we have encountered the race condition.
        // if before < after: we have managed to read the state before the transaction was written,
        //    and the cache needs to be cleared later.
        assertEquals( join( "\n\t", after ), before.size(), after.size() );
    }

    @Repeat( times = 10 )
    @Test
    public void shouldNotCacheDuplicateRelationshipsStressTest() throws Exception
    {
        final GraphDatabaseAPI graphDb = db.getGraphDatabaseAPI();
        final CountDownLatch prepareLatch = new CountDownLatch( 2 );
        final CountDownLatch startSignal = new CountDownLatch( 1 );
        final Node node = createNode( graphDb );
        Relationship[] initialRels = createRelationships( graphDb, node, 1000, null );
        db.clearCache();

        ControlledThread reader = new ControlledThread( "Reader", prepareLatch, startSignal, seed+1 )
        {
            @Override
            protected void perform()
            {
                try ( Transaction tx = graphDb.beginTx() )
                {
                    IteratorUtil.count( node.getRelationships() );
                    tx.success();
                }
                catch ( NotFoundException e )
                {
                    // Expected, although perhaps not every single time?
                }
            }
        };
        ControlledThread evictor = new ControlledThread( "Evictor", prepareLatch, startSignal, seed+2 )
        {
            @Override
            protected void perform()
            {
                db.clearCache();
            }
        };
        prepareLatch.await();

        Pair<Relationship[],Relationship[]> modification = modifyRelationships( graphDb, node, 100, startSignal );
        Relationship[] additionalRels = modification.first();
        Relationship[] removedRels = modification.other();

        // Allow the other threads to speak freely about any error that may have occurred as well
        reader.awaitCompletion();
        evictor.awaitCompletion();

        // Verify that no duplicates exist and that the number of relationships add up
        Relationship[] rels = null;
        try ( Transaction tx = graphDb.beginTx() )
        {
            try
            {
                rels = duplicateSafeCountRelationships( node );
                if ( rels.length != initialRels.length + additionalRels.length - removedRels.length )
                {
                    throw new IllegalStateException( "Relationship count mismatch" );
                }
                tx.success();
            }
            catch ( IllegalStateException e )
            {
                fail( e.getMessage() + ":\n" +
                        "  initial:    " + Arrays.toString( initialRels ) + "\n" +
                        "  additional: " + Arrays.toString( additionalRels ) + "\n" +
                        "  removed:    " + Arrays.toString( removedRels ) + "\n" +
        (rels != null ? "  rels:       " + Arrays.toString( rels ) + "\n" : "") +
        (rels != null ? "  missing:    " + Arrays.toString( missingRels( initialRels, additionalRels, removedRels, rels ) ) + "\n" : "") +
                        "  on-disk:\n"   + onDiskChain( graphDb, node.getId() ) + "\n" +
                        "  seed:       " + seed );
            }
        }
    }

    private Relationship[] missingRels( Relationship[] initialRels, Relationship[] additionalRels, Relationship[] removedRels,
            Relationship[] rels )
    {
        Set<Relationship> set = new HashSet<>();
        set.addAll( Arrays.asList( initialRels ) );
        set.addAll( Arrays.asList( additionalRels ) );
        set.removeAll( Arrays.asList( removedRels ) );
        set.removeAll( Arrays.asList( rels ) );
        return set.toArray( new Relationship[set.size()] );
    }

    private String onDiskChain( GraphDatabaseAPI graphDb, long nodeId )
    {
        StringBuilder builder = new StringBuilder();
        NeoStore neoStore = graphDb.getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate();
        NodeRecord node = neoStore.getNodeStore().getRecord( nodeId );
        if ( node.isDense() )
        {
            RelationshipGroupRecord group = neoStore.getRelationshipGroupStore().getRecord( node.getNextRel() );
            do
            {
                builder.append( "group " + group );
                builder.append( "out:\n" );
                printRelChain( builder, neoStore, nodeId, group.getFirstOut() );
                builder.append( "in:\n" );
                printRelChain( builder, neoStore, nodeId, group.getFirstIn() );
                builder.append( "loop:\n" );
                printRelChain( builder, neoStore, nodeId, group.getFirstLoop() );
                group = group.getNext() != -1 ? neoStore.getRelationshipGroupStore().getRecord( group.getNext() ) : null;
            } while ( group != null );
        }
        else
        {
            printRelChain( builder, neoStore, nodeId, node.getNextRel() );
        }
        return builder.toString();
    }

    private void printRelChain( StringBuilder builder, NeoStore access, long nodeId, long firstRelId )
    {
        for ( long rel = firstRelId; rel != Record.NO_NEXT_RELATIONSHIP.intValue(); )
        {
            RelationshipRecord record = access.getRelationshipStore().getRecord( rel );
            builder.append( rel + "\t" + record + "\n" );
            if ( record.getFirstNode() == nodeId )
            {
                rel = record.getFirstNextRel();
            }
            else
            {
                rel = record.getSecondNextRel();
            }
        }
    }

    private Relationship[] duplicateSafeCountRelationships( final Node node )
    {
        Set<Relationship> relationships = new HashSet<>();
        List<Relationship> result = new ArrayList<>();
        for ( Relationship relationship : node.getRelationships() )
        {
            if ( !relationships.add( relationship ) )
            {
                throw new IllegalStateException( "Spotted duplication relationship " + relationship );
            }
            result.add( relationship );
        }

        return result.toArray( new Relationship[result.size()] );
    }

    private Node createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            tx.success();
            return node;
        }
    }

    private Relationship[] createRelationships( GraphDatabaseAPI graphDb, Node node, int relsBound,
            CountDownLatch startLatch )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node other = graphDb.createNode();

            int nbrRels = random.nextInt( relsBound );
            Relationship[] result = new Relationship[nbrRels];
            for ( int i = 0; i < nbrRels; i++ )
            {
                result[i] = createRandomRelationship( node, other );
            }

            tx.success();

            if ( startLatch != null )
            {
                startLatch.countDown();
            }

            return result;
        }
    }

    private Relationship createRandomRelationship( Node node, Node other )
    {
        int dir = random.nextInt( 3 );
        RelationshipType type = withName( "TYPE_" + random.nextInt( 4 ) );
        Relationship rel;
        if ( dir == 0 )
        {   // OUTGOING
            rel = node.createRelationshipTo( other, type );
        }
        else if ( dir == 1 )
        {   // INCOMING
            rel = other.createRelationshipTo( node, type );
        }
        else
        {   // LOOP
            rel = node.createRelationshipTo( node, type );
        }
        return rel;
    }

    private Pair<Relationship[],Relationship[]> modifyRelationships( GraphDatabaseAPI graphDb, Node node,
            int maxNumberOfChanges, CountDownLatch startLatch )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            List<Relationship> createdRelationships = new ArrayList<>();
            List<Relationship> deletedRelationships = new ArrayList<>();
            Node other = graphDb.createNode();
            int changes = random.nextInt( maxNumberOfChanges );
            for ( int i = 0; i < changes; i++ )
            {
                if ( random.nextFloat() < 0.8f ) // 1/5 or so
                {
                    createdRelationships.add( createRandomRelationship( node, other ) );
                }
                else
                {
                    Relationship deletedRelationship = deleteRandomRelationship( node );
                    if ( deletedRelationship != null )
                    {
                        deletedRelationships.add( deletedRelationship );
                    }
                }
            }

            tx.success();

            if ( startLatch != null )
            {
                startLatch.countDown();
            }

            return Pair.of(
                    createdRelationships.toArray( new Relationship[createdRelationships.size()] ),
                    deletedRelationships.toArray( new Relationship[deletedRelationships.size()] ) );
        }
    }

    private Relationship deleteRandomRelationship( Node node )
    {
        List<Relationship> rels = toList( node.getRelationships() );
        if ( !rels.isEmpty() )
        {
            Relationship relationship = rels.get( random.nextInt( rels.size() ) );
            relationship.delete();
            return relationship;
        }
        return null;
    }

    private static abstract class ControlledThread extends Thread
    {
        private final Random random;
        private final CountDownLatch prepareLatch;
        private final CountDownLatch startSignal;
        private volatile Exception error;

        ControlledThread( String name, CountDownLatch prepareLatch, CountDownLatch startSignal, long seed )
        {
            super( name );
            this.random = new Random( seed );
            this.prepareLatch = prepareLatch;
            this.startSignal = startSignal;
            start();
        }

        @Override
        public void run()
        {
            try
            {
                prepareLatch.countDown();
                startSignal.await();
                LockSupport.parkNanos( random.nextInt( 20_000_000 ) );
                perform();
            }
            catch ( Exception e )
            {
                error = e;
                throw Exceptions.launderedException( e );
            }
        }

        protected abstract void perform();

        protected void awaitCompletion() throws Exception
        {
            join();
            if ( error != null )
            {
                throw error;
            }
        }
    }

    private Function<GraphDatabaseService, Node> createNode( final Barrier done )
    {
        return new NamedFunction<GraphDatabaseService, Node>( "create-node" )
        {
            @Override
            public Node apply( GraphDatabaseService graphDb )
            {
                try ( Transaction tx = graphDb.beginTx() )
                {
                    Node node = graphDb.createNode();
                    node.createRelationshipTo( graphDb.createNode(), withName( "FOO" ) );
                    tx.success();
                    done.reached();
                    return node;
                }
            }
        };
    }

    private Function<Node, Relationship> addRelationship( final Barrier done )
    {
        return new NamedFunction<Node, Relationship>( "add-relationship" )
        {
            @Override
            public Relationship apply( Node node )
            {
                GraphDatabaseService graphDb = node.getGraphDatabase();
                try ( Transaction tx = graphDb.beginTx() )
                {
                    Relationship rel = node.createRelationshipTo( graphDb.createNode(), withName( "FOO" ) );
                    tx.success();
                    done.reached();
                    return rel;
                }
            }
        };
    }

    private static List<String> countRelationshipsOfAllNodes( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            List<String> relationships = new ArrayList<>();
            for ( Node node : GlobalGraphOperations.at( graphDb ).getAllNodes() )
            {
                for ( Relationship relationship : node.getRelationships() )
                {
                    relationships.add( format( "(%d)%s[%d]%s(%d)",
                                               node.getId(),
                                               node.equals( relationship.getStartNode() ) ? "-" : "<-",
                                               relationship.getId(),
                                               node.equals( relationship.getEndNode() ) ? "-" : "->",
                                               relationship.getOtherNode( node ).getId() ) );
                }
            }
            tx.success();
            return relationships;
        }
    }

    private static class Release implements Runnable
    {
        private final Barrier.Control barrierControl;

        public Release( Barrier.Control barrierControl )
        {
            this.barrierControl = barrierControl;
        }

        @Override
        public void run()
        {
            barrierControl.release();
        }
    }

    private static class NodeCache extends StrongReferenceCache<NodeImpl>
    {
        private final Map<String, Function<StackTraceElement[], Barrier>> barriers = new ConcurrentHashMap<>();

        public NodeCache()
        {
            super( CacheProvider.NODE_CACHE_NAME );
        }

        public Barrier.Control blockThread( String threadName, Predicate<StackTraceElement[]> tracePredicate )
        {
            Barrier.Control barrier = new Barrier.Control();
            barriers.put( threadName, new Conditional<StackTraceElement[], Barrier>( tracePredicate, barrier ) );
            return barrier;
        }

        @Override
        public NodeImpl get( long key )
        {
            Barrier barrier = barrierFor( currentThread() );
            if ( barrier != null )
            {
                barrier.reached();
            }
            return super.get( key );
        }

        private Barrier barrierFor( Thread thread )
        {
            Function<StackTraceElement[], Barrier> conditional = barriers.get( thread.getName() );
            return conditional == null ? null : conditional.apply( thread.getStackTrace() );
        }
    }

    private static class Conditional<CONDITION, VALUE> implements Function<CONDITION, VALUE>
    {
        private final Predicate<CONDITION> predicate;
        private final VALUE value;

        public Conditional( Predicate<CONDITION> predicate, VALUE value )
        {
            this.predicate = predicate;
            this.value = value;
        }

        @Override
        public VALUE apply( CONDITION condition )
        {
            return predicate.accept( condition ) ? value : null;
        }
    }

    private static String join( String sep, Collection<?> items )
    {
        StringBuilder result = new StringBuilder();
        for ( Object item : items )
        {
            result.append( sep ).append( item );
        }
        return result.toString();
    }
}
