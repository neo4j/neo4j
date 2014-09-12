/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.test.Barrier;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.NamedFunction;
import org.neo4j.test.ThreadingRule;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class CacheRaceTest
{
    private final NodeCache nodeCache = new NodeCache();
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
                    return new StrongReferenceCache<RelationshipImpl>( CacheProvider.RELATIONSHIP_CACHE_NAME );
                }
            } );
        }
    };
    public final @Rule ThreadingRule threading = new ThreadingRule();

    @Test
    public void shouldNotGetDuplicateRelationshipsForNewNode() throws Exception
    {
        // given
        GraphDatabaseService graphDb = db.getGraphDatabaseService();
        Barrier.Control committing = new Barrier.Control();
        Future<Node> node = threading.execute( createNode( committing ), graphDb );
        committing.await();
        nodeCache.clear();
        Barrier.Control updateCache = nodeCache.blockThread( "create-node" );
        threading.threadBlockMonitor( currentThread(), new Release( updateCache ) );
        committing.release();
        updateCache.await(); // the 'create-node' thread is awaiting permission to update the cache
        List<String> before = countRelationshipsOfAllNodes( graphDb ); // read the store to re-populate the cache

        // when
        updateCache.release();
        node.get();

        // then
        List<String> after = countRelationshipsOfAllNodes( graphDb ); // read the store to re-populate the cache
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
        nodeCache.clear();
        Barrier.Control updateCache = nodeCache.blockThread( "add-relationship" );
        threading.threadBlockMonitor( currentThread(), new Release( updateCache ) );
        committing.release();
        updateCache.await(); // the 'create-node' thread is awaiting permission to update the cache
        List<String> before = countRelationshipsOfAllNodes( graphDb ); // read the store to re-populate the cache

        // when
        updateCache.release();
        rel.get();

        // then
        List<String> after = countRelationshipsOfAllNodes( graphDb ); // read the store to re-populate the cache
        assertEquals( join( "\n\t", after ), before.size(), after.size() );
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
            List<String> relationships = new ArrayList<String>();
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
        private final Map<String/*thread name*/, Barrier> barriers = new ConcurrentHashMap<String, Barrier>();

        public NodeCache()
        {
            super( CacheProvider.NODE_CACHE_NAME );
        }

        public Barrier.Control blockThread( String threadName )
        {
            Barrier.Control barrier = new Barrier.Control();
            barriers.put( threadName, barrier );
            return barrier;
        }

        @Override
        public NodeImpl get( long key )
        {
            Barrier barrier = barriers.get( currentThread().getName() );
            if ( barrier != null )
            {
                barrier.reached();
            }
            return super.get( key );
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