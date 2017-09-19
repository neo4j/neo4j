/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.time.Clock;
import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FulltextIndexType.NODES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FulltextIndexType.RELATIONSHIPS;

public class LuceneFulltextUpdaterTest
{
    public static final StandardAnalyzer ANALYZER = new StandardAnalyzer();
    private static final Log LOG = NullLog.getInstance();

    @Rule
    public DatabaseRule dbRule = new EmbeddedDatabaseRule().startLazily();

    private static final RelationshipType RELTYPE = RelationshipType.withName( "type" );

    private AvailabilityGuard availabilityGuard = new AvailabilityGuard( Clock.systemDefaultZone(), LOG );
    private GraphDatabaseAPI db;
    private FulltextFactory fulltextFactory;
    private JobScheduler scheduler;

    @Before
    public void setUp() throws Exception
    {
        db = dbRule.getGraphDatabaseAPI();
        scheduler = dbRule.resolveDependency( JobScheduler.class );
        FileSystemAbstraction fs = dbRule.resolveDependency( FileSystemAbstraction.class );
        File storeDir = dbRule.getStoreDir();
        fulltextFactory = new FulltextFactory( fs, storeDir, ANALYZER );
    }

    @Test
    public void shouldFindNodeWithString() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                node2.setProperty( "prop",
                        "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                        "cross between a zebra and any other equine: essentially, a zebra hybrid." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, "hello", firstID );
                assertExactQueryFindsIds( reader, "zebra", secondID );
                assertExactQueryFindsIds( reader, "zedonk", secondID );
                assertExactQueryFindsIds( reader, "cross", secondID );
            }
        }
    }

    @Test
    public void shouldFindNodeWithNumber() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                node.setProperty( "prop", 1 );
                node2.setProperty( "prop", 234 );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, "1", firstID );
                assertExactQueryFindsIds( reader, "234", secondID );
            }
        }
    }

    @Test
    public void shouldFindNodeWithBoolean() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                node.setProperty( "prop", true );
                node2.setProperty( "prop", false );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, "true", firstID );
                assertExactQueryFindsIds( reader, "false", secondID );
            }
        }
    }

    @Test
    public void shouldFindNodeWithArrays() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            long thirdID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                Node node3 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                thirdID = node3.getId();
                node.setProperty( "prop", new String[]{"hello", "I", "live", "here"} );
                node2.setProperty( "prop", new int[]{1, 27, 48} );
                node3.setProperty( "prop", new int[]{1, 2, 48} );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, "live", firstID );
                assertExactQueryFindsIds( reader, "27", secondID );
                assertExactQueryFindsIds( reader, new String[]{"1", "2"}, secondID, thirdID );
            }
        }
    }

    @Test
    public void shouldRepresentPropertyChanges() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                node2.setProperty( "prop",
                        "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                        "cross between a zebra and any other equine: essentially, a zebra hybrid." );

                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.getNodeById( firstID );
                node.setProperty( "prop", "Hahahaha! potato!" );
                Node node2 = db.getNodeById( secondID );
                node2.setProperty( "prop", "This one is a potato farmer." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsNothing( reader, "hello" );
                assertExactQueryFindsNothing( reader, "zebra" );
                assertExactQueryFindsNothing( reader, "zedonk" );
                assertExactQueryFindsNothing( reader, "cross" );
                assertExactQueryFindsIds( reader, "hahahaha", firstID );
                assertExactQueryFindsIds( reader, "farmer", secondID );
                assertExactQueryFindsIds( reader, "potato", firstID, secondID );
            }
        }
    }

    @Test
    public void shouldNotFindRemovedNodes() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                node2.setProperty( "prop",
                        "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                        "cross between a zebra and any other equine: essentially, a zebra hybrid." );

                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                db.getNodeById( firstID ).delete();
                db.getNodeById( secondID ).delete();

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsNothing( reader, "hello" );
                assertExactQueryFindsNothing( reader, "zebra" );
                assertExactQueryFindsNothing( reader, "zedonk" );
                assertExactQueryFindsNothing( reader, "cross" );
            }
        }
    }

    @Test
    public void shouldNotFindRemovedProperties() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, Arrays.asList( "prop", "prop2" ), provider );
            provider.init();

            long firstID;
            long secondID;
            long thirdID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                Node node3 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                thirdID = node3.getId();

                node.setProperty( "prop", "Hello. Hello again." );
                node.setProperty( "prop", "zebra" );

                node2.setProperty( "prop",
                        "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                        "cross between a zebra and any other equine: essentially, a zebra hybrid." );
                node2.setProperty( "prop", "Hello. Hello again." );

                node3.setProperty( "prop", "Hello. Hello again." );

                tx.success();
            }

            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.getNodeById( firstID );
                Node node2 = db.getNodeById( secondID );
                Node node3 = db.getNodeById( thirdID );

                node.setProperty( "prop", "tomtar" );
                node.setProperty( "prop2", "tomtar" );

                node2.setProperty( "prop", "tomtar" );
                node2.setProperty( "prop2", "Hello" );

                node3.removeProperty( "prop" );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, "hello", secondID );
                assertExactQueryFindsNothing( reader, "zebra" );
                assertExactQueryFindsNothing( reader, "zedonk" );
                assertExactQueryFindsNothing( reader, "cross" );
            }
        }
    }

    @Test
    public void shouldOnlyIndexIndexedProperties() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                firstID = node.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                node.setProperty( "prop2", "zebra" );
                node2.setProperty( "prop2", "zebra" );
                node2.setProperty( "prop3", "hello" );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, "hello", firstID );
                assertExactQueryFindsNothing( reader, "zebra" );
            }
        }
    }

    @Test
    public void shouldSearchAcrossMultipleProperties() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, Arrays.asList( "prop", "prop2" ), provider );
            provider.init();

            long firstID;
            long secondID;
            long thirdID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                Node node3 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                thirdID = node3.getId();
                node.setProperty( "prop", "Tomtar tomtar oftsat i tomteutstyrsel." );
                node2.setProperty( "prop", "Olof och Hans" );
                node2.setProperty( "prop2", "och karl" );
                node3.setProperty( "prop2", "Tomtar som inte tomtar ser upp till tomtar som tomtar." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, new String[]{"tomtar", "karl"}, firstID, secondID, thirdID );
            }
        }
    }

    @Test
    public void shouldOrderResultsBasedOnRelevance() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, Arrays.asList( "first", "last" ), provider );
            provider.init();

            long firstID;
            long secondID;
            long thirdID;
            long fourthID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                Node node3 = db.createNode();
                Node node4 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                thirdID = node3.getId();
                fourthID = node4.getId();
                node.setProperty( "first", "Full" );
                node.setProperty( "last", "Hanks" );
                node2.setProperty( "first", "Tom" );
                node2.setProperty( "last", "Hunk" );
                node3.setProperty( "first", "Tom" );
                node3.setProperty( "last", "Hanks" );
                node4.setProperty( "first", "Tom Hanks" );
                node4.setProperty( "last", "Tom Hanks" );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, new String[]{"Tom", "Hanks"}, firstID, secondID, thirdID, fourthID );
            }
        }
    }

    @Test
    public void shouldDifferentiateNodesAndRelationships() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            fulltextFactory.createFulltextIndex( "relationships", RELATIONSHIPS, singletonList( "prop" ), provider );
            provider.init();

            long firstNodeID;
            long secondNodeID;
            long firstRelID;
            long secondRelID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                Relationship rel1 = node.createRelationshipTo( node2, RELTYPE );
                Relationship rel2 = node2.createRelationshipTo( node, RELTYPE );
                firstNodeID = node.getId();
                secondNodeID = node2.getId();
                firstRelID = rel1.getId();
                secondRelID = rel2.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                node2.setProperty( "prop",
                        "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                        "cross between a zebra and any other equine: essentially, a zebra hybrid." );
                rel1.setProperty( "prop", "Hello. Hello again." );
                rel2.setProperty( "prop", "And now, something completely different" );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, "hello", firstNodeID );
                assertExactQueryFindsIds( reader, "zebra", secondNodeID );
                assertExactQueryFindsNothing( reader, "different" );
            }
            try ( ReadOnlyFulltext reader = provider.getReader( "relationships", RELATIONSHIPS ) )
            {
                assertExactQueryFindsIds( reader, "hello", firstRelID );
                assertExactQueryFindsNothing( reader, "zebra" );
                assertExactQueryFindsIds( reader, "different", secondRelID );
            }
        }
    }

    @Test
    public void fuzzyQueryShouldBeFuzzy() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                node.setProperty( "prop", "Hello. Hello again." );
                node2.setProperty( "prop",
                        "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                        "cross between a zebra and any other equine: essentially, a zebra hybrid." );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertFuzzyQueryFindsIds( reader, "hella", firstID );
                assertFuzzyQueryFindsIds( reader, "zebre", secondID );
                assertFuzzyQueryFindsIds( reader, "zedink", secondID );
                assertFuzzyQueryFindsIds( reader, "cruss", secondID );
                assertExactQueryFindsNothing( reader, "hella" );
                assertExactQueryFindsNothing( reader, "zebre" );
                assertExactQueryFindsNothing( reader, "zedink" );
                assertExactQueryFindsNothing( reader, "cruss" );
            }
        }
    }

    @Test
    public void fuzzyQueryShouldReturnExactMatchesFirst() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            provider.init();

            long firstID;
            long secondID;
            long thirdID;
            long fourthID;
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                Node node3 = db.createNode();
                Node node4 = db.createNode();
                firstID = node.getId();
                secondID = node2.getId();
                thirdID = node3.getId();
                fourthID = node4.getId();
                node.setProperty( "prop", "zibre" );
                node2.setProperty( "prop", "zebrae" );
                node3.setProperty( "prop", "zebra" );
                node4.setProperty( "prop", "zibra" );

                tx.success();
            }

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertFuzzyQueryFindsIds( reader, "zebra", firstID, secondID, thirdID, fourthID );
            }
        }
    }

    @Test
    public void shouldNotReturnNonMatches() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            fulltextFactory.createFulltextIndex( "relationships", RELATIONSHIPS, singletonList( "prop" ), provider );
            provider.init();

            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode();
                Node node2 = db.createNode();
                Relationship rel1 = node.createRelationshipTo( node2, RELTYPE );
                Relationship rel2 = node2.createRelationshipTo( node, RELTYPE );
                node.setProperty( "prop", "Hello. Hello again." );
                node2.setProperty( "prop2",
                        "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                        "cross between a zebra and any other equine: essentially, a zebra hybrid." );
                rel1.setProperty( "prop", "Hello. Hello again." );
                rel2.setProperty( "prop2",
                        "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                        "cross between a zebra and any other equine: essentially, a zebra hybrid." );

                tx.success();
            }
            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsNothing( reader, "zebra" );
            }
            try ( ReadOnlyFulltext reader = provider.getReader( "relationships", RELATIONSHIPS ) )
            {
                assertExactQueryFindsNothing( reader, "zebra" );
            }
        }
    }

    @Test
    public void shouldPopulateIndexWithExistingNodesAndRelationships() throws Exception
    {
        long firstNodeID;
        long secondNodeID;
        long firstRelID;
        long secondRelID;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            Node node2 = db.createNode();

            // skip a few rel ids, so the ones we work with are different from the node ids, just in case.
            node.createRelationshipTo( node2, RELTYPE );
            node.createRelationshipTo( node2, RELTYPE );

            Relationship rel1 = node.createRelationshipTo( node2, RELTYPE );
            Relationship rel2 = node2.createRelationshipTo( node, RELTYPE );
            firstNodeID = node.getId();
            secondNodeID = node2.getId();
            firstRelID = rel1.getId();
            secondRelID = rel2.getId();
            node.setProperty( "prop", "Hello. Hello again." );
            node2.setProperty( "prop", "This string is slightly shorter than the zebra one" );
            rel1.setProperty( "prop", "Goodbye" );
            rel2.setProperty( "prop", "And now, something completely different" );

            tx.success();
        }

        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            fulltextFactory.createFulltextIndex( "relationships", RELATIONSHIPS, singletonList( "prop" ), provider );
            provider.init();
            provider.awaitPopulation();

            try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
            {
                assertExactQueryFindsIds( reader, "hello", firstNodeID );
                assertExactQueryFindsIds( reader, "string", secondNodeID );
                assertExactQueryFindsNothing( reader, "goodbye" );
                assertExactQueryFindsNothing( reader, "different" );
            }
            try ( ReadOnlyFulltext reader = provider.getReader( "relationships", RELATIONSHIPS ) )
            {
                assertExactQueryFindsNothing( reader, "hello" );
                assertExactQueryFindsNothing( reader, "string" );
                assertExactQueryFindsIds( reader, "goodbye", firstRelID );
                assertExactQueryFindsIds( reader, "different", secondRelID );
            }
        }
    }

    @Test
    public void shouldReturnMatchesThatContainLuceneSyntaxCharacters() throws Exception
    {
        try ( FulltextProvider provider = createProvider() )
        {
            fulltextFactory.createFulltextIndex( "nodes", NODES, singletonList( "prop" ), provider );
            provider.init();
            String[] luceneSyntaxElements =
                    {"+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", "\\"};

            long nodeId;
            try ( Transaction tx = db.beginTx() )
            {
                nodeId = db.createNodeId();
                tx.success();
            }

            for ( String elm : luceneSyntaxElements )
            {
                setNodeProp( nodeId, "Hello" + elm + " How are you " + elm + "today?" );

                try ( ReadOnlyFulltext reader = provider.getReader( "nodes", NODES ) )
                {
                    assertExactQueryFindsIds( reader, "Hello" + elm, nodeId );
                    assertExactQueryFindsIds( reader, elm + "today", nodeId );
                }
            }
        }
    }

    private FulltextProvider createProvider()
    {
        return new FulltextProvider( db, LOG, availabilityGuard, scheduler );
    }

    private void assertExactQueryFindsNothing( ReadOnlyFulltext reader, String query )
    {
        assertExactQueryFindsIds( reader, query );
    }

    private void assertExactQueryFindsIds( ReadOnlyFulltext reader, String[] query, long... ids )
    {
        PrimitiveLongIterator result = reader.query( query );
        assertQueryResultsMatch( result, ids );
    }

    private void assertExactQueryFindsIds( ReadOnlyFulltext reader, String query, long... ids )
    {
        assertExactQueryFindsIds( reader, new String[]{query}, ids );
    }

    private void assertFuzzyQueryFindsIds( ReadOnlyFulltext reader, String query, long... ids )
    {
        PrimitiveLongIterator result = reader.fuzzyQuery( query );
        assertQueryResultsMatch( result, ids );
    }

    private void assertQueryResultsMatch( PrimitiveLongIterator result, long[] ids )
    {
        PrimitiveLongSet set = PrimitiveLongCollections.setOf( ids );
        while ( result.hasNext() )
        {
            assertTrue( set.remove( result.next() ) );
        }
        assertTrue( set.isEmpty() );
    }

    private void setNodeProp( long nodeId, String value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( nodeId );
            node.setProperty( "prop", value );
            tx.success();
        }
    }
}
