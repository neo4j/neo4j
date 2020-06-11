/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingImpl;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.IOUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.exceptions.schema.RepeatedLabelInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInSchemaException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedRelationshipTypeInSchemaException;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.procedure.builtin.FulltextProcedures;
import org.neo4j.test.ThreadTestUtils;
import org.neo4j.util.concurrent.BinaryLatch;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.AWAIT_REFRESH;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.DB_AWAIT_INDEX;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.DB_INDEXES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.DROP;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.LIST_AVAILABLE_ANALYZERS;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.NODE_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.QUERY_NODES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.QUERY_RELS;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.RELATIONSHIP_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asStrList;

class FulltextProceduresTest extends FulltextProceduresTestSupport
{
    @Test
    void createNodeFulltextIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "test-index", asStrList( "Label1", "Label2" ), asStrList( "prop1", "prop2" ) ) ).close();
            tx.commit();
        }
        Result result;
        Map<String,Object> row;
        try ( Transaction tx = db.beginTx() )
        {
            result = tx.execute( DB_INDEXES );
            assertTrue( result.hasNext() );
            row = result.next();
            assertEquals( asList( "Label1", "Label2" ), row.get( "labelsOrTypes" ) );
            assertEquals( asList( "prop1", "prop2" ), row.get( "properties" ) );
            assertEquals( "test-index", row.get( "name" ) );
            assertEquals( "FULLTEXT", row.get( "type" ) );
            assertFalse( result.hasNext() );
            result.close();
            tx.commit();
        }
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            result = tx.execute( DB_INDEXES );
            assertTrue( result.hasNext() );
            assertEquals( "ONLINE", result.next().get( "state" ) );
            assertFalse( result.hasNext() );
            result.close();
            assertNotNull( tx.schema().getIndexByName( "test-index" ) );
            tx.commit();
        }
        restartDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            result = tx.execute( DB_INDEXES );
            assertTrue( result.hasNext() );
            row = result.next();
            assertEquals( asList( "Label1", "Label2" ), row.get( "labelsOrTypes" ) );
            assertEquals( asList( "prop1", "prop2" ), row.get( "properties" ) );
            assertEquals( "test-index", row.get( "name" ) );
            assertEquals( "FULLTEXT", row.get( "type" ) );
            assertEquals( "ONLINE", row.get( "state" ) );
            assertFalse( result.hasNext() );
            //noinspection ConstantConditions
            assertFalse( result.hasNext() );
            assertNotNull( tx.schema().getIndexByName( "test-index" ) );
            tx.commit();
        }
    }

    @Test
    void createRelationshipFulltextIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( RELATIONSHIP_CREATE, "test-index", asStrList( "Reltype1", "Reltype2" ), asStrList( "prop1", "prop2" ) ) ).close();
            tx.commit();
        }
        Result result;
        Map<String,Object> row;
        try ( Transaction tx = db.beginTx() )
        {
            result = tx.execute( DB_INDEXES );
            assertTrue( result.hasNext() );
            row = result.next();
            assertEquals( asList( "Reltype1", "Reltype2" ), row.get( "labelsOrTypes" ) );
            assertEquals( asList( "prop1", "prop2" ), row.get( "properties" ) );
            assertEquals( "test-index", row.get( "name" ) );
            assertEquals( "FULLTEXT", row.get( "type" ) );
            assertFalse( result.hasNext() );
            result.close();
            tx.commit();
        }
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            result = tx.execute( DB_INDEXES );
            assertTrue( result.hasNext() );
            assertEquals( "ONLINE", result.next().get( "state" ) );
            assertFalse( result.hasNext() );
            result.close();
            assertNotNull( tx.schema().getIndexByName( "test-index" ) );
            tx.commit();
        }
        restartDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            result = tx.execute( DB_INDEXES );
            assertTrue( result.hasNext() );
            row = result.next();
            assertEquals( asList( "Reltype1", "Reltype2" ), row.get( "labelsOrTypes" ) );
            assertEquals( asList( "prop1", "prop2" ), row.get( "properties" ) );
            assertEquals( "test-index", row.get( "name" ) );
            assertEquals( "FULLTEXT", row.get( "type" ) );
            assertEquals( "ONLINE", row.get( "state" ) );
            assertFalse( result.hasNext() );
            //noinspection ConstantConditions
            assertFalse( result.hasNext() );
            assertNotNull( tx.schema().getIndexByName( "test-index" ) );
            tx.commit();
        }
    }

    @Test
    void dropIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "node", asStrList( "Label1", "Label2" ), asStrList( "prop1", "prop2" ) ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, "rel", asStrList( "Reltype1", "Reltype2" ), asStrList( "prop1", "prop2" ) ) ).close();
            Map<String,String> indexes = new HashMap<>();
            tx.execute( "call db.indexes()" ).forEachRemaining( m -> indexes.put( (String) m.get( "name" ), (String) m.get( "description" ) ) );

            tx.execute( format( DROP, "node" ) );
            indexes.remove( "node" );
            Map<String,String> newIndexes = new HashMap<>();
            tx.execute( "call db.indexes()" ).forEachRemaining( m -> newIndexes.put( (String) m.get( "name" ), (String) m.get( "description" ) ) );
            assertEquals( indexes, newIndexes );

            tx.execute( format( DROP, "rel" ) );
            indexes.remove( "rel" );
            newIndexes.clear();
            tx.execute( "call db.indexes()" ).forEachRemaining( m -> newIndexes.put( (String) m.get( "name" ), (String) m.get( "description" ) ) );
            assertEquals( indexes, newIndexes );
            tx.commit();
        }
    }

    @Test
    void mustNotBeAbleToCreateTwoIndexesWithSameName()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "node", asStrList( "Label1", "Label2" ), asStrList( "prop1", "prop2" ) ) ).close();
            tx.commit();
        }
        assertThrows( QueryExecutionException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( NODE_CREATE, "node", asStrList( "Label1", "Label2" ), asStrList( "prop3", "prop4" ) ) ).close();
                tx.commit();
            }
        }, "already exists" );
    }

    @Test
    void mustNotBeAbleToCreateNormalIndexWithSameNameAndSchemaAsExistingFulltextIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "node", asStrList( "Label1" ), asStrList( "prop1" ) ) ).close();
            tx.commit();
        }
        assertThrows( QueryExecutionException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( "CREATE INDEX `node` FOR (n:Label1) ON (n.prop1)" ).close();
                tx.commit();
            }
        }, "already exists" );
    }

    @Test
    void mustNotBeAbleToCreateNormalIndexWithSameNameDifferentSchemaAsExistingFulltextIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "node", asStrList( "Label1" ), asStrList( "prop1", "prop2" ) ) ).close();
            tx.commit();
        }
        assertThrows( QueryExecutionException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( "CREATE INDEX `node` FOR (n:Label1) ON (n.prop1)" ).close();
                tx.commit();
            }
        }, "There already exists an index called 'node'." );
    }

    @Test
    void mustNotBeAbleToCreateFulltextIndexWithSameNameAndSchemaAsExistingNormalIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE INDEX `node` FOR (n:Label1) ON (n.prop1)" ).close();
            tx.commit();
        }
        assertThrows( QueryExecutionException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( NODE_CREATE, "node", asStrList( "Label1" ), asStrList( "prop1" ) ) ).close();
                tx.commit();
            }
        }, "already exists" );
    }

    @Test
    void mustNotBeAbleToCreateFulltextIndexWithSameNameDifferentSchemaAsExistingNormalIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE INDEX `node` FOR (n:Label1) ON (n.prop1)" ).close();
            tx.commit();
        }
        assertThrows( QueryExecutionException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( NODE_CREATE, "node", asStrList( "Label1" ), asStrList( "prop1", "prop2" ) ) ).close();
                tx.commit();
            }
        }, "already exists" );
    }

    @Test
    void nodeIndexesMustHaveLabels()
    {
        assertThrows( QueryExecutionException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( NODE_CREATE, "nodeIndex", asStrList(), asStrList( PROP ) ) ).close();
            }
        } );
    }

    @Test
    void relationshipIndexesMustHaveRelationshipTypes()
    {
        assertThrows( QueryExecutionException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( RELATIONSHIP_CREATE, "relIndex", asStrList(), asStrList( PROP ) ) );
            }
        } );
    }

    @Test
    void nodeIndexesMustHaveProperties()
    {
        assertThrows( QueryExecutionException.class, () -> {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( NODE_CREATE, "nodeIndex", asStrList( "Label" ), asStrList() ) ).close();
            }
        } );
    }

    @Test
    void relationshipIndexesMustHaveProperties()
    {
        assertThrows( QueryExecutionException.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( RELATIONSHIP_CREATE, "relIndex", asStrList( "RELTYPE" ), asStrList() ) );
            }
        } );
    }

    @Test
    void creatingIndexesWhichImpliesTokenCreateMustNotBlockForever()
    {
        try ( Transaction tx = db.beginTx() )
        {
            // The property keys and labels we ask for do not exist, so those tokens will have to be allocated.
            // This test verifies that the locking required for the index modifications do not conflict with the
            // locking required for the token allocation.
            tx.execute( format( NODE_CREATE, "nodesA", asStrList( "SOME_LABEL" ), asStrList( "this" ) ) );
            tx.execute( format( RELATIONSHIP_CREATE, "relsA", asStrList( "SOME_REL_TYPE" ), asStrList( "foo" ) ) );
            tx.execute( format( NODE_CREATE, "nodesB", asStrList( "SOME_OTHER_LABEL" ), asStrList( "that" ) ) );
            tx.execute( format( RELATIONSHIP_CREATE, "relsB", asStrList( "SOME_OTHER_REL_TYPE" ), asStrList( "bar" ) ) );
        }
    }

    @Test
    void creatingIndexWithSpecificAnalyzerMustUseThatAnalyzerForPopulationUpdatingAndQuerying()
    {
        LongHashSet noResults = new LongHashSet();
        LongHashSet swedishNodes = new LongHashSet();
        LongHashSet englishNodes = new LongHashSet();
        LongHashSet swedishRels = new LongHashSet();
        LongHashSet englishRels = new LongHashSet();

        String labelledSwedishNodes = "labelledSwedishNodes";
        String typedSwedishRelationships = "typedSwedishRelationships";

        try ( Transaction tx = db.beginTx() )
        {
            // Nodes and relationships picked up by index population.
            Node nodeA = tx.createNode( LABEL );
            nodeA.setProperty( PROP, "En apa och en tomte bodde i ett hus." );
            swedishNodes.add( nodeA.getId() );
            Node nodeB = tx.createNode( LABEL );
            nodeB.setProperty( PROP, "Hello and hello again, in the end." );
            englishNodes.add( nodeB.getId() );
            Relationship relA = nodeA.createRelationshipTo( nodeB, REL );
            relA.setProperty( PROP, "En apa och en tomte bodde i ett hus." );
            swedishRels.add( relA.getId() );
            Relationship relB = nodeB.createRelationshipTo( nodeA, REL );
            relB.setProperty( PROP, "Hello and hello again, in the end." );
            englishRels.add( relB.getId() );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            String lbl = asStrList( LABEL.name() );
            String rel = asStrList( REL.name() );
            String props = asStrList( PROP );
            String swedish = props + ", {analyzer: '" + FulltextAnalyzerTest.SWEDISH + "'}";
            tx.execute( format( NODE_CREATE, labelledSwedishNodes, lbl, swedish ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, typedSwedishRelationships, rel, swedish ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            // Nodes and relationships picked up by index updates.
            Node nodeC = tx.createNode( LABEL );
            nodeC.setProperty( PROP, "En apa och en tomte bodde i ett hus." );
            swedishNodes.add( nodeC.getId() );
            Node nodeD = tx.createNode( LABEL );
            nodeD.setProperty( PROP, "Hello and hello again, in the end." );
            englishNodes.add( nodeD.getId() );
            Relationship relC = nodeC.createRelationshipTo( nodeD, REL );
            relC.setProperty( PROP, "En apa och en tomte bodde i ett hus." );
            swedishRels.add( relC.getId() );
            Relationship relD = nodeD.createRelationshipTo( nodeC, REL );
            relD.setProperty( PROP, "Hello and hello again, in the end." );
            englishRels.add( relD.getId() );
            tx.commit();
        }
        assertQueryFindsIds( db, true, labelledSwedishNodes, "and", englishNodes ); // english word
        // swedish stop word (ignored by swedish analyzer, and not among the english nodes)
        assertQueryFindsIds( db, true, labelledSwedishNodes, "ett", noResults );
        assertQueryFindsIds( db, true, labelledSwedishNodes, "apa", swedishNodes ); // swedish word

        assertQueryFindsIds( db, false, typedSwedishRelationships, "and", englishRels );
        assertQueryFindsIds( db, false, typedSwedishRelationships, "ett", noResults );
        assertQueryFindsIds( db, false, typedSwedishRelationships, "apa", swedishRels );
    }

    @Test
    void mustFailToCreateIndexWithUnknownAnalyzer()
    {
        try ( Transaction tx = db.beginTx() )
        {
            String label = asStrList( LABEL.name() );
            String props = asStrList( PROP );
            String analyzer = props + ", {analyzer: 'blablalyzer'}";
            try
            {
                tx.execute( format( NODE_CREATE, "my_index", label, analyzer ) ).close();
                fail( "Expected query to fail." );
            }
            catch ( QueryExecutionException e )
            {
                assertThat( e.getMessage() ).contains( "blablalyzer" );
            }
        }
    }

    @Test
    void queryShouldFindDataAddedInLaterTransactions()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "node", asStrList( "Label1", "Label2" ), asStrList( "prop1", "prop2" ) ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, "rel", asStrList( "Reltype1", "Reltype2" ), asStrList( "prop1", "prop2" ) ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();
        long horseId;
        long horseRelId;
        try ( Transaction tx = db.beginTx() )
        {
            Node zebra = tx.createNode();
            zebra.setProperty( "prop1", "zebra" );
            Node horse = tx.createNode( Label.label( "Label1" ) );
            horse.setProperty( "prop2", "horse" );
            horse.setProperty( "prop3", "zebra" );
            Relationship horseRel = zebra.createRelationshipTo( horse, RelationshipType.withName( "Reltype1" ) );
            horseRel.setProperty( "prop1", "horse" );
            Relationship loop = horse.createRelationshipTo( horse, RelationshipType.withName( "loop" ) );
            loop.setProperty( "prop2", "zebra" );

            horseId = horse.getId();
            horseRelId = horseRel.getId();
            tx.commit();
        }
        assertQueryFindsIds( db, true, "node", "horse", newSetWith( horseId ) );
        assertQueryFindsIds( db, true, "node", "horse zebra", newSetWith( horseId ) );

        assertQueryFindsIds( db, false, "rel", "horse", newSetWith( horseRelId ) );
        assertQueryFindsIds( db, false, "rel", "horse zebra", newSetWith( horseRelId ) );
    }

    @Test
    void queryShouldFindDataAddedInIndexPopulation()
    {
        // when
        Node node1;
        Node node2;
        Relationship relationship;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = tx.createNode( LABEL );
            node1.setProperty( PROP, "This is a integration test." );
            node2 = tx.createNode( LABEL );
            node2.setProperty( "otherprop", "This is a related integration test" );
            relationship = node1.createRelationshipTo( node2, REL );
            relationship.setProperty( PROP, "They relate" );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "node", asStrList( LABEL.name() ), asStrList( PROP, "otherprop" ) ) );
            tx.execute( format( RELATIONSHIP_CREATE, "rel", asStrList( REL.name() ), asStrList( PROP ) ) );
            tx.commit();
        }
        awaitIndexesOnline();

        // then
        assertQueryFindsIds( db, true, "node", "integration", node1.getId(), node2.getId() );
        assertQueryFindsIds( db, true, "node", "test", node1.getId(), node2.getId() );
        assertQueryFindsIds( db, true, "node", "related", newSetWith( node2.getId() ) );
        assertQueryFindsIds( db, false, "rel", "relate", newSetWith( relationship.getId() ) );
    }

    @Test
    void updatesToEventuallyConsistentIndexMustEventuallyBecomeVisible()
    {
        String value = "bla bla";
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "node", asStrList( LABEL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT ) );
            tx.execute( format( RELATIONSHIP_CREATE, "rel", asStrList( REL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT ) );
            tx.commit();
        }

        int entityCount = 200;
        LongHashSet nodeIds = new LongHashSet();
        LongHashSet relIds = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < entityCount; i++ )
            {
                Node node = tx.createNode( LABEL );
                node.setProperty( PROP, value );
                Relationship rel = node.createRelationshipTo( node, REL );
                rel.setProperty( PROP, value );
                nodeIds.add( node.getId() );
                relIds.add( rel.getId() );
            }
            tx.commit();
        }

        // Assert that we can observe our updates within 20 seconds from now. We have, after all, already committed the transaction.
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 20 );
        boolean success = false;
        do
        {
            try
            {
                assertQueryFindsIds( db, true, "node", "bla", nodeIds );
                assertQueryFindsIds( db, false, "rel", "bla", relIds );
                success = true;
            }
            catch ( Throwable throwable )
            {
                if ( deadline <= System.currentTimeMillis() )
                {
                    // We're past the deadline. This test is not successful.
                    throw throwable;
                }
            }
        }
        while ( !success );
    }

    @Test
    void updatesToEventuallyConsistentIndexMustBecomeVisibleAfterAwaitRefresh()
    {
        String value = "bla bla";
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "node", asStrList( LABEL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT ) );
            tx.execute( format( RELATIONSHIP_CREATE, "rel", asStrList( REL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT ) );
            tx.commit();
        }
        awaitIndexesOnline();

        int entityCount = 200;
        LongHashSet nodeIds = new LongHashSet();
        LongHashSet relIds = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < entityCount; i++ )
            {
                Node node = tx.createNode( LABEL );
                node.setProperty( PROP, value );
                Relationship rel = node.createRelationshipTo( node, REL );
                rel.setProperty( PROP, value );
                nodeIds.add( node.getId() );
                relIds.add( rel.getId() );
            }
            tx.commit();
        }

        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( AWAIT_REFRESH ).close();
            transaction.commit();
        }
        assertQueryFindsIds( db, true, "node", "bla", nodeIds );
        assertQueryFindsIds( db, false, "rel", "bla", relIds );
    }

    @Test
    void eventuallyConsistentIndexMustPopulateWithExistingDataWhenCreated()
    {
        String value = "bla bla";
        int entityCount = 200;
        LongHashSet nodeIds = new LongHashSet();
        LongHashSet relIds = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < entityCount; i++ )
            {
                Node node = tx.createNode( LABEL );
                node.setProperty( PROP, value );
                Relationship rel = node.createRelationshipTo( node, REL );
                rel.setProperty( PROP, value );
                nodeIds.add( node.getId() );
                relIds.add( rel.getId() );
            }
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "node", asStrList( LABEL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT ) );
            tx.execute( format( RELATIONSHIP_CREATE, "rel", asStrList( REL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT ) );
            tx.commit();
        }

        awaitIndexesOnline();
        assertQueryFindsIds( db, true, "node", "bla", nodeIds );
        assertQueryFindsIds( db, false, "rel", "bla", relIds );
    }

    @Test
    void concurrentPopulationAndUpdatesToAnEventuallyConsistentIndexMustEventuallyResultInCorrectIndexState() throws Exception
    {
        String oldValue = "red";
        String newValue = "green";
        int entityCount = 200;
        LongHashSet nodeIds = new LongHashSet();
        LongHashSet relIds = new LongHashSet();

        // First we create the nodes and relationships with the property value "red".
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < entityCount; i++ )
            {
                Node node = tx.createNode( LABEL );
                node.setProperty( PROP, oldValue );
                Relationship rel = node.createRelationshipTo( node, REL );
                rel.setProperty( PROP, oldValue );
                nodeIds.add( node.getId() );
                relIds.add( rel.getId() );
            }
            tx.commit();
        }

        // Then, in two concurrent transactions, we create our indexes AND change all the property values to "green".
        CountDownLatch readyLatch = new CountDownLatch( 2 );
        BinaryLatch startLatch = new BinaryLatch();
        Runnable createIndexes = () ->
        {
            readyLatch.countDown();
            startLatch.await();
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( NODE_CREATE, "node", asStrList( LABEL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT ) );
                tx.execute( format( RELATIONSHIP_CREATE, "rel", asStrList( REL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT ) );
                tx.commit();
            }
        };
        Runnable makeAllEntitiesGreen = () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                // Prepare our transaction state first.
                nodeIds.forEach( nodeId -> tx.getNodeById( nodeId ).setProperty( PROP, newValue ) );
                relIds.forEach( relId -> tx.getRelationshipById( relId ).setProperty( PROP, newValue ) );
                tx.commit();
                // Okay, NOW we're ready to race!
                readyLatch.countDown();
                startLatch.await();
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool( 2 );
        Future<?> future1 = executor.submit( createIndexes );
        Future<?> future2 = executor.submit( makeAllEntitiesGreen );
        readyLatch.await();
        startLatch.release();

        // Finally, when everything has settled down, we should see that all of the nodes and relationships are indexed with the value "green".
        try
        {
            future1.get();
            future2.get();
            awaitIndexesOnline();
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( AWAIT_REFRESH ).close();
            }
            assertQueryFindsIds( db, true, "node", newValue, nodeIds );
            assertQueryFindsIds( db, false, "rel", newValue, relIds );
        }
        finally
        {
            IOUtils.closeAllSilently( executor::shutdown );
        }
    }

    @Test
    void mustBeAbleToListAvailableAnalyzers()
    {
        // Verify that a couple of expected analyzers are available.
        try ( Transaction tx = db.beginTx() )
        {
            Set<String> analyzers = new HashSet<>();
            try ( ResourceIterator<String> iterator = tx.execute( LIST_AVAILABLE_ANALYZERS ).columnAs( "analyzer" ) )
            {
                while ( iterator.hasNext() )
                {
                    analyzers.add( iterator.next() );
                }
            }
            assertThat( analyzers ).contains( "english" );
            assertThat( analyzers ).contains( "swedish" );
            assertThat( analyzers ).contains( "standard" );
            assertThat( analyzers ).contains( "galician" );
            assertThat( analyzers ).contains( "irish" );
            assertThat( analyzers ).contains( "latvian" );
            assertThat( analyzers ).contains( "sorani" );
            tx.commit();
        }

        // Verify that all analyzers have a description.
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( LIST_AVAILABLE_ANALYZERS ) )
            {
                while ( result.hasNext() )
                {
                    Map<String,Object> row = result.next();
                    Object description = row.get( "description" );
                    if ( !row.containsKey( "description" ) || !(description instanceof String) || ((String) description).trim().isEmpty() )
                    {
                        fail( "Found no description for analyzer: " + row );
                    }
                }
            }
            tx.commit();
        }
    }

    @SuppressWarnings( "unchecked" )
    @Test
    void analyzersMustKnowTheirStopWords()
    {
        // Verify that analyzers have stop-words.
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( LIST_AVAILABLE_ANALYZERS ) )
            {
                while ( result.hasNext() )
                {
                    Map<String,Object> row = result.next();
                    Object stopwords = row.get( "stopwords" );
                    if ( !row.containsKey( "stopwords" ) || !(stopwords instanceof List) )
                    {
                        fail( "Found no stop-words list for analyzer: " + row );
                    }

                    List<String> words = (List<String>) stopwords;
                    String analyzerName = (String) row.get( "analyzer" );
                    if ( analyzerName.equals( "english" ) || analyzerName.equals( "standard" ) )
                    {
                        assertThat( words ).contains( "and" );
                    }
                    else if ( analyzerName.equals( "standard-no-stop-words" ) )
                    {
                        assertTrue( words.isEmpty() );
                    }
                }
            }
            tx.commit();
        }

        // Verify that the stop-words data-sets are clean; that they contain no comments, white-space or empty strings.
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( LIST_AVAILABLE_ANALYZERS ) )
            {
                while ( result.hasNext() )
                {
                    Map<String,Object> row = result.next();
                    List<String> stopwords = (List<String>) row.get( "stopwords" );
                    for ( String stopword : stopwords )
                    {
                        if ( stopword.isBlank() || stopword.contains( "#" ) || stopword.contains( " " ) )
                        {
                            fail( "The list of stop-words for the " + row.get( "analyzer" ) + " analyzer contains dirty data. " +
                                    "Specifically, '" + stopword + "' does not look like a valid stop-word. The full list:" +
                                    System.lineSeparator() + stopwords );
                        }
                    }
                }
            }
            tx.commit();
        }
    }

    @Test
    void queryNodesMustThrowWhenQueryingRelationshipIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( Exception.class, () -> tx.execute( format( QUERY_NODES, "rels", "bla bla" ) ).next() );
        }
    }

    @Test
    void queryRelationshipsMustThrowWhenQueryingNodeIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( Exception.class, () -> tx.execute( format( QUERY_RELS, "nodes", "bla bla" ) ).next() );
        }
    }

    @Test
    void fulltextIndexMustIgnoreNonStringPropertiesForUpdate()
    {
        Label label = LABEL;
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "nodes", asStrList( label.name() ), asStrList( PROP ) ) ).close();
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }

        awaitIndexesOnline();

        List<Value> values = generateRandomNonStringValues();

        try ( Transaction tx = db.beginTx() )
        {
            for ( Value value : values )
            {
                Node node = tx.createNode( label );
                Object propertyValue = value.asObject();
                node.setProperty( PROP, propertyValue );
                node.createRelationshipTo( node, REL ).setProperty( PROP, propertyValue );
            }
            tx.commit();
        }

        for ( Value value : values )
        {
            try ( Transaction tx = db.beginTx() )
            {
                String fulltextQuery = quoteValueForQuery( value );
                String cypherQuery = format( QUERY_NODES, "nodes", fulltextQuery );
                Result nodes;
                try
                {
                    nodes = tx.execute( cypherQuery );
                }
                catch ( QueryExecutionException e )
                {
                    throw new AssertionError( "Failed to execute query: " + cypherQuery + " based on value " + value.prettyPrint(), e );
                }
                if ( nodes.hasNext() )
                {
                    fail( "did not expect to find any nodes, but found at least: " + nodes.next() );
                }
                nodes.close();
                Result relationships = tx.execute( format( QUERY_RELS, "rels", fulltextQuery ) );
                if ( relationships.hasNext() )
                {
                    fail( "did not expect to find any relationships, but found at least: " + relationships.next() );
                }
                relationships.close();
                tx.commit();
            }
        }
    }

    @Test
    void fulltextIndexMustIgnoreNonStringPropertiesForPopulation()
    {
        List<Value> values = generateRandomNonStringValues();

        try ( Transaction tx = db.beginTx() )
        {
            for ( Value value : values )
            {
                Node node = tx.createNode( LABEL );
                Object propertyValue = value.asObject();
                node.setProperty( PROP, propertyValue );
                node.createRelationshipTo( node, REL ).setProperty( PROP, propertyValue );
            }
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }

        awaitIndexesOnline();

        for ( Value value : values )
        {
            try ( Transaction tx = db.beginTx() )
            {
                String fulltextQuery = quoteValueForQuery( value );
                String cypherQuery = format( QUERY_NODES, "nodes", fulltextQuery );
                Result nodes;
                try
                {
                    nodes = tx.execute( cypherQuery );
                }
                catch ( QueryExecutionException e )
                {
                    throw new AssertionError( "Failed to execute query: " + cypherQuery + " based on value " + value.prettyPrint(), e );
                }
                if ( nodes.hasNext() )
                {
                    fail( "did not expect to find any nodes, but found at least: " + nodes.next() );
                }
                nodes.close();
                Result relationships = tx.execute( format( QUERY_RELS, "rels", fulltextQuery ) );
                if ( relationships.hasNext() )
                {
                    fail( "did not expect to find any relationships, but found at least: " + relationships.next() );
                }
                relationships.close();
                tx.commit();
            }
        }
    }

    @Test
    void entitiesMustBeRemovedFromFulltextIndexWhenPropertyValuesChangeAwayFromText()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            nodeId = node.getId();
            node.setProperty( PROP, "bla bla" );
            tx.commit();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.setProperty( PROP, 42 );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( format( QUERY_NODES, "nodes", "bla" ) );
            assertFalse( result.hasNext() );
            result.close();
            tx.commit();
        }
    }

    @Test
    void entitiesMustBeAddedToFulltextIndexWhenPropertyValuesChangeToText()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, 42 );
            nodeId = node.getId();
            tx.commit();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.setProperty( PROP, "bla bla" );
            tx.commit();
        }

        assertQueryFindsIds( db, true, "nodes", "bla", nodeId );
    }

    @Test
    void propertiesMustBeRemovedFromFulltextIndexWhenTheirValueTypeChangesAwayFromText()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "nodes", asStrList( LABEL.name() ), asStrList( "prop1", "prop2" ) ) ).close();
            tx.commit();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            nodeId = node.getId();
            node.setProperty( "prop1", "foo" );
            node.setProperty( "prop2", "bar" );
            tx.commit();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.setProperty( "prop2", 42 );
            tx.commit();
        }

        assertQueryFindsIds( db, true, "nodes", "foo", nodeId );
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( format( QUERY_NODES, "nodes", "bar" ) );
            assertFalse( result.hasNext() );
            result.close();
            tx.commit();
        }
    }

    @Test
    void propertiesMustBeAddedToFulltextIndexWhenTheirValueTypeChangesToText()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "nodes", asStrList( LABEL.name() ), asStrList( "prop1", "prop2" ) ) ).close();
            tx.commit();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            nodeId = node.getId();
            node.setProperty( "prop1", "foo" );
            node.setProperty( "prop2", 42 );
            tx.commit();
        }

        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.setProperty( "prop2", "bar" );
            tx.commit();
        }

        assertQueryFindsIds( db, true, "nodes", "foo", nodeId );
        assertQueryFindsIds( db, true, "nodes", "bar", nodeId );
    }

    @Test
    void mustBeAbleToIndexHugeTextPropertiesInIndexUpdates() throws Exception
    {
        String meditationes;
        try ( BufferedReader reader = new BufferedReader(
                new InputStreamReader( getClass().getResourceAsStream( DESCARTES_MEDITATIONES ), StandardCharsets.UTF_8 ) ) )
        {
            meditationes = reader.lines().collect( Collectors.joining( "\n" ) );
        }

        Label label = Label.label( "Book" );
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "books", asStrList( label.name() ), asStrList( "title", "author", "contents" ) ) ).close();
            tx.commit();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label );
            nodeId = node.getId();
            node.setProperty( "title", "Meditationes de prima philosophia" );
            node.setProperty( "author", "René Descartes" );
            node.setProperty( "contents", meditationes );
            tx.commit();
        }

        awaitIndexesOnline();

        assertQueryFindsIds( db, true, "books", "impellit scriptum offerendum", nodeId );
    }

    @Test
    void mustBeAbleToIndexHugeTextPropertiesInIndexPopulation() throws Exception
    {
        String meditationes;
        try ( BufferedReader reader = new BufferedReader(
                new InputStreamReader( getClass().getResourceAsStream( DESCARTES_MEDITATIONES ), StandardCharsets.UTF_8 ) ) )
        {
            meditationes = reader.lines().collect( Collectors.joining( "\n" ) );
        }

        Label label = Label.label( "Book" );
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label );
            nodeId = node.getId();
            node.setProperty( "title", "Meditationes de prima philosophia" );
            node.setProperty( "author", "René Descartes" );
            node.setProperty( "contents", meditationes );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "books", asStrList( label.name() ), asStrList( "title", "author", "contents" ) ) ).close();
            tx.commit();
        }

        awaitIndexesOnline();

        assertQueryFindsIds( db, true, "books", "impellit scriptum offerendum", nodeId );
    }

    @Test
    void mustBeAbleToQuerySpecificPropertiesViaLuceneSyntax()
    {
        Label book = Label.label( "Book" );
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "books", asStrList( book.name() ), asStrList( "title", "author" ) ) ).close();
            tx.commit();
        }

        long book2id;
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node book1 = tx.createNode( book );
            book1.setProperty( "author", "René Descartes" );
            book1.setProperty( "title", "Meditationes de prima philosophia" );
            Node book2 = tx.createNode( book );
            book2.setProperty( "author", "E. M. Curley" );
            book2.setProperty( "title", "Descartes Against the Skeptics" );
            book2id = book2.getId();
            tx.commit();
        }

        LongHashSet ids = newSetWith( book2id );
        assertQueryFindsIds( db, true, "books", "title:Descartes", ids );
    }

    @Test
    void mustIndexNodesByCorrectProperties()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "nodes", asStrList( LABEL.name() ), asStrList( "a", "b", "c", "d", "e", "f" ) ) ).close();
            tx.commit();
        }
        long nodeId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( "e", "value" );
            nodeId = node.getId();
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "e:value", nodeId );
    }

    @Test
    void queryingIndexInPopulatingStateMustBlockUntilIndexIsOnline()
    {
        long nodeCount = 10_000;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodeCount; i++ )
            {

                tx.createNode( LABEL ).setProperty( PROP, "value" );
            }
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( format( QUERY_NODES, "nodes", "value" ) );
                  Stream<Map<String,Object>> stream = result.stream() )
            {
                assertThat( stream.count() ).isEqualTo( nodeCount );
            }
            tx.commit();
        }
    }

    @Test
    void queryingIndexInPopulatingStateMustBlockUntilIndexIsOnlineEvenWhenTransactionHasState()
    {
        long nodeCount = 10_000;
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < nodeCount; i++ )
            {

                tx.createNode( LABEL ).setProperty( PROP, "value" );
            }
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( LABEL ).setProperty( PROP, "value" );
            try ( Result result = tx.execute( format( QUERY_NODES, "nodes", "value" ) );
                  Stream<Map<String,Object>> stream = result.stream() )
            {
                assertThat( stream.count() ).isEqualTo( nodeCount + 1 );
            }
            tx.commit();
        }
    }

    @Test
    void queryingIndexInTransactionItWasCreatedInMustThrow()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            assertThrows( QueryExecutionException.class, () -> tx.execute( format( QUERY_NODES, "nodes", "value" ) ).next() );
        }
    }

    @Test
    void queryResultsMustNotIncludeNodesDeletedInOtherConcurrentlyCommittedTransactions() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeIdA;
        long nodeIdB;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = tx.createNode( LABEL );
            nodeA.setProperty( PROP, "value" );
            nodeIdA = nodeA.getId();
            Node nodeB = tx.createNode( LABEL );
            nodeB.setProperty( PROP, "value" );
            nodeIdB = nodeB.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( format( QUERY_NODES, "nodes", "value" ) ) )
            {
                ThreadTestUtils.forkFuture( () ->
                {
                    try ( Transaction forkedTx = db.beginTx() )
                    {
                        tx.getNodeById( nodeIdA ).delete();
                        tx.getNodeById( nodeIdB ).delete();
                        forkedTx.commit();
                    }
                    return null;
                } ).get();
                assertThat( result.stream().count() ).isEqualTo( 0L );
            }
            tx.commit();
        }
    }

    @Test
    void queryResultsMustNotIncludeRelationshipsDeletedInOtherConcurrentlyCommittedTransactions() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        long relIdA;
        long relIdB;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            Relationship relA = node.createRelationshipTo( node, REL );
            relA.setProperty( PROP, "value" );
            relIdA = relA.getId();
            Relationship relB = node.createRelationshipTo( node, REL );
            relB.setProperty( PROP, "value" );
            relIdB = relB.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( format( QUERY_RELS, "rels", "value" ) ) )
            {
                ThreadTestUtils.forkFuture( () ->
                {
                    try ( Transaction forkedTx = db.beginTx() )
                    {
                        tx.getRelationshipById( relIdA ).delete();
                        tx.getRelationshipById( relIdB ).delete();
                        forkedTx.commit();
                    }
                    return null;
                } ).get();
                assertThat( result.stream().count() ).isEqualTo( 0L );
            }
            tx.commit();
        }
    }

    @Test
    void queryResultsMustNotIncludeNodesDeletedInThisTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeIdA;
        long nodeIdB;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node nodeA = tx.createNode( LABEL );
            nodeA.setProperty( PROP, "value" );
            nodeIdA = nodeA.getId();
            Node nodeB = tx.createNode( LABEL );
            nodeB.setProperty( PROP, "value" );
            nodeIdB = nodeB.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( nodeIdA ).delete();
            tx.getNodeById( nodeIdB ).delete();
            try ( Result result = tx.execute( format( QUERY_NODES, "nodes", "value" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 0L );
            }
            tx.commit();
        }
    }

    @Test
    void queryResultsMustNotIncludeRelationshipsDeletedInThisTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        long relIdA;
        long relIdB;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            Relationship relA = node.createRelationshipTo( node, REL );
            relA.setProperty( PROP, "value" );
            relIdA = relA.getId();
            Relationship relB = node.createRelationshipTo( node, REL );
            relB.setProperty( PROP, "value" );
            relIdB = relB.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getRelationshipById( relIdA ).delete();
            tx.getRelationshipById( relIdB ).delete();
            try ( Result result = tx.execute( format( QUERY_RELS, "rels", "value" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 0L );
            }
            tx.commit();
        }
    }

    @Test
    void queryResultsMustIncludeNodesAddedInThisTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        awaitIndexesOnline();
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode( LABEL );
            node.setProperty( PROP, "value" );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "value", newSetWith( node.getId() ) );
    }

    @Test
    void queryResultsMustIncludeRelationshipsAddedInThisTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        awaitIndexesOnline();
        Relationship relationship;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            relationship = node.createRelationshipTo( node, REL );
            relationship.setProperty( PROP, "value" );
            tx.commit();
        }
        assertQueryFindsIds( db, false, "rels", "value", newSetWith( relationship.getId() ) );
    }

    @Test
    void queryResultsMustIncludeNodesWithPropertiesAddedToBeIndexed()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = tx.createNode( LABEL ).getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( nodeId ).setProperty( PROP, "value" );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "prop:value", nodeId );
    }

    @Test
    void queryResultsMustIncludeRelationshipsWithPropertiesAddedToBeIndexed()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        long relId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo( node, REL );
            relId = rel.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Relationship rel = tx.getRelationshipById( relId );
            rel.setProperty( PROP, "value" );
            tx.commit();
        }
        assertQueryFindsIds( db, false, "rels", "prop:value", relId );
    }

    @Test
    void queryResultsMustIncludeNodesWithLabelsModifedToBeIndexed()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            node.setProperty( PROP, "value" );
            nodeId = node.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.addLabel( LABEL );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "value", nodeId );
    }

    @Test
    void queryResultsMustIncludeUpdatedValueOfChangedNodeProperties()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "primo" );
            nodeId = node.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( nodeId ).setProperty( PROP, "secundo" );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "primo" );
        assertQueryFindsIds( db, true, "nodes", "secundo", nodeId );
    }

    @Test
    void queryResultsMustIncludeUpdatedValuesOfChangedRelationshipProperties()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        long relId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo( node, REL );
            rel.setProperty( PROP, "primo" );
            relId = rel.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getRelationshipById( relId ).setProperty( PROP, "secundo" );
            tx.commit();
        }
        assertQueryFindsIds( db, false, "rels", "primo" );
        assertQueryFindsIds( db, false, "rels", "secundo", relId );
    }

    @Test
    void queryResultsMustNotIncludeNodesWithRemovedIndexedProperties()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "value" );
            nodeId = node.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( nodeId ).removeProperty( PROP );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "value" );
    }

    @Test
    void queryResultsMustNotIncludeRelationshipsWithRemovedIndexedProperties()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        long relId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo( node, REL );
            rel.setProperty( PROP, "value" );
            relId = rel.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getRelationshipById( relId ).removeProperty( PROP );
            tx.commit();
        }
        assertQueryFindsIds( db, false, "rels", "value" );
    }

    @Test
    void queryResultsMustNotIncludeNodesWithRemovedIndexedLabels()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "value" );
            nodeId = node.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( nodeId ).removeLabel( LABEL );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "nodes" );
    }

    @Test
    void queryResultsMustIncludeOldNodePropertyValuesWhenModificationsAreUndone()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "primo" );
            nodeId = node.getId();
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "primo", nodeId );
        assertQueryFindsIds( db, true, "nodes", "secundo" );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.setProperty( PROP, "secundo" );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "primo" );
        assertQueryFindsIds( db, true, "nodes", "secundo", nodeId );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.setProperty( PROP, "primo" );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "primo", nodeId );
        assertQueryFindsIds( db, true, "nodes", "secundo" );
    }

    @Test
    void queryResultsMustIncludeOldRelationshipPropertyValuesWhenModificationsAreUndone()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        long relId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo( node, REL );
            rel.setProperty( PROP, "primo" );
            relId = rel.getId();
            tx.commit();
        }
        assertQueryFindsIds( db, false, "rels", "primo", relId );
        assertQueryFindsIds( db, false, "rels", "secundo" );
        try ( Transaction tx = db.beginTx() )
        {
            Relationship rel = tx.getRelationshipById( relId );
            rel.setProperty( PROP, "secundo" );
            tx.commit();
        }
        assertQueryFindsIds( db, false, "rels", "primo" );
        assertQueryFindsIds( db, false, "rels", "secundo", relId );
        try ( Transaction tx = db.beginTx() )
        {
            Relationship rel = tx.getRelationshipById( relId );
            rel.setProperty( PROP, "primo" );
            tx.commit();
        }
        assertQueryFindsIds( db, false, "rels", "primo", relId );
        assertQueryFindsIds( db, false, "rels", "secundo" );
    }

    @Test
    void queryResultsMustIncludeOldNodePropertyValuesWhenRemovalsAreUndone()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "primo" );
            nodeId = node.getId();
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "primo", nodeId );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.removeProperty( PROP );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "primo" );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.setProperty( PROP, "primo" );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "primo", nodeId );
    }

    @Test
    void queryResultsMustIncludeOldRelationshipPropertyValuesWhenRemovalsAreUndone()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        long relId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo( node, REL );
            rel.setProperty( PROP, "primo" );
            relId = rel.getId();
            tx.commit();
        }
        assertQueryFindsIds( db, false, "rels", "primo", relId );
        try ( Transaction tx = db.beginTx() )
        {
            Relationship rel = tx.getRelationshipById( relId );
            rel.removeProperty( PROP );
            tx.commit();
        }
        assertQueryFindsIds( db, false, "rels", "primo" );
        try ( Transaction tx = db.beginTx() )
        {
            Relationship rel = tx.getRelationshipById( relId );
            rel.setProperty( PROP, "primo" );
            tx.commit();
        }
        assertQueryFindsIds( db, false, "rels", "primo", relId );
    }

    @Test
    void queryResultsMustIncludeNodesWhenNodeLabelRemovalsAreUndone()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long nodeId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "primo" );
            nodeId = node.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.removeLabel( LABEL );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "primo" );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.addLabel( LABEL );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "primo", nodeId );
    }

    @Test
    void queryResultsFromTransactionStateMustSortTogetherWithResultFromBaseIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long firstId;
        long secondId;
        long thirdId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node first = tx.createNode( LABEL );
            first.setProperty( PROP, "God of War" );
            firstId = first.getId();
            Node third = tx.createNode( LABEL );
            third.setProperty( PROP, "God Wars: Future Past" );
            thirdId = third.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node second = tx.createNode( LABEL );
            second.setProperty( PROP, "God of War III Remastered" );
            secondId = second.getId();
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "god of war", firstId, secondId, thirdId );
    }

    @Test
    void queryResultsMustBeOrderedByScore()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        long firstId;
        long secondId;
        long thirdId;
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "dude sweet" );
            firstId = node.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "dude sweet dude sweet" );
            secondId = node.getId();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "dude sweet dude dude dude sweet" );
            thirdId = node.getId();
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "dude", thirdId, secondId, firstId );
    }

    @Test
    void queryingDroppedIndexForNodesInDroppingTransactionMustThrow()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( DROP, "nodes" ) ).close();
            assertThrows( QueryExecutionException.class, () -> tx.execute( format( QUERY_NODES, "nodes", "blabla" ) ).next() );
        }
    }

    @Test
    void queryingDroppedIndexForRelationshipsInDroppingTransactionMustThrow()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( DROP, "rels" ) ).close();
            assertThrows( QueryExecutionException.class, () -> tx.execute( format( QUERY_RELS, "rels", "blabla" ) ).next() );
        }
    }

    @Test
    void creatingAndDroppingIndexesInSameTransactionMustNotThrow()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.execute( format( DROP, "nodes" ) ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleRelationshipIndex( tx );
            tx.execute( format( DROP, "rels" ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            assertFalse( tx.schema().getIndexes().iterator().hasNext() );
            tx.commit();
        }
    }

    @Test
    void eventuallyConsistentIndexMustNotIncludeEntitiesAddedInTransaction()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "nodes", asStrList( LABEL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, "rels", asStrList( REL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "value" );
            node.createRelationshipTo( node, REL ).setProperty( PROP, "value" );
        }

        assertQueryFindsIds( db, true, "nodes", "value" );
        assertQueryFindsIds( db, false, "rels", "value" );
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( AWAIT_REFRESH ).close();
            transaction.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "value" );
        assertQueryFindsIds( db, false, "rels", "value" );
    }

    @Test
    void prefixedFulltextIndexSettingMustBeRecognized()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "nodes", asStrList( LABEL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT_PREFIXED ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, "rels", asStrList( REL.name() ), asStrList( PROP ) + EVENTUALLY_CONSISTENT_PREFIXED ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : tx.schema().getIndexes() )
            {
                Map<IndexSetting,Object> indexConfiguration = index.getIndexConfiguration();
                Object eventuallyConsistentObj = indexConfiguration.get( IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT );
                assertNotNull( eventuallyConsistentObj );
                assertThat( eventuallyConsistentObj ).isInstanceOf( Boolean.class );
                assertTrue( (Boolean) eventuallyConsistentObj );
            }
            tx.commit();
        }
    }

    @Test
    void prefixedFulltextIndexSettingMustBeRecognizedTogetherWithNonPrefixed()
    {
        try ( Transaction tx = db.beginTx() )
        {
            String mixedPrefixConfig = ", {`fulltext.analyzer`: 'english', eventually_consistent: 'true'}";
            tx.execute( format( NODE_CREATE, "nodes", asStrList( LABEL.name() ), asStrList( PROP ) + mixedPrefixConfig ) ).close();
            tx.execute( format( RELATIONSHIP_CREATE, "rels", asStrList( REL.name() ), asStrList( PROP ) + mixedPrefixConfig ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : tx.schema().getIndexes() )
            {
                Map<IndexSetting,Object> indexConfiguration = index.getIndexConfiguration();
                Object eventuallyConsistentObj = indexConfiguration.get( IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT );
                assertNotNull( eventuallyConsistentObj );
                assertThat( eventuallyConsistentObj ).isInstanceOf( Boolean.class );
                assertTrue( (Boolean) eventuallyConsistentObj );
                Object analyzerObj = indexConfiguration.get( IndexSettingImpl.FULLTEXT_ANALYZER );
                assertNotNull( analyzerObj );
                assertThat( analyzerObj ).isInstanceOf( String.class );
                assertEquals( "english", analyzerObj );
            }
            tx.commit();
        }
    }

    @Test
    void mustThrowOnDuplicateFulltextIndexSetting()
    {
        String duplicateConfig = ", {`fulltext.analyzer`: 'english', analyzer: 'swedish'}";

        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "nodes", asStrList( LABEL.name() ), asStrList( PROP ) + duplicateConfig ) ).close();
            fail( "Expected to fail" );
        }
        catch ( QueryExecutionException e )
        {
            assertThat( e.getMessage() ).contains( "Config setting was specified more than once, 'analyzer'." );
            Throwable rootCause = getRootCause( e );
            assertThat( rootCause ).isInstanceOf( IllegalArgumentException.class );
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( RELATIONSHIP_CREATE, "rels", asStrList( REL.name() ), asStrList( PROP ) + duplicateConfig ) ).close();
            fail( "Expected to fail" );
        }
        catch ( QueryExecutionException e )
        {
            assertThat( e.getMessage() ).contains( "Config setting was specified more than once, 'analyzer'." );
            Throwable rootCause = getRootCause( e );
            assertThat( rootCause ).isInstanceOf( IllegalArgumentException.class );
        }
    }

    @Test
    void transactionStateMustNotPreventIndexUpdatesFromBeingApplied() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        awaitIndexesOnline();
        LongHashSet nodeIds = new LongHashSet();
        LongHashSet relIds = new LongHashSet();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode( LABEL );
                node.setProperty( PROP, "value" );
                Relationship rel = node.createRelationshipTo( node, REL );
                rel.setProperty( PROP, "value" );
                nodeIds.add( node.getId() );
                relIds.add( rel.getId() );

                executor.submit( () ->
                {
                    try ( Transaction forkedTx = db.beginTx() )
                    {
                        Node node2 = forkedTx.createNode( LABEL );
                        node2.setProperty( PROP, "value" );
                        Relationship rel2 = node2.createRelationshipTo( node2, REL );
                        rel2.setProperty( PROP, "value" );
                        nodeIds.add( node2.getId() );
                        relIds.add( rel2.getId() );
                        forkedTx.commit();
                    }
                }).get();
                tx.commit();
            }
            assertQueryFindsIds( db, true, "nodes", "value", nodeIds );
            assertQueryFindsIds( db, false, "rels", "value", relIds );
        }
        finally
        {
            executor.shutdown();
        }
    }

    @Test
    void dropMustNotApplyToRegularSchemaIndexes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).create();
            tx.commit();
        }
        awaitIndexesOnline();
        String schemaIndexName;
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( "call db.indexes()" ) )
            {
                assertTrue( result.hasNext() );
                schemaIndexName = result.next().get( "name" ).toString();
            }
            assertThrows( QueryExecutionException.class, () -> tx.execute( format( DROP, schemaIndexName ) ).close() );
        }
    }

    @Test
    void fulltextIndexMustNotBeAvailableForRegularIndexSeeks()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        String valueToQueryFor = "value to query for";
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            List<Value> values = generateRandomSimpleValues();
            for ( Value value : values )
            {
                tx.createNode( LABEL ).setProperty( PROP, value.asObject() );
            }
            tx.createNode( LABEL ).setProperty( PROP, valueToQueryFor );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Map<String,Object> params = new HashMap<>();
            params.put( "prop", valueToQueryFor );
            try ( Result result = tx.execute( "profile match (n:" + LABEL.name() + ") where n." + PROP + " = $prop return n", params ) )
            {
                assertNoIndexSeeks( result );
            }
            try ( Result result = tx.execute( "cypher 3.5 profile match (n:" + LABEL.name() + ") where n." + PROP + " = $prop return n", params ) )
            {
                assertNoIndexSeeks( result );
            }
            tx.commit();
        }
    }

    @Test
    void fulltextIndexMustNotBeAvailableForRegularIndexSeeksAfterShutDown()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        restartDatabase();
        String valueToQueryFor = "value to query for";
        awaitIndexesOnline();
        try ( Transaction tx = db.beginTx() )
        {
            List<Value> values = generateRandomSimpleValues();
            for ( Value value : values )
            {
                tx.createNode( LABEL ).setProperty( PROP, value.asObject() );
            }
            tx.createNode( LABEL ).setProperty( PROP, valueToQueryFor );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Map<String,Object> params = new HashMap<>();
            params.put( "prop", valueToQueryFor );
            try ( Result result = tx.execute( "profile match (n:" + LABEL.name() + ") where n." + PROP + " = $prop return n", params ) )
            {
                assertNoIndexSeeks( result );
            }
            try ( Result result = tx.execute( "cypher 3.5 profile match (n:" + LABEL.name() + ") where n." + PROP + " = $prop return n", params ) )
            {
                assertNoIndexSeeks( result );
            }
            tx.commit();
        }
    }

    @Test
    void nodeOutputMustBeOrderedByScoreDescending()
    {
        FulltextProcedures.NodeOutput a = new FulltextProcedures.NodeOutput( null, Float.NaN );
        FulltextProcedures.NodeOutput b = new FulltextProcedures.NodeOutput( null, Float.POSITIVE_INFINITY );
        FulltextProcedures.NodeOutput c = new FulltextProcedures.NodeOutput( null, Float.MAX_VALUE );
        FulltextProcedures.NodeOutput d = new FulltextProcedures.NodeOutput( null, 1.0f );
        FulltextProcedures.NodeOutput e = new FulltextProcedures.NodeOutput( null, Float.MIN_NORMAL );
        FulltextProcedures.NodeOutput f = new FulltextProcedures.NodeOutput( null, Float.MIN_VALUE );
        FulltextProcedures.NodeOutput g = new FulltextProcedures.NodeOutput( null, 0.0f );
        FulltextProcedures.NodeOutput h = new FulltextProcedures.NodeOutput( null, -1.0f );
        FulltextProcedures.NodeOutput i = new FulltextProcedures.NodeOutput( null, Float.NEGATIVE_INFINITY );
        FulltextProcedures.NodeOutput[] expectedOrder = new FulltextProcedures.NodeOutput[] {a, b, c, d, e, f, g, h, i};
        FulltextProcedures.NodeOutput[] array = Arrays.copyOf( expectedOrder, expectedOrder.length );

        for ( int counter = 0; counter < 10; counter++ )
        {
            ArrayUtils.shuffle( array );
            Arrays.sort( array );
            assertArrayEquals( expectedOrder, array );
        }
    }

    @Test
    void relationshipOutputMustBeOrderedByScoreDescending()
    {
        FulltextProcedures.RelationshipOutput a = new FulltextProcedures.RelationshipOutput( null, Float.NaN );
        FulltextProcedures.RelationshipOutput b = new FulltextProcedures.RelationshipOutput( null, Float.POSITIVE_INFINITY );
        FulltextProcedures.RelationshipOutput c = new FulltextProcedures.RelationshipOutput( null, Float.MAX_VALUE );
        FulltextProcedures.RelationshipOutput d = new FulltextProcedures.RelationshipOutput( null, 1.0f );
        FulltextProcedures.RelationshipOutput e = new FulltextProcedures.RelationshipOutput( null, Float.MIN_NORMAL );
        FulltextProcedures.RelationshipOutput f = new FulltextProcedures.RelationshipOutput( null, Float.MIN_VALUE );
        FulltextProcedures.RelationshipOutput g = new FulltextProcedures.RelationshipOutput( null, 0.0f );
        FulltextProcedures.RelationshipOutput h = new FulltextProcedures.RelationshipOutput( null, -1.0f );
        FulltextProcedures.RelationshipOutput i = new FulltextProcedures.RelationshipOutput( null, Float.NEGATIVE_INFINITY );
        FulltextProcedures.RelationshipOutput[] expectedOrder = new FulltextProcedures.RelationshipOutput[] {a, b, c, d, e, f, g, h, i};
        FulltextProcedures.RelationshipOutput[] array = Arrays.copyOf( expectedOrder, expectedOrder.length );

        for ( int counter = 0; counter < 10; counter++ )
        {
            ArrayUtils.shuffle( array );
            Arrays.sort( array );
            assertArrayEquals( expectedOrder, array );
        }
    }

    @Test
    void awaitIndexProcedureMustWorkOnIndexNames()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 1000; i++ )
            {
                Node node = tx.createNode( LABEL );
                node.setProperty( PROP, "value" );
                Relationship rel = node.createRelationshipTo( node, REL );
                rel.setProperty( PROP, "value" );
            }
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( DB_AWAIT_INDEX, "nodes" ) ).close();
            tx.execute( format( DB_AWAIT_INDEX, "rels" ) ).close();
            tx.commit();
        }
    }

    @Test
    void mustBePossibleToDropFulltextIndexByNameForWhichNormalIndexExistWithMatchingSchema()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE INDEX FOR (n:Person) ON (n.name)" ).close();
            tx.execute( "call db.index.fulltext.createNodeIndex('nameIndex', ['Person'], ['name'])" ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            // This must not throw:
            tx.execute( "call db.index.fulltext.drop('nameIndex')" ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( single( tx.schema().getIndexes() ).getName() ).isNotEqualTo( "nameIndex" );
            tx.commit();
        }
    }

    @Test
    void fulltextIndexesMustNotPreventNormalSchemaIndexesFromBeingDropped()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "CREATE INDEX FOR (n:Person) ON (n.name)" ).close();
            tx.execute( "call db.index.fulltext.createNodeIndex('nameIndex', ['Person'], ['name'])" ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            // This must not throw:
            tx.execute( "DROP INDEX ON :Person(name)" ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( single( tx.schema().getIndexes() ).getName() ).isEqualTo( "nameIndex" );
            tx.commit();
        }
    }

    @Test
    void creatingNormalIndexWithFulltextProviderMustThrow()
    {
        String providerName = FulltextIndexProviderFactory.DESCRIPTOR.name();
        assertThat( providerName ).isEqualTo( "fulltext-1.0" ); // Sanity check that this test is up to date.

        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "call db.createIndex( \"MyIndex\", ['User'], ['searchableString'], \"" + providerName + "\" );" ).close();
            tx.commit();
        }
        catch ( QueryExecutionException e )
        {
            assertThat( e.getMessage() ).contains(
                    "Could not create index with specified index provider 'fulltext-1.0'. To create fulltext index, please use 'db.index.fulltext" +
                            ".createNodeIndex' or 'db.index.fulltext.createRelationshipIndex'." );
        }

        try ( Transaction tx = db.beginTx() )
        {
            long indexCount = tx.execute( DB_INDEXES ).stream().count();
            assertThat( indexCount ).isEqualTo( 0L );
            tx.commit();
        }
    }

    @Test
    void mustSupportWildcardEndsLikeStartsWith()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        LongHashSet ids = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "abcdef" );
            ids.add( node.getId() );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "abcxyz" );
            ids.add( node.getId() );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "abc*", ids );
    }

    @Test
    void mustSupportWildcardBeginningsLikeEndsWith()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        LongHashSet ids = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "defabc" );
            ids.add( node.getId() );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "xyzabc" );
            ids.add( node.getId() );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "*abc", ids );
    }

    @Test
    void mustSupportWildcardBeginningsAndEndsLikeContains()
    {
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            tx.commit();
        }
        LongHashSet ids = new LongHashSet();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "defabcdef" );
            ids.add( node.getId() );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, "xyzabcxyz" );
            ids.add( node.getId() );
            tx.commit();
        }
        assertQueryFindsIds( db, true, "nodes", "*abc*", ids );
    }

    @Test
    void mustMatchCaseInsensitiveWithStandardAnalyzer()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'A'}))" ).close();
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'B'}))" ).close();
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'C'}))" ).close();
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'b'}))" ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "myindex", asStrList( "Label" ), asStrList( "id" ) + ", {analyzer: 'standard'}" ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( format( QUERY_NODES, "myindex", "A" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 0L ); // The letter 'A' is a stop-word in English, so it is not indexed.
            }
            try ( Result result = tx.execute( format( QUERY_NODES, "myindex", "B" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 2000L ); // Both upper- and lower-case 'B' nodes.
            }
            try ( Result result = tx.execute( format( QUERY_NODES, "myindex", "C" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 1000L ); // We only have upper-case 'C' nodes.
            }
            tx.commit();
        }
    }

    @Test
    void mustMatchCaseInsensitiveWithSimpleAnalyzer()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'A'}))" ).close();
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'B'}))" ).close();
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'C'}))" ).close();
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'b'}))" ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "myindex", asStrList( "Label" ), asStrList( "id" ) + ", {analyzer: 'simple'}" ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( format( QUERY_NODES, "myindex", "A" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 1000L ); // We only have upper-case 'A' nodes.
            }
            try ( Result result = tx.execute( format( QUERY_NODES, "myindex", "B" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 2000L ); // Both upper- and lower-case 'B' nodes.
            }
            try ( Result result = tx.execute( format( QUERY_NODES, "myindex", "C" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 1000L ); // We only have upper-case 'C' nodes.
            }
            tx.commit();
        }
    }

    @Test
    void mustMatchCaseInsensitiveWithDefaultAnalyzer()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'A'}))" ).close();
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'B'}))" ).close();
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'C'}))" ).close();
            tx.execute( "foreach (x in range (1,1000) | create (n:Label {id:'b'}))" ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "myindex", asStrList( "Label" ), asStrList( "id" ) ) ).close();
            tx.commit();
        }
        awaitIndexesOnline();

        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( format( QUERY_NODES, "myindex", "A" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 1000L ); // We only have upper-case 'A' nodes.
            }
            try ( Result result = tx.execute( format( QUERY_NODES, "myindex", "B" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 2000L ); // Both upper- and lower-case 'B' nodes.
            }
            try ( Result result = tx.execute( format( QUERY_NODES, "myindex", "C" ) ) )
            {
                assertThat( result.stream().count() ).isEqualTo( 1000L ); // We only have upper-case 'C' nodes.
            }
            tx.commit();
        }
    }

    @Test
    void makeSureFulltextIndexDoesNotBlockSchemaIndexOnSameSchemaPattern()
    {
        final Label label = Label.label( "label" );
        final String prop = "prop";
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "myindex", asStrList( label.name() ), asStrList( prop ) ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( DB_AWAIT_INDEX, "myindex" ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label ).on( prop ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 2, Iterables.count( tx.schema().getIndexes() ) );
            tx.commit();
        }
    }

    @Test
    void makeSureSchemaIndexDoesNotBlockFulltextIndexOnSameSchemaPattern()
    {
        final Label label = Label.label( "label" );
        final String prop = "prop";
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label ).on( prop ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( NODE_CREATE, "myindex", asStrList( label.name() ), asStrList( prop ) ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( DB_AWAIT_INDEX, "myindex" ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 2, Iterables.count( tx.schema().getIndexes() ) );
            tx.commit();
        }
    }

    @Test
    void shouldNotBePossibleToCreateIndexWithDuplicateProperty()
    {
        Exception e = assertThrows( Exception.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( NODE_CREATE, "myindex", asStrList( "Label" ), asStrList( "id", "id" ) ) );
            }
        } );
        Throwable cause = getRootCause( e );
        assertThat( cause ).isInstanceOf( RepeatedPropertyInSchemaException.class );
    }

    @Test
    void shouldNotBePossibleToCreateIndexWithDuplicateLabel()
    {
        Exception e = assertThrows( Exception.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( NODE_CREATE, "myindex", asStrList( "Label", "Label" ), asStrList( "id" ) ) );
            }
        } );
        Throwable cause = getRootCause( e );
        assertThat( cause ).isInstanceOf( RepeatedLabelInSchemaException.class );
    }

    @Test
    void shouldNotBePossibleToCreateIndexWithDuplicateRelType()
    {
        Exception e = assertThrows( Exception.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( RELATIONSHIP_CREATE, "myindex", asStrList( "RelType", "RelType" ), asStrList( "id" ) ) );
            }
        } );
        Throwable cause = getRootCause( e );
        assertThat( cause ).isInstanceOf( RepeatedRelationshipTypeInSchemaException.class );
    }

    @Test
    void attemptingToIndexOnPropertyUsedForInternalReferenceMustThrow()
    {
        var e = assertThrows( Exception.class, () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.execute( format( NODE_CREATE, "myindex",
                        asStrList( "Label" ),
                        asStrList( LuceneFulltextDocumentStructure.FIELD_ENTITY_ID ) ) )
                        .close();
                tx.commit();
            }
        });
        assertThat( e.getMessage() ).contains( LuceneFulltextDocumentStructure.FIELD_ENTITY_ID );
    }

    @Test
    void fulltextIndexMustWorkAfterRestartWithTxStateChanges()
    {
        // Create node and relationship fulltext indexes
        try ( Transaction tx = db.beginTx() )
        {
            createSimpleNodesIndex( tx );
            createSimpleRelationshipIndex( tx );
            tx.commit();
        }
        awaitIndexesOnline();

        restartDatabase();

        // Query node
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode(); // Tx state changed
            tx.execute( format( QUERY_NODES, "nodes", "*" ) ).close();
            tx.commit();
        }

        // Query relationship
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode(); // Tx state changed
            tx.execute( format( QUERY_RELS, "rels", "*" ) ).close();
            tx.commit();
        }
    }

    @Test
    void relationshipIndexAndDetachDelete() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( RELATIONSHIP_CREATE, "rels", asStrList( REL.name() ), asStrList( PROP ) ) ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
        long nodeId;
        long relId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            nodeId = node.getId();
            Relationship rel = node.createRelationshipTo( node, REL );
            relId = rel.getId();
            rel.setProperty( PROP, "blabla" );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, false, "rels", "blabla", relId );
            tx.execute( "match (n) where id(n) = " + nodeId + " detach delete n" ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, false, "rels", "blabla" );
            tx.commit();
        }

        checkDatabaseConsistency();
    }

    private void checkDatabaseConsistency()
    {
        DatabaseLayout layout = db.databaseLayout();
        controller.restartDbms( b ->
        {
            try
            {
                ConsistencyCheckService cc = new ConsistencyCheckService();
                ConsistencyCheckService.Result result = cc.runFullConsistencyCheck(
                        layout, Config.defaults(), ProgressMonitorFactory.NONE, NullLogProvider.nullLogProvider(), false, ConsistencyFlags.DEFAULT );
                if ( !result.isSuccessful() )
                {
                    Files.lines( result.reportFile().toPath() ).forEach( System.out::println );
                }
                assertTrue( result.isSuccessful() );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            return b;
        } );
    }

    @Test
    public void relationshipIndexAndDetachDeleteWithRestart() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( RELATIONSHIP_CREATE, "rels", asStrList( REL.name() ), asStrList( PROP ) ) ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
        long nodeId;
        long relId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            nodeId = node.getId();
            Relationship rel = node.createRelationshipTo( node, REL );
            relId = rel.getId();
            rel.setProperty( PROP, "blabla" );
            tx.commit();
        }

        restartDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, false, "rels", "blabla", relId );
            tx.execute( "match (n) where id(n) = " + nodeId + " detach delete n" ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, false, "rels", "blabla" );
            tx.commit();
        }

        checkDatabaseConsistency();
    }

    @Test
    public void relationshipIndexAndPropertyRemove() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( RELATIONSHIP_CREATE, "rels", asStrList( REL.name() ), asStrList( PROP ) ) ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
        long relId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo( node, REL );
            relId = rel.getId();
            rel.setProperty( PROP, "blabla" );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, false, "rels", "blabla", relId );
            Relationship rel = tx.getRelationshipById( relId );
            rel.removeProperty( PROP );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, false, "rels", "blabla" );
            tx.commit();
        }

        checkDatabaseConsistency();
    }

    @Test
    public void relationshipIndexAndPropertyRemoveWithRestart() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( RELATIONSHIP_CREATE, "rels", asStrList( REL.name() ), asStrList( PROP ) ) ).close();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
        long relId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode();
            Relationship rel = node.createRelationshipTo( node, REL );
            relId = rel.getId();
            rel.setProperty( PROP, "blabla" );
            tx.commit();
        }

        restartDatabase();

        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, false, "rels", "blabla", relId );
            Relationship rel = tx.getRelationshipById( relId );
            rel.removeProperty( PROP );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertQueryFindsIds( db, false, "rels", "blabla" );
            tx.commit();
        }

        checkDatabaseConsistency();
    }

    @Nested
    class SkipAndLimitNodes
    {
        long topNode;
        long middleNode;
        long bottomNode;

        @BeforeEach
        void setUp()
        {
            try ( Transaction tx = db.beginTx() )
            {
                createSimpleNodesIndex( tx );
                tx.commit();
            }
            awaitIndexesOnline();

            try ( Transaction tx = db.beginTx() )
            {
                Node top = tx.createNode( LABEL );
                Node middle = tx.createNode( LABEL );
                Node bottom = tx.createNode( LABEL );
                top.setProperty( PROP, "zebra zebra zebra zebra donkey" );
                middle.setProperty( PROP, "zebra zebra zebra donkey" );
                bottom.setProperty( PROP, "zebra donkey" );
                topNode = top.getId();
                middleNode = middle.getId();
                bottomNode = bottom.getId();
                tx.commit();
            }
        }

        @Test
        void queryNodesMustApplySkip()
        {
            try ( Transaction tx = db.beginTx();
                  var iterator = tx.execute( "CALL db.index.fulltext.queryNodes('nodes', 'zebra', {skip:1})" ).columnAs( "node" ) )
            {
                assertTrue( iterator.hasNext() );
                assertThat( ((Node) iterator.next()).getId() ).isEqualTo( middleNode );
                assertTrue( iterator.hasNext() );
                assertThat( ((Node) iterator.next()).getId() ).isEqualTo( bottomNode );
                assertFalse( iterator.hasNext() );
                tx.commit();
            }
        }

        @Test
        void queryNodesMustApplyLimit()
        {
            try ( Transaction tx = db.beginTx();
                  var iterator = tx.execute( "CALL db.index.fulltext.queryNodes('nodes', 'zebra', {limit:1})" ).columnAs( "node" ) )
            {
                assertTrue( iterator.hasNext() );
                assertThat( ((Node) iterator.next()).getId() ).isEqualTo( topNode );
                assertFalse( iterator.hasNext() );
                tx.commit();
            }
        }

        @Test
        void queryNodesMustApplySkipAndLimit()
        {
            try ( Transaction tx = db.beginTx();
                  var iterator = tx.execute( "CALL db.index.fulltext.queryNodes('nodes', 'zebra', {skip:1, limit:1})" ).columnAs( "node" ) )
            {
                assertTrue( iterator.hasNext() );
                assertThat( ((Node) iterator.next()).getId() ).isEqualTo( middleNode );
                assertFalse( iterator.hasNext() );
                tx.commit();
            }
        }

        @Test
        void queryNodesWithSkipAndLimitMustIgnoreNodesDeletedInTransaction()
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.getNodeById( topNode ).delete();
                try ( var iterator = tx.execute( "CALL db.index.fulltext.queryNodes('nodes', 'zebra', {skip:1})" ).columnAs( "node" ) )
                {
                    assertTrue( iterator.hasNext() );
                    assertThat( ((Node) iterator.next()).getId() ).isEqualTo( bottomNode ); // Without topNode, middleNode is now the one we skip.
                    assertFalse( iterator.hasNext() );
                }
                try ( var iterator = tx.execute( "CALL db.index.fulltext.queryNodes('nodes', 'zebra', {limit:1})" ).columnAs( "node" ) )
                {
                    assertTrue( iterator.hasNext() );
                    assertThat( ((Node) iterator.next()).getId() ).isEqualTo( middleNode ); // Without topNode, middleNode is now the best match.
                    assertFalse( iterator.hasNext() );
                }
                tx.commit();
            }
        }

        @Test
        void queryNodesWithSkipAndLimitMustIncludeNodesAddedInTransaction()
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode( LABEL );
                long nodeId = node.getId();
                node.setProperty( PROP, "zebra zebra donkey" );
                try ( var iterator = tx.execute( "CALL db.index.fulltext.queryNodes('nodes', 'zebra', {skip:1, limit:2})" ).columnAs( "node" ) )
                {
                    assertTrue( iterator.hasNext() );
                    assertThat( ((Node) iterator.next()).getId() ).isEqualTo( middleNode );
                    assertTrue( iterator.hasNext() );
                    assertThat( ((Node) iterator.next()).getId() ).isEqualTo( nodeId );
                    assertFalse( iterator.hasNext() );
                }
                tx.commit();
            }
        }
    }

    @Nested
    class SkipAndLimitRelationships
    {
        long topRel;
        long middleRel;
        long bottomRel;

        @BeforeEach
        void setUp()
        {
            try ( Transaction tx = db.beginTx() )
            {
                createSimpleRelationshipIndex( tx );
                tx.commit();
            }
            awaitIndexesOnline();

            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode();
                Relationship top = node.createRelationshipTo( node, REL );
                Relationship middle = node.createRelationshipTo( node, REL );
                Relationship bottom = node.createRelationshipTo( node, REL );
                top.setProperty( PROP, "zebra zebra zebra zebra donkey" );
                middle.setProperty( PROP, "zebra zebra zebra donkey" );
                bottom.setProperty( PROP, "zebra donkey" );
                topRel = top.getId();
                middleRel = middle.getId();
                bottomRel = bottom.getId();
                tx.commit();
            }
        }

        @Test
        void queryRelationshipsMustApplySkip()
        {
            try ( Transaction tx = db.beginTx();
                  var iterator = tx.execute( "CALL db.index.fulltext.queryRelationships('rels', 'zebra', {skip:1})" ).columnAs( "relationship" ) )
            {
                assertTrue( iterator.hasNext() );
                assertThat( ((Relationship) iterator.next()).getId() ).isEqualTo( middleRel );
                assertTrue( iterator.hasNext() );
                assertThat( ((Relationship) iterator.next()).getId() ).isEqualTo( bottomRel );
                assertFalse( iterator.hasNext() );
                tx.commit();
            }
        }

        @Test
        void queryRelationshipsMustApplyLimit()
        {
            try ( Transaction tx = db.beginTx();
                  var iterator = tx.execute( "CALL db.index.fulltext.queryRelationships('rels', 'zebra', {limit:1})" ).columnAs( "relationship" ) )
            {
                assertTrue( iterator.hasNext() );
                assertThat( ((Relationship) iterator.next()).getId() ).isEqualTo( topRel );
                assertFalse( iterator.hasNext() );
                tx.commit();
            }
        }

        @Test
        void queryRelationshipsMustApplySkipAndLimit()
        {
            try ( Transaction tx = db.beginTx();
                  var iterator = tx.execute( "CALL db.index.fulltext.queryRelationships('rels', 'zebra', {skip:1, limit:1})" ).columnAs( "relationship" ) )
            {
                assertTrue( iterator.hasNext() );
                assertThat( ((Relationship) iterator.next()).getId() ).isEqualTo( middleRel );
                assertFalse( iterator.hasNext() );
                tx.commit();
            }
        }

        @Test
        void queryRelationshipsWithSkipAndLimitMustIgnoreRelationshipsDeletedInTransaction()
        {
            try ( Transaction tx = db.beginTx() )
            {
                tx.getRelationshipById( topRel ).delete();
                try ( var iterator = tx.execute( "CALL db.index.fulltext.queryRelationships('rels', 'zebra', {skip:1})" ).columnAs( "relationship" ) )
                {
                    assertTrue( iterator.hasNext() );
                    assertThat( ((Relationship) iterator.next()).getId() ).isEqualTo( bottomRel ); // Without topRel, middleRel is now the one we skip.
                    assertFalse( iterator.hasNext() );
                }
                try ( var iterator = tx.execute( "CALL db.index.fulltext.queryRelationships('rels', 'zebra', {limit:1})" ).columnAs( "relationship" ) )
                {
                    assertTrue( iterator.hasNext() );
                    assertThat( ((Relationship) iterator.next()).getId() ).isEqualTo( middleRel ); // Without topRel, middleRel is now the best match.
                    assertFalse( iterator.hasNext() );
                }
                tx.commit();
            }
        }

        @Test
        void queryRelationshipsWithSkipAndLimitMustIncludeRelationshipsAddedInTransaction()
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node = tx.createNode();
                Relationship rel = node.createRelationshipTo( node, REL );
                long relId = rel.getId();
                rel.setProperty( PROP, "zebra zebra donkey" );
                try ( var iterator = tx.execute( "CALL db.index.fulltext.queryRelationships('rels', 'zebra', {skip:1, limit:2})" ).columnAs( "relationship" ) )
                {
                    assertTrue( iterator.hasNext() );
                    assertThat( ((Relationship) iterator.next()).getId() ).isEqualTo( middleRel );
                    assertTrue( iterator.hasNext() );
                    assertThat( ((Relationship) iterator.next()).getId() ).isEqualTo( relId );
                    assertFalse( iterator.hasNext() );
                }
                tx.commit();
            }
        }
    }
}
