/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext.lucene;

import org.junit.Test;

import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.test.Race;

import static org.junit.Assert.assertEquals;
import static org.neo4j.storageengine.api.EntityType.NODE;
import static org.neo4j.storageengine.api.EntityType.RELATIONSHIP;

public class LuceneFulltextUpdaterTest extends LuceneFulltextTestSupport
{
    @Test
    public void shouldFindNodeWithString() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );
        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", "hello", false, firstID );
            assertExactQueryFindsIds( "nodes", "zebra", false, secondID );
            assertExactQueryFindsIds( "nodes", "zedonk", false, secondID );
            assertExactQueryFindsIds( "nodes", "cross", false, secondID );
        }
    }

    @Test
    public void shouldFindNodeWithNumber() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( 1 );
            secondID = createNodeIndexableByPropertyValue( 234 );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", "1", false, firstID );
            assertExactQueryFindsIds( "nodes", "234", false, secondID );
        }
    }

    @Test
    public void shouldFindNodeWithBoolean() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( true );
            secondID = createNodeIndexableByPropertyValue( false );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", "true", false, firstID );
            assertExactQueryFindsIds( "nodes", "false", false, secondID );
        }
    }

    @Test
    public void shouldFindNodeWithArrays() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        long thirdID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( new String[]{"hello", "I", "live", "here"} );
            secondID = createNodeIndexableByPropertyValue( new int[]{1, 27, 48} );
            thirdID = createNodeIndexableByPropertyValue( new int[]{1, 2, 48} );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", "live", false, firstID );
            assertExactQueryFindsIds( "nodes", "27", false, secondID );
            assertExactQueryFindsIds( "nodes", Arrays.asList( "1", "2" ), false, secondID, thirdID );
        }
    }

    @Test
    public void shouldRepresentPropertyChanges() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            setNodeProp( firstID, "Finally! Potato!" );
            setNodeProp( secondID, "This one is a potato farmer." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsNothing( "nodes", "hello" );
            assertExactQueryFindsNothing( "nodes", "zebra" );
            assertExactQueryFindsNothing( "nodes", "zedonk" );
            assertExactQueryFindsNothing( "nodes", "cross" );
            assertExactQueryFindsIds( "nodes", "finally", false, firstID );
            assertExactQueryFindsIds( "nodes", "farmer", false, secondID );
            assertExactQueryFindsIds( "nodes", "potato", false, firstID, secondID );
        }
    }

    @Test
    public void shouldNotFindRemovedNodes() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( firstID ).delete();
            db.getNodeById( secondID ).delete();

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsNothing( "nodes", "hello" );
            assertExactQueryFindsNothing( "nodes", "zebra" );
            assertExactQueryFindsNothing( "nodes", "zedonk" );
            assertExactQueryFindsNothing( "nodes", "cross" );
        }
    }

    @Test
    public void shouldNotFindRemovedProperties() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], "prop", "prop2" );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );
        long firstID;
        long secondID;
        long thirdID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            thirdID = createNodeIndexableByPropertyValue( "Hello. Hello again." );

            setNodeProp( firstID, "zebra" );
            setNodeProp( secondID, "Hello. Hello again." );

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

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", "hello", false, secondID );
            assertExactQueryFindsNothing( "nodes", "zebra" );
            assertExactQueryFindsNothing( "nodes", "zedonk" );
            assertExactQueryFindsNothing( "nodes", "cross" );
        }
    }

    @Test
    public void shouldOnlyIndexIndexedProperties() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            setNodeProp( firstID, "prop2", "zebra" );

            Node node2 = db.createNode();
            node2.setProperty( "prop2", "zebra" );
            node2.setProperty( "prop3", "hello" );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", "hello", false, firstID );
            assertExactQueryFindsNothing( "nodes", "zebra" );
        }
    }

    @Test
    public void shouldSearchAcrossMultipleProperties() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], "prop", "prop2" );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        long thirdID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Tomtar tomtar oftsat i tomteutstyrsel." );
            secondID = createNodeIndexableByPropertyValue( "Olof och Hans" );
            setNodeProp( secondID, "prop2", "och karl" );

            Node node3 = db.createNode();
            thirdID = node3.getId();
            node3.setProperty( "prop2", "Tomtar som inte tomtar ser upp till tomtar som tomtar." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", Arrays.asList( "tomtar", "karl" ), false, firstID, secondID, thirdID );
        }
    }

    @Test
    public void shouldOrderResultsBasedOnRelevance() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], "first", "last" );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );
        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = db.createNode().getId();
            secondID = db.createNode().getId();
            thirdID = db.createNode().getId();
            fourthID = db.createNode().getId();
            setNodeProp( firstID, "first", "Full" );
            setNodeProp( firstID, "last", "Hanks" );
            setNodeProp( secondID, "first", "Tom" );
            setNodeProp( secondID, "last", "Hunk" );
            setNodeProp( thirdID, "first", "Tom" );
            setNodeProp( thirdID, "last", "Hanks" );
            setNodeProp( fourthID, "first", "Tom Hanks" );
            setNodeProp( fourthID, "last", "Tom Hanks" );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIdsInOrder( "nodes", Arrays.asList( "Tom", "Hanks" ), false, fourthID, thirdID, firstID, secondID );
        }
    }

    @Test
    public void shouldDifferentiateNodesAndRelationships() throws Exception
    {
        IndexDescriptor nodes = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        IndexDescriptor rels = fulltextAccessor.indexDescriptorFor( "rels", RELATIONSHIP, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( nodes );
            stmt.schemaWriteOperations().nonSchemaIndexCreate( rels );
            transaction.success();
        }
        await( nodes );
        await( rels );
        long firstNodeID;
        long secondNodeID;
        long firstRelID;
        long secondRelID;
        try ( Transaction tx = db.beginTx() )
        {
            firstNodeID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondNodeID = createNodeIndexableByPropertyValue( "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            firstRelID = createRelationshipIndexableByPropertyValue( firstNodeID, secondNodeID, "Hello. Hello again." );
            secondRelID = createRelationshipIndexableByPropertyValue( secondNodeID, firstNodeID, "And now, something completely different" );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", "hello", false, firstNodeID );
            assertExactQueryFindsIds( "nodes", "zebra", false, secondNodeID );
            assertExactQueryFindsNothing( "nodes", "different" );

            assertExactQueryFindsIds( "rels", "hello", false, firstRelID );
            assertExactQueryFindsNothing( "rels", "zebra" );
            assertExactQueryFindsIds( "rels", "different", false, secondRelID );
        }
    }

    @Test
    public void fuzzyQueryShouldBeFuzzy() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertFuzzyQueryFindsIds( "nodes", "hella", false, firstID );
            assertFuzzyQueryFindsIds( "nodes", "zebre", false, secondID );
            assertFuzzyQueryFindsIds( "nodes", "zedink", false, secondID );
            assertFuzzyQueryFindsIds( "nodes", "cruss", false, secondID );
            assertExactQueryFindsNothing( "nodes", "hella" );
            assertExactQueryFindsNothing( "nodes", "zebre" );
            assertExactQueryFindsNothing( "nodes", "zedink" );
            assertExactQueryFindsNothing( "nodes", "cruss" );
        }
    }

    @Test
    public void fuzzyQueryShouldReturnExactMatchesFirst() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "zibre" );
            secondID = createNodeIndexableByPropertyValue( "zebrae" );
            thirdID = createNodeIndexableByPropertyValue( "zebra" );
            fourthID = createNodeIndexableByPropertyValue( "zibra" );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertFuzzyQueryFindsIdsInOrder( "nodes", "zebra", true, thirdID, secondID, fourthID, firstID );
        }
    }

    @Test
    public void shouldNotReturnNonMatches() throws Exception
    {
        IndexDescriptor nodes = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        IndexDescriptor rels = fulltextAccessor.indexDescriptorFor( "rels", RELATIONSHIP, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( nodes );
            stmt.schemaWriteOperations().nonSchemaIndexCreate( rels );
            transaction.success();
        }
        await( nodes );
        await( rels );
        try ( Transaction tx = db.beginTx() )
        {
            long firstNode = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            long secondNode = createNodeWithProperty( "prop2", "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            createRelationshipIndexableByPropertyValue( firstNode, secondNode, "Hello. Hello again." );
            createRelationshipWithProperty( secondNode, firstNode, "prop2",
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                            "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsNothing( "nodes", "zebra" );
            assertExactQueryFindsNothing( "rels", "zebra" );
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
            // skip a few rel ids, so the ones we work with are different from the node ids, just in case.
            Node node = db.createNode();
            node.createRelationshipTo( node, RELTYPE );
            node.createRelationshipTo( node, RELTYPE );
            node.createRelationshipTo( node, RELTYPE );

            firstNodeID = createNodeIndexableByPropertyValue( "Hello. Hello again." );
            secondNodeID = createNodeIndexableByPropertyValue( "This string is slightly shorter than the zebra one" );
            firstRelID = createRelationshipIndexableByPropertyValue( firstNodeID, secondNodeID, "Goodbye" );
            secondRelID = createRelationshipIndexableByPropertyValue( secondNodeID, firstNodeID, "And now, something completely different" );

            tx.success();
        }

        IndexDescriptor nodes = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        IndexDescriptor rels = fulltextAccessor.indexDescriptorFor( "rels", RELATIONSHIP, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( nodes );
//            stmt.schemaWriteOperations().nonSchemaIndexCreate( rels );
            transaction.success();
        }
        await( nodes );
//        await( rels );
        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", "hello", false, firstNodeID );
            assertExactQueryFindsIds( "nodes", "string", false, secondNodeID );
            assertExactQueryFindsNothing( "nodes", "goodbye" );
            assertExactQueryFindsNothing( "nodes", "different" );

//            assertExactQueryFindsNothing( "rels", "hello" );
//            assertExactQueryFindsNothing( "rels", "string" );
//            assertExactQueryFindsIds( "rels", "goodbye", false, firstRelID );
//            assertExactQueryFindsIds( "rels", "different", false, secondRelID );
        }
    }

    @Test
    public void shouldReturnMatchesThatContainLuceneSyntaxCharacters() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );
        String[] luceneSyntaxElements = {"+", "-", "&&", "||", "!", "(", ")", "{", "}", "[", "]", "^", "\"", "~", "*", "?", ":", "\\"};

        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = db.createNodeId();
            tx.success();
        }

        for ( String elm : luceneSyntaxElements )
        {
            setNodeProp( nodeId, "Hello" + elm + " How are you " + elm + "today?" );

            try ( Transaction tx = db.beginTx() )
            {
                assertExactQueryFindsIds( "nodes", "Hello" + elm, false, nodeId );
                assertExactQueryFindsIds( "nodes", elm + "today", false, nodeId );
            }
        }
    }

    @Test
    public void exactMatchAllShouldOnlyReturnStuffThatMatchesAll() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], "first", "last" );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        long fifthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = db.createNode().getId();
            secondID = db.createNode().getId();
            thirdID = db.createNode().getId();
            fourthID = db.createNode().getId();
            fifthID = db.createNode().getId();
            setNodeProp( firstID, "first", "Full" );
            setNodeProp( firstID, "last", "Hanks" );
            setNodeProp( secondID, "first", "Tom" );
            setNodeProp( secondID, "last", "Hunk" );
            setNodeProp( thirdID, "first", "Tom" );
            setNodeProp( thirdID, "last", "Hanks" );
            setNodeProp( fourthID, "first", "Tom Hanks" );
            setNodeProp( fourthID, "last", "Tom Hanks" );
            setNodeProp( fifthID, "last", "Tom Hanks" );
            setNodeProp( fifthID, "first", "Morgan" );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", Arrays.asList( "Tom", "Hanks" ), true, thirdID, fourthID, fifthID );
        }
    }

    @Test
    public void fuzzyMatchAllShouldOnlyReturnStuffThatKindaMatchesAll() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], "first", "last" );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        long fifthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = db.createNode().getId();
            secondID = db.createNode().getId();
            thirdID = db.createNode().getId();
            fourthID = db.createNode().getId();
            fifthID = db.createNode().getId();
            setNodeProp( firstID, "first", "Christian" );
            setNodeProp( firstID, "last", "Hanks" );
            setNodeProp( secondID, "first", "Tom" );
            setNodeProp( secondID, "last", "Hungarian" );
            setNodeProp( thirdID, "first", "Tom" );
            setNodeProp( thirdID, "last", "Hunk" );
            setNodeProp( fourthID, "first", "Tim" );
            setNodeProp( fourthID, "last", "Hanks" );
            setNodeProp( fifthID, "last", "Tom Hanks" );
            setNodeProp( fifthID, "first", "Morgan" );

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertFuzzyQueryFindsIds( "nodes", Arrays.asList( "Tom", "Hanks" ), true, thirdID, fourthID, fifthID );
        }
    }

    @Test
    public void shouldBeAbleToUpdateAndQueryAfterIndexChange() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "thing" );

            secondID = db.createNode().getId();
            setNodeProp( secondID, "prop2", "zebra" );

            thirdID = createNodeIndexableByPropertyValue( "zebra" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", Arrays.asList( "thing", "zebra" ), false, firstID, thirdID );
        }

        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().indexDrop( descriptor );
            descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], "prop2" );
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        try ( Transaction tx = db.beginTx() )
        {
            setNodeProp( firstID, "prop2", "thing" );

            fourthID = db.createNode().getId();
            setNodeProp( fourthID, "prop2", "zebra" );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", Arrays.asList( "thing", "zebra" ), false, firstID, secondID, fourthID );
        }
    }

    @Test
    public void shouldBeAbleToDropAndReaddIndex() throws Exception
    {
        IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        long firstID;
        long secondID;

        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( "thing" );

            secondID = createNodeIndexableByPropertyValue( "zebra" );
            tx.success();
        }

        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().indexDrop( descriptor );
            transaction.success();
        }
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        try ( Transaction tx = db.beginTx() )
        {
            assertExactQueryFindsIds( "nodes", Arrays.asList( "thing", "zebra" ), false, firstID, secondID );
        }
    }

    @Test
    public void concurrentUpdatesAndIndexChangesShouldResultInValidState() throws Throwable
    {
        final IndexDescriptor descriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], PROP );
        try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
        {
            stmt.schemaWriteOperations().nonSchemaIndexCreate( descriptor );
            transaction.success();
        }
        await( descriptor );

        int aliceThreads = 10;
        int bobThreads = 10;
        int nodesCreatedPerThread = 10;
        Race race = new Race();
        Runnable aliceWork = () ->
        {
            for ( int i = 0; i < nodesCreatedPerThread; i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    createNodeIndexableByPropertyValue( "alice" );
                    tx.success();
                }
            }
        };
        Runnable changeConfig = () ->
        {
            try
            {
                IndexDescriptor newDescriptor = fulltextAccessor.indexDescriptorFor( "nodes", NODE, new String[0], "otherProp" );
                try ( Transaction transaction = db.beginTx(); Statement stmt = db.statement() )
                {
                    stmt.schemaWriteOperations().indexDrop( descriptor );
                    stmt.schemaWriteOperations().nonSchemaIndexCreate( newDescriptor );
                    transaction.success();
                }
                await( newDescriptor );
            }
            catch ( Exception e )
            {
                throw new AssertionError( e );
            }
        };
        Runnable bobWork = () ->
        {
            for ( int i = 0; i < nodesCreatedPerThread; i++ )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    createNodeWithProperty( "otherProp", "bob" );
                    tx.success();
                }
            }
        };
        race.addContestants( aliceThreads, aliceWork );
        race.addContestant( changeConfig );
        race.addContestants( bobThreads, bobWork );
        race.go();

        try ( Transaction tx = db.beginTx() )
        {
            PrimitiveLongIterator bob = fulltextAccessor.query( "nodes", "bob" );
            assertEquals( bobThreads * nodesCreatedPerThread, PrimitiveLongCollections.count( bob ) );
            PrimitiveLongIterator alice = fulltextAccessor.query( "nodes", "alice" );
            assertEquals( 0, PrimitiveLongCollections.count( alice ) );
        }
    }
}
