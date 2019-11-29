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
package org.neo4j.kernel.api.impl.fulltext;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexLimitation;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.IndexValueCapability;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.ANALYZER;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT;

public class LuceneFulltextIndexTest extends LuceneFulltextTestSupport
{
    private static final String NODE_INDEX_NAME = "nodes";
    private static final String REL_INDEX_NAME = "rels";

    @Test
    public void shouldFindNodeWithString() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }
        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( tx, LABEL, "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( tx, LABEL,
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                            "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "hello", firstID );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "zebra", secondID );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "zedonk", secondID );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "cross", secondID );
        }
    }

    @Test
    public void shouldRepresentPropertyChanges() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( tx, LABEL, "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( tx, LABEL,
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                            "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            setNodeProp( tx, firstID, "Finally! Potato!" );
            setNodeProp( tx, secondID, "This one is a potato farmer." );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "hello" );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "zebra" );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "zedonk" );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "cross" );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "finally", firstID );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "farmer", secondID );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "potato", firstID, secondID );
        }
    }

    @Test
    public void shouldNotFindRemovedNodes() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( tx, LABEL, "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( tx, LABEL,
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                            "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( firstID ).delete();
            tx.getNodeById( secondID ).delete();

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "hello" );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "zebra" );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "zedonk" );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "cross" );
        }
    }

    @Test
    public void shouldNotFindRemovedProperties() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).on( PROP2 ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }
        long firstID;
        long secondID;
        long thirdID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( tx, LABEL, "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( tx, LABEL,
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                            "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            thirdID = createNodeIndexableByPropertyValue( tx, LABEL, "Hello. Hello again." );

            setNodeProp( tx, firstID, "zebra" );
            setNodeProp( tx, secondID, "Hello. Hello again." );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( firstID );
            Node node2 = tx.getNodeById( secondID );
            Node node3 = tx.getNodeById( thirdID );

            node.setProperty( PROP, "tomtar" );
            node.setProperty( PROP2, "tomtar" );

            node2.setProperty( PROP, "tomtar" );
            node2.setProperty( PROP2, "Hello" );

            node3.removeProperty( PROP );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "hello", secondID );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "zebra" );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "zedonk" );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "cross" );
        }
    }

    @Test
    public void shouldOnlyIndexIndexedProperties() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }

        long firstID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( tx, LABEL, "Hello. Hello again." );
            setNodeProp( tx, firstID, PROP2, "zebra" );

            Node node2 = tx.createNode( LABEL );
            node2.setProperty( PROP2, "zebra" );
            node2.setProperty( PROP3, "hello" );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "hello", firstID );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "zebra" );
        }
    }

    @Test
    public void shouldSearchAcrossMultipleProperties() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).on( PROP2 ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }

        long firstID;
        long secondID;
        long thirdID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( tx, LABEL, "Tomtar tomtar oftsat i tomteutstyrsel." );
            secondID = createNodeIndexableByPropertyValue( tx, LABEL, "Olof och Hans" );
            setNodeProp( tx, secondID, PROP2, "och karl" );

            Node node3 = tx.createNode( LABEL );
            thirdID = node3.getId();
            node3.setProperty( PROP2, "Tomtar som inte tomtar ser upp till tomtar som tomtar." );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "tomtar Karl", firstID, secondID, thirdID );
        }
    }

    @Test
    public void shouldOrderResultsBasedOnRelevance() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( "first" ).on( "last" ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }
        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = tx.createNode( LABEL ).getId();
            secondID = tx.createNode( LABEL ).getId();
            thirdID = tx.createNode( LABEL ).getId();
            fourthID = tx.createNode( LABEL ).getId();
            setNodeProp( tx, firstID, "first", "Full" );
            setNodeProp( tx, firstID, "last", "Hanks" );
            setNodeProp( tx, secondID, "first", "Tom" );
            setNodeProp( tx, secondID, "last", "Hunk" );
            setNodeProp( tx, thirdID, "first", "Tom" );
            setNodeProp( tx, thirdID, "last", "Hanks" );
            setNodeProp( tx, fourthID, "first", "Tom Hanks" );
            setNodeProp( tx, fourthID, "last", "Tom Hanks" );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsNodeIdsInOrder( ktx, NODE_INDEX_NAME, "Tom Hanks", fourthID, thirdID, firstID, secondID );
        }
    }

    @Test
    public void shouldDifferentiateNodesAndRelationships() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.schema().indexFor( RELTYPE ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( REL_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
            tx.schema().awaitIndexOnline( REL_INDEX_NAME, 30, TimeUnit.SECONDS );
        }
        long firstNodeID;
        long secondNodeID;
        long firstRelID;
        long secondRelID;
        try ( Transaction tx = db.beginTx() )
        {
            firstNodeID = createNodeIndexableByPropertyValue( tx, LABEL, "Hello. Hello again." );
            secondNodeID = createNodeIndexableByPropertyValue( tx, LABEL,
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                            "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            firstRelID = createRelationshipIndexableByPropertyValue( tx, firstNodeID, secondNodeID, "Hello. Hello again." );
            secondRelID = createRelationshipIndexableByPropertyValue( tx, secondNodeID, firstNodeID, "And now, something completely different" );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "hello", firstNodeID );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "zebra", secondNodeID );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "different" );

            assertQueryFindsIds( ktx, false, REL_INDEX_NAME, "hello", firstRelID );
            assertQueryFindsNothing( ktx, false, REL_INDEX_NAME, "zebra" );
            assertQueryFindsIds( ktx, false, REL_INDEX_NAME, "different", secondRelID );
        }
    }

    @Test
    public void shouldNotReturnNonMatches() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.schema().indexFor( RELTYPE ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( REL_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
            tx.schema().awaitIndexOnline( REL_INDEX_NAME, 30, TimeUnit.SECONDS );
        }
        try ( Transaction tx = db.beginTx() )
        {
            long firstNode = createNodeIndexableByPropertyValue( tx, LABEL, "Hello. Hello again." );
            long secondNode = createNodeWithProperty( tx, LABEL, PROP2,
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                            "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            createRelationshipIndexableByPropertyValue( tx, firstNode, secondNode, "Hello. Hello again." );
            createRelationshipWithProperty( tx, secondNode, firstNode, PROP2,
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                            "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "zebra" );
            assertQueryFindsNothing( ktx, false, REL_INDEX_NAME, "zebra" );
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
            Node node = tx.createNode();
            node.createRelationshipTo( node, RELTYPE );
            node.createRelationshipTo( node, RELTYPE );
            node.createRelationshipTo( node, RELTYPE );

            firstNodeID = createNodeIndexableByPropertyValue( tx, LABEL, "Hello. Hello again." );
            secondNodeID = createNodeIndexableByPropertyValue( tx, LABEL, "This string is slightly shorter than the zebra one" );
            firstRelID = createRelationshipIndexableByPropertyValue( tx, firstNodeID, secondNodeID, "Goodbye" );
            secondRelID = createRelationshipIndexableByPropertyValue( tx, secondNodeID, firstNodeID, "And now, something completely different" );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.schema().indexFor( RELTYPE ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( REL_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
            tx.schema().awaitIndexOnline( REL_INDEX_NAME, 30, TimeUnit.SECONDS );
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "hello", firstNodeID );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "string", secondNodeID );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "goodbye" );
            assertQueryFindsNothing( ktx, true, NODE_INDEX_NAME, "different" );

            assertQueryFindsNothing( ktx, false, REL_INDEX_NAME, "hello" );
            assertQueryFindsNothing( ktx, false, REL_INDEX_NAME, "string" );
            assertQueryFindsIds( ktx, false, REL_INDEX_NAME, "goodbye", firstRelID );
            assertQueryFindsIds( ktx, false, REL_INDEX_NAME, "different", secondRelID );
        }
    }

    @Test
    public void shouldBeAbleToUpdateAndQueryAfterIndexChange() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }

        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( tx, LABEL, "thing" );

            secondID = tx.createNode( LABEL ).getId();
            setNodeProp( tx, secondID, PROP2, "zebra" );

            thirdID = createNodeIndexableByPropertyValue( tx, LABEL, "zebra" );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "thing zebra", firstID, thirdID );
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().getIndexByName( NODE_INDEX_NAME ).drop();
            tx.schema().indexFor( LABEL ).on( PROP2 ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }

        try ( Transaction tx = db.beginTx() )
        {
            setNodeProp( tx, firstID, PROP2, "thing" );

            fourthID = tx.createNode( LABEL ).getId();
            setNodeProp( tx, fourthID, PROP2, "zebra" );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "thing zebra", firstID, secondID, fourthID );
        }
    }

    @Test
    public void shouldBeAbleToDropAndReadIndex() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }

        long firstID;
        long secondID;

        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( tx, LABEL, "thing" );

            secondID = createNodeIndexableByPropertyValue( tx, LABEL, "zebra" );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().getIndexByName( NODE_INDEX_NAME ).drop();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "thing zebra", firstID, secondID );
        }
    }

    @Test
    public void completeConfigurationMustInjectMissingConfigurations() throws Exception
    {
        int label;
        int propertyKey;
        try ( Transaction tx = db.beginTx() )
        {
            createNodeIndexableByPropertyValue( tx, LABEL, "bla" );
            tx.commit();
        }
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            label = tx.tokenRead().nodeLabel( LABEL.name() );
            propertyKey = tx.tokenRead().propertyKey( PROP );
            tx.success();
        }

        IndexConfig indexConfig = IndexConfig.with( EVENTUALLY_CONSISTENT, Values.booleanValue( true ) );
        FulltextSchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, new int[]{label}, new int[]{propertyKey} );

        IndexProviderDescriptor providerDescriptor = indexProvider.getProviderDescriptor();
        IndexDescriptor descriptor = indexProvider.completeConfiguration( IndexPrototype.forSchema( schema, providerDescriptor )
                .withName( "index_1" ).withIndexConfig( indexConfig ).materialise( 1 ) );

        assertThat( (Value) descriptor.getIndexConfig().get( ANALYZER ) ).isEqualTo( Values.stringValue( "standard-no-stop-words" ) );
        assertThat( (Value) descriptor.getIndexConfig().get( EVENTUALLY_CONSISTENT ) ).isEqualTo( Values.booleanValue( true ) );
        assertThat( asList( descriptor.getCapability().limitations() ) ).containsExactly( IndexLimitation.EVENTUALLY_CONSISTENT );
    }

    @Test
    public void completeConfigurationMustNotOverwriteExistingConfiguration()
    {
        IndexConfig indexConfig = IndexConfig.with( "A", Values.stringValue( "B" ) );
        FulltextSchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, new int[]{1}, new int[]{1} );
        IndexProviderDescriptor providerDescriptor = indexProvider.getProviderDescriptor();
        IndexDescriptor descriptor = indexProvider.completeConfiguration( IndexPrototype.forSchema( schema, providerDescriptor )
                .withName( "index_1" ).materialise( 1 ) ).withIndexConfig( indexConfig );
        assertEquals( Values.stringValue( "B" ), descriptor.getIndexConfig().get( "A" ) );
    }

    @Test
    public void completeConfigurationMustBeIdempotent()
    {
        FulltextSchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, new int[]{1}, new int[]{1} );
        IndexProviderDescriptor providerDescriptor = indexProvider.getProviderDescriptor();
        IndexDescriptor onceCompleted = indexProvider.completeConfiguration( IndexPrototype.forSchema( schema, providerDescriptor )
                .withName( "index_1" ).materialise( 1 ) );
        IndexDescriptor twiceCompleted = indexProvider.completeConfiguration( onceCompleted );
        assertEquals( onceCompleted.getIndexConfig(), twiceCompleted.getIndexConfig() );
    }

    @Test
    public void mustAssignCapabilitiesToDescriptorsThatHaveNone()
    {
        FulltextSchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, new int[]{1}, new int[]{1} );
        IndexProviderDescriptor providerDescriptor = indexProvider.getProviderDescriptor();
        IndexDescriptor completed = indexProvider.completeConfiguration( IndexPrototype.forSchema( schema, providerDescriptor )
                .withName( "index_1" ).materialise( 1 ) );
        assertNotEquals( completed.getCapability(), IndexCapability.NO_CAPABILITY );
        completed = completed.withIndexCapability( IndexCapability.NO_CAPABILITY );
        completed = indexProvider.completeConfiguration( completed );
        assertNotEquals( completed.getCapability(), IndexCapability.NO_CAPABILITY );
    }

    @Test
    public void mustNotOverwriteExistingCapabilities()
    {
        IndexCapability capability = new IndexCapability()
        {
            @Override
            public IndexOrder[] orderCapability( ValueCategory... valueCategories )
            {
                return new IndexOrder[0];
            }

            @Override
            public IndexValueCapability valueCapability( ValueCategory... valueCategories )
            {
                return IndexValueCapability.NO;
            }
        };
        FulltextSchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, new int[]{1}, new int[]{1} );
        IndexProviderDescriptor providerDescriptor = indexProvider.getProviderDescriptor();
        IndexDescriptor index = IndexPrototype.forSchema( schema, providerDescriptor )
                .withName( "index_1" ).materialise( 1 ).withIndexCapability( capability );
        IndexDescriptor completed = indexProvider.completeConfiguration( index );
        assertSame( capability, completed.getCapability() );
    }

    @Test
    public void fulltextIndexMustNotAnswerCoreApiIndexQueries()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = createNodeIndexableByPropertyValue( tx, LABEL, 1 );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.findNode( LABEL, PROP, 1 );
            assertThat( node.getId() ).isEqualTo( nodeId );
        }
    }

    @Test
    public void fulltextIndexMustNotAnswerCoreApiCompositeIndexQueries()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP ).on( PROP2 ).withIndexType( IndexType.FULLTEXT ).withName( NODE_INDEX_NAME ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( NODE_INDEX_NAME, 30, TimeUnit.SECONDS );
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( LABEL );
            node.setProperty( PROP, 1 );
            node.setProperty( PROP2, 2 );
            nodeId = node.getId();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            try ( ResourceIterator<Node> nodes = tx.findNodes( LABEL, PROP, 1, PROP2, 2 ) )
            {
                assertTrue( nodes.hasNext() );
                Node node = nodes.next();
                assertThat( node.getId() ).isEqualTo( nodeId );
                assertFalse( nodes.hasNext() );
            }

            try ( ResourceIterator<Node> nodes = tx.findNodes( LABEL, PROP2, 2, PROP, 1 ) )
            {
                assertTrue( nodes.hasNext() );
                Node node = nodes.next();
                assertThat( node.getId() ).isEqualTo( nodeId );
                assertFalse( nodes.hasNext() );
            }

            try ( ResourceIterator<Node> nodes = tx.findNodes( LABEL, PROP, 1 ) )
            {
                assertTrue( nodes.hasNext() );
                Node node = nodes.next();
                assertThat( node.getId() ).isEqualTo( nodeId );
                assertFalse( nodes.hasNext() );
            }

            try ( ResourceIterator<Node> nodes = tx.findNodes( LABEL, PROP2, 2 ) )
            {
                assertTrue( nodes.hasNext() );
                Node node = nodes.next();
                assertThat( node.getId() ).isEqualTo( nodeId );
                assertFalse( nodes.hasNext() );
            }
        }
    }
}
