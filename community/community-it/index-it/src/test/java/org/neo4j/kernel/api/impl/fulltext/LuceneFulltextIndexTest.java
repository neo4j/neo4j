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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
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
import org.neo4j.values.storable.ValueCategory;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.kernel.impl.index.schema.FulltextIndexSettingsKeys.ANALYZER;
import static org.neo4j.kernel.impl.index.schema.FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT;

public class LuceneFulltextIndexTest extends LuceneFulltextTestSupport
{
    private static final String NODE_INDEX_NAME = "nodes";
    private static final String REL_INDEX_NAME = "rels";

    @Test
    public void shouldFindNodeWithString() throws Exception
    {
        IndexDescriptor index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, PROP );
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );
        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( LABEL, "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( LABEL, "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
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
        IndexDescriptor index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, PROP );
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( LABEL, "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( LABEL, "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            setNodeProp( firstID, "Finally! Potato!" );
            setNodeProp( secondID, "This one is a potato farmer." );

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
        IndexDescriptor index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, PROP );
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( LABEL, "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( LABEL, "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );

            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( firstID ).delete();
            db.getNodeById( secondID ).delete();

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
        IndexDescriptor index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, "prop", "prop2" );
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );
        long firstID;
        long secondID;
        long thirdID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( LABEL, "Hello. Hello again." );
            secondID = createNodeIndexableByPropertyValue( LABEL, "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                    "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            thirdID = createNodeIndexableByPropertyValue( LABEL, "Hello. Hello again." );

            setNodeProp( firstID, "zebra" );
            setNodeProp( secondID, "Hello. Hello again." );

            tx.commit();
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
        IndexDescriptor index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, PROP );
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );

        long firstID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( LABEL, "Hello. Hello again." );
            setNodeProp( firstID, "prop2", "zebra" );

            Node node2 = db.createNode( LABEL );
            node2.setProperty( "prop2", "zebra" );
            node2.setProperty( "prop3", "hello" );

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
        IndexDescriptor index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, "prop", "prop2" );
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        long thirdID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( LABEL, "Tomtar tomtar oftsat i tomteutstyrsel." );
            secondID = createNodeIndexableByPropertyValue( LABEL, "Olof och Hans" );
            setNodeProp( secondID, "prop2", "och karl" );

            Node node3 = db.createNode( LABEL );
            thirdID = node3.getId();
            node3.setProperty( "prop2", "Tomtar som inte tomtar ser upp till tomtar som tomtar." );

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
        IndexDescriptor index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, "first", "last" );
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );
        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = db.createNode( LABEL ).getId();
            secondID = db.createNode( LABEL ).getId();
            thirdID = db.createNode( LABEL ).getId();
            fourthID = db.createNode( LABEL ).getId();
            setNodeProp( firstID, "first", "Full" );
            setNodeProp( firstID, "last", "Hanks" );
            setNodeProp( secondID, "first", "Tom" );
            setNodeProp( secondID, "last", "Hunk" );
            setNodeProp( thirdID, "first", "Tom" );
            setNodeProp( thirdID, "last", "Hanks" );
            setNodeProp( fourthID, "first", "Tom Hanks" );
            setNodeProp( fourthID, "last", "Tom Hanks" );

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
        SchemaDescriptor nodes = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, PROP );
        SchemaDescriptor rels = indexProvider.schemaFor( RELATIONSHIP, new String[]{RELTYPE.name()}, indexConfig, PROP );
        IndexDescriptor nodesIndex;
        IndexDescriptor relsIndex;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            nodesIndex = tx.schemaWrite().indexCreate( nodes, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            relsIndex = tx.schemaWrite().indexCreate( rels, FulltextIndexProviderFactory.DESCRIPTOR.name(), REL_INDEX_NAME );
            tx.success();
        }
        await( nodesIndex );
        await( relsIndex );
        long firstNodeID;
        long secondNodeID;
        long firstRelID;
        long secondRelID;
        try ( Transaction tx = db.beginTx() )
        {
            firstNodeID = createNodeIndexableByPropertyValue( LABEL, "Hello. Hello again." );
            secondNodeID = createNodeIndexableByPropertyValue( LABEL,
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                            "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            firstRelID = createRelationshipIndexableByPropertyValue( firstNodeID, secondNodeID, "Hello. Hello again." );
            secondRelID = createRelationshipIndexableByPropertyValue( secondNodeID, firstNodeID, "And now, something completely different" );

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
        SchemaDescriptor nodes = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, PROP );
        SchemaDescriptor rels = indexProvider.schemaFor( RELATIONSHIP, new String[]{RELTYPE.name()}, indexConfig, PROP );
        IndexDescriptor nodesIndex;
        IndexDescriptor relsIndex;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            nodesIndex = tx.schemaWrite().indexCreate( nodes, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            relsIndex = tx.schemaWrite().indexCreate( rels, FulltextIndexProviderFactory.DESCRIPTOR.name(), REL_INDEX_NAME );
            tx.success();
        }
        await( nodesIndex );
        await( relsIndex );
        try ( Transaction tx = db.beginTx() )
        {
            long firstNode = createNodeIndexableByPropertyValue( LABEL, "Hello. Hello again." );
            long secondNode = createNodeWithProperty( LABEL, "prop2",
                    "A zebroid (also zedonk, zorse, zebra mule, zonkey, and zebmule) is the offspring of any " +
                            "cross between a zebra and any other equine: essentially, a zebra hybrid." );
            createRelationshipIndexableByPropertyValue( firstNode, secondNode, "Hello. Hello again." );
            createRelationshipWithProperty( secondNode, firstNode, "prop2",
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
            Node node = db.createNode();
            node.createRelationshipTo( node, RELTYPE );
            node.createRelationshipTo( node, RELTYPE );
            node.createRelationshipTo( node, RELTYPE );

            firstNodeID = createNodeIndexableByPropertyValue( LABEL, "Hello. Hello again." );
            secondNodeID = createNodeIndexableByPropertyValue( LABEL, "This string is slightly shorter than the zebra one" );
            firstRelID = createRelationshipIndexableByPropertyValue( firstNodeID, secondNodeID, "Goodbye" );
            secondRelID = createRelationshipIndexableByPropertyValue( secondNodeID, firstNodeID, "And now, something completely different" );

            tx.commit();
        }

        SchemaDescriptor nodes = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, PROP );
        SchemaDescriptor rels = indexProvider.schemaFor( RELATIONSHIP, new String[]{RELTYPE.name()}, indexConfig, PROP );
        IndexDescriptor nodesIndex;
        IndexDescriptor relsIndex;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            nodesIndex = tx.schemaWrite().indexCreate( nodes, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            relsIndex = tx.schemaWrite().indexCreate( rels, FulltextIndexProviderFactory.DESCRIPTOR.name(), REL_INDEX_NAME );
            tx.success();
        }
        await( nodesIndex );
        await( relsIndex );
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
        IndexDescriptor index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, PROP );
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;
        long thirdID;
        long fourthID;
        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( LABEL, "thing" );

            secondID = db.createNode( LABEL ).getId();
            setNodeProp( secondID, "prop2", "zebra" );

            thirdID = createNodeIndexableByPropertyValue( LABEL, "zebra" );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            assertQueryFindsIds( ktx, true, NODE_INDEX_NAME, "thing zebra", firstID, thirdID );
        }

        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            SchemaDescriptor descriptor = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, "prop2" );
            tx.schemaWrite().indexDrop( index );
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );

        try ( Transaction tx = db.beginTx() )
        {
            setNodeProp( firstID, "prop2", "thing" );

            fourthID = db.createNode( LABEL ).getId();
            setNodeProp( fourthID, "prop2", "zebra" );
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
        SchemaDescriptor descriptor = indexProvider.schemaFor( NODE, new String[]{LABEL.name()}, indexConfig, PROP );
        IndexDescriptor index;
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );

        long firstID;
        long secondID;

        try ( Transaction tx = db.beginTx() )
        {
            firstID = createNodeIndexableByPropertyValue( LABEL, "thing" );

            secondID = createNodeIndexableByPropertyValue( LABEL, "zebra" );
            tx.commit();
        }

        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            tx.schemaWrite().indexDrop( index );
            tx.success();
        }
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            index = tx.schemaWrite().indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), NODE_INDEX_NAME );
            tx.success();
        }
        await( index );

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
            createNodeIndexableByPropertyValue( LABEL, "bla" );
            tx.commit();
        }
        try ( KernelTransactionImplementation tx = getKernelTransaction() )
        {
            label = tx.tokenRead().nodeLabel( LABEL.name() );
            propertyKey = tx.tokenRead().propertyKey( PROP );
            tx.success();
        }

        IndexConfig indexConfig = IndexConfig.empty().withIfAbsent( EVENTUALLY_CONSISTENT, Values.booleanValue( true ) );
        FulltextSchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, indexConfig, new int[]{label}, new int[]{propertyKey} );

        IndexProviderDescriptor providerDescriptor = indexProvider.getProviderDescriptor();
        IndexDescriptor descriptor = indexProvider.completeConfiguration( IndexPrototype.forSchema( schema, providerDescriptor )
                .withName( "index_1" ).materialise( 1 ) );

        assertThat( descriptor.schema().getIndexConfig().get( ANALYZER ), is( Values.stringValue( "standard-no-stop-words" ) ) );
        assertThat( descriptor.schema().getIndexConfig().get( EVENTUALLY_CONSISTENT ), is( Values.booleanValue( true ) ) );
        assertThat( asList( descriptor.getCapability().limitations() ), contains( IndexLimitation.EVENTUALLY_CONSISTENT ) );
    }

    @Test
    public void completeConfigurationMustNotOverwriteExistingConfiguration()
    {
        IndexConfig indexConfig = IndexConfig.empty().withIfAbsent( "A", Values.stringValue( "B" ) );
        FulltextSchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, indexConfig, new int[]{1}, new int[]{1} );
        IndexProviderDescriptor providerDescriptor = indexProvider.getProviderDescriptor();
        IndexDescriptor descriptor = indexProvider.completeConfiguration( IndexPrototype.forSchema( schema, providerDescriptor )
                .withName( "index_1" ).materialise( 1 ) );
        assertEquals( Values.stringValue( "B" ), descriptor.schema().getIndexConfig().get( "A" ) );
    }

    @Test
    public void completeConfigurationMustBeIdempotent()
    {
        FulltextSchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, IndexConfig.empty(), new int[]{1}, new int[]{1} );
        IndexProviderDescriptor providerDescriptor = indexProvider.getProviderDescriptor();
        IndexDescriptor onceCompleted = indexProvider.completeConfiguration( IndexPrototype.forSchema( schema, providerDescriptor )
                .withName( "index_1" ).materialise( 1 ) );
        IndexDescriptor twiceCompleted = indexProvider.completeConfiguration( onceCompleted );
        assertEquals( onceCompleted.schema().getIndexConfig(), twiceCompleted.schema().getIndexConfig() );
    }

    @Test
    public void mustAssignCapabilitiesToDescriptorsThatHaveNone()
    {
        FulltextSchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, IndexConfig.empty(), new int[]{1}, new int[]{1} );
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
        FulltextSchemaDescriptor schema = SchemaDescriptor.fulltext( NODE, IndexConfig.empty(), new int[]{1}, new int[]{1} );
        IndexProviderDescriptor providerDescriptor = indexProvider.getProviderDescriptor();
        IndexDescriptor index = IndexPrototype.forSchema( schema, providerDescriptor )
                .withName( "index_1" ).materialise( 1 ).withIndexCapability( capability );
        IndexDescriptor completed = indexProvider.completeConfiguration( index );
        assertSame( capability, completed.getCapability() );
    }
}
