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
package org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication.PropertyDeduplicatorTestUtil.findTokenFor;

public class IndexLookupTest
{
    @ClassRule
    public static EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( IndexLookupTest.class );

    private static GraphDatabaseAPI api;
    private static IndexLookup indexLookup;

    private static long indexedNode;
    private static long notIndexedNode;
    private static long usedLabelId;
    private static long notUsedLabelId;

    private static int notUsedPropertyId;
    private static int usedPropertyId;
    private static String indexedNodePropertyValue;
    private static String notIndexedNodePropertyValue;

    @BeforeClass
    public static void setUp()
    {
        api = dbRule.getGraphDatabaseAPI();

        String notUsedIndexPropKey = "notUsed";
        String usedIndexPropKey = "used";

        Label usedLabel = DynamicLabel.label( "UsedLabel" );
        Label notUsedLabel = DynamicLabel.label( "NotUsedLabel" );

        try ( Transaction transaction = api.beginTx() )
        {
            api.schema().indexFor( usedLabel ).on( usedIndexPropKey ).create();
            transaction.success();
        }

        try ( Transaction transaction = api.beginTx() )
        {
            api.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            indexedNodePropertyValue = "value1";
            notIndexedNodePropertyValue = "value2";

            Node nodeA = api.createNode( usedLabel );
            nodeA.setProperty( usedIndexPropKey, indexedNodePropertyValue );
            nodeA.setProperty( notUsedIndexPropKey, notIndexedNodePropertyValue );
            indexedNode = nodeA.getId();

            Node nodeB = api.createNode( notUsedLabel );
            nodeB.setProperty( usedIndexPropKey, notIndexedNodePropertyValue );
            nodeB.setProperty( notUsedIndexPropKey, indexedNodePropertyValue );
            notIndexedNode = nodeB.getId();

            transaction.success();
        }

        DependencyResolver resolver = api.getDependencyResolver();
        NeoStoresSupplier neoStoresSupplier = resolver.resolveDependency( NeoStoresSupplier.class );
        NeoStores neoStores = neoStoresSupplier.get();
        SchemaStore schemaStore = neoStores.getSchemaStore();
        SchemaIndexProvider schemaIndexProvider = resolver.resolveDependency( SchemaIndexProvider.class );
        indexLookup = new IndexLookup( schemaStore, schemaIndexProvider );

        LabelTokenStore labelTokenStore = neoStores.getLabelTokenStore();
        notUsedLabelId = findTokenFor( labelTokenStore, notUsedLabel.name() ).id();
        usedLabelId = findTokenFor( labelTokenStore, usedLabel.name() ).id();

        PropertyKeyTokenStore propertyKeyTokenStore = neoStores.getPropertyKeyTokenStore();
        notUsedPropertyId = findTokenFor( propertyKeyTokenStore, notUsedIndexPropKey ).id();
        usedPropertyId = findTokenFor( propertyKeyTokenStore, usedIndexPropKey ).id();
    }

    @Test
    public void getAnyIndexOrNullMustReturnNullWhenThereIsNoSuchIndex() throws Exception
    {
        // Testing:
        // 1. The label is indexed but the property key is not
        // 2. Both the label and the property key are not indexed
        IndexLookup.Index index =
                indexLookup.getAnyIndexOrNull( new long[]{ usedLabelId, notUsedLabelId }, notUsedPropertyId );
        assertThat( index, nullValue() );

        // Testing:
        // 3. The label is not indexed but the property key is.
        index = indexLookup.getAnyIndexOrNull( new long[]{ notUsedLabelId }, usedPropertyId );
        assertThat( index, nullValue() );
    }

    @Test
    public void getAnyIndexOrNullMustReturnAnyRelevantIndex() throws Exception
    {
        IndexLookup.Index index =
                indexLookup.getAnyIndexOrNull( new long[]{ usedLabelId, notUsedLabelId }, usedPropertyId );
        assertThat( index, notNullValue() );
    }

    @Test
    public void containsMustReturnTrueForAnIndexedNodeAndPropertyValue() throws Exception
    {
        IndexLookup.Index index =
                indexLookup.getAnyIndexOrNull( new long[]{ usedLabelId, notUsedLabelId }, usedPropertyId );
        assertTrue( index.contains( indexedNode, indexedNodePropertyValue ) );
    }

    @Test
    public void containsMustReturnFalseWhenNodeIsNotIndexed() throws Exception
    {
        IndexLookup.Index index =
                indexLookup.getAnyIndexOrNull( new long[]{ usedLabelId, notUsedLabelId }, usedPropertyId );
        assertFalse( index.contains( notIndexedNode, indexedNodePropertyValue ) );
        assertFalse( index.contains( notIndexedNode, notIndexedNodePropertyValue ) );
    }
    
    @Test
    public void containsMustReturnFalseWhenTheValueIsNotIndexed() throws Exception
    {
        IndexLookup.Index index =
                indexLookup.getAnyIndexOrNull( new long[]{usedLabelId, notUsedLabelId}, usedPropertyId );
        assertFalse( index.contains( indexedNode, notIndexedNodePropertyValue ) );
    }
}
