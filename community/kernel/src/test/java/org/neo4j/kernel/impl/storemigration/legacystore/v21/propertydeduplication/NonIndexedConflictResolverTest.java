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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication.PropertyDeduplicatorTestUtil.findTokenFor;
import static org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication.PropertyDeduplicatorTestUtil.replacePropertyKey;

public class NonIndexedConflictResolverTest
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( IndexLookupTest.class );
    private GraphDatabaseAPI api;
    private PropertyKeyTokenStore propertyKeyTokenStore;
    private PropertyStore propertyStore;
    private Token tokenA;
    private NodeStore nodeStore;
    private long nodeIdA;
    private long nodeIdB;
    private long nodeIdC;

    @Before
    public void setUp()
    {
        api = dbRule.getGraphDatabaseAPI();

        String propKeyA = "keyA";
        String propKeyB = "keyB";
        String propKeyC = "keyC";
        String propKeyD = "keyD";
        String propKeyE = "keyE";

        try ( Transaction transaction = api.beginTx() )
        {
            Node nodeA = api.createNode();
            nodeA.setProperty( propKeyA, "value" );
            nodeA.setProperty( propKeyB, "value" );
            nodeIdA = nodeA.getId();

            Node nodeB = api.createNode();
            nodeB.setProperty( propKeyA, "value" );
            nodeB.setProperty( propKeyB, "value" );
            nodeIdB = nodeB.getId();

            Node nodeC = api.createNode();
            nodeC.setProperty( propKeyA, "longer val" );
            nodeC.setProperty( propKeyB, "longer val" );
            nodeC.setProperty( propKeyC, "longer val" );
            nodeC.setProperty( propKeyD, "longer val" );
            nodeC.setProperty( propKeyE, "longer val" );
            nodeIdC = nodeC.getId();

            transaction.success();
        }

        DependencyResolver resolver = api.getDependencyResolver();
        NeoStoresSupplier neoStoresSupplier = resolver.resolveDependency( NeoStoresSupplier.class );
        NeoStores neoStores = neoStoresSupplier.get();
        nodeStore = neoStores.getNodeStore();
        propertyStore = neoStores.getPropertyStore();
        propertyKeyTokenStore = neoStores.getPropertyKeyTokenStore();

        tokenA = findTokenFor( propertyKeyTokenStore, propKeyA );
        Token tokenB = findTokenFor( propertyKeyTokenStore, propKeyB );
        Token tokenC = findTokenFor( propertyKeyTokenStore, propKeyC );
        Token tokenD = findTokenFor( propertyKeyTokenStore, propKeyD );
        Token tokenE = findTokenFor( propertyKeyTokenStore, propKeyE );

        replacePropertyKey( propertyStore, nodeStore.getRecord( nodeIdA ), tokenB, tokenA );
        replacePropertyKey( propertyStore, nodeStore.getRecord( nodeIdB ), tokenB, tokenA );

        NodeRecord nodeRecordC = nodeStore.getRecord( nodeIdC );
        replacePropertyKey( propertyStore, nodeRecordC, tokenB, tokenA );
        replacePropertyKey( propertyStore, nodeRecordC, tokenC, tokenA );
        replacePropertyKey( propertyStore, nodeRecordC, tokenD, tokenA );
        replacePropertyKey( propertyStore, nodeRecordC, tokenE, tokenA );
    }

    @Test
    public void shouldRenameDuplicatedPropertyKeysOnANode() throws Exception
    {
        // Given
        NonIndexedConflictResolver resolver = new NonIndexedConflictResolver( propertyKeyTokenStore, propertyStore );

        List<DuplicateCluster> clusters = new ArrayList<>();
        long propertyId = addDuplicateCluster( nodeIdA, clusters );

        // When
        resolver.visited( 0, clusters );

        // Then
        Token duplicateTokenA = findTokenFor( propertyKeyTokenStore, "__DUPLICATE_keyA_1" );
        assertNotNull( duplicateTokenA );

        Set<Integer> propertyKeyIdsA = collectPropertyKeyIds( propertyId );
        assertThat( propertyKeyIdsA, contains( tokenA.id(), duplicateTokenA.id() ) );
    }

    private long addDuplicateCluster( long nodeId, List<DuplicateCluster> clusters )
    {
        DuplicateCluster cluster = new DuplicateCluster( tokenA.id() );
        long propertyId = nodeStore.getRecord( nodeId ).getNextProp();
        long headPropertyId = propertyId;
        while ( propertyId != Record.NO_NEXT_PROPERTY.intValue() )
        {
            cluster.add( propertyId );
            propertyId = propertyStore.getRecord( propertyId ).getNextProp();
        }
        clusters.add( cluster );
        return headPropertyId;
    }

    @Test
    public void shouldReuseExistingPropertyKeyTokensWhenThatHaveAlreadyCreatedOnPreviousNodes() throws IOException
    {
        // Given
        NonIndexedConflictResolver resolver = new NonIndexedConflictResolver( propertyKeyTokenStore, propertyStore );
        List<DuplicateCluster> clusterListA = new ArrayList<>();
        long propertyIdA = addDuplicateCluster( nodeIdA, clusterListA );

        List<DuplicateCluster> clusterListB = new ArrayList<>();
        long propertyIdB = addDuplicateCluster( nodeIdB, clusterListB );

        // When
        resolver.visited( 0, clusterListA );
        resolver.visited( 0, clusterListB );

        // Then
        Token duplicateTokenA = findTokenFor( propertyKeyTokenStore, "__DUPLICATE_keyA_1" );
        assertNotNull( duplicateTokenA );

        Set<Integer> propertyKeyIdsA = collectPropertyKeyIds( propertyIdA );
        assertThat( propertyKeyIdsA, contains( tokenA.id(), duplicateTokenA.id() ) );

        Set<Integer> propertyKeyIdsB = collectPropertyKeyIds( propertyIdB );
        assertThat( propertyKeyIdsB, contains( tokenA.id(), duplicateTokenA.id() ) );
    }

    @Test
    public void shouldCreateNewPropertyKeyTokenWhenItIsNotCreatedOnPreviousNodes() throws IOException
    {
        // Given
        NonIndexedConflictResolver resolver = new NonIndexedConflictResolver( propertyKeyTokenStore, propertyStore );
        List<DuplicateCluster> clusterListA = new ArrayList<>();
        long propertyIdA = addDuplicateCluster( nodeIdA, clusterListA );

        List<DuplicateCluster> clusterListC = new ArrayList<>();
        long propertyIdC = addDuplicateCluster( nodeIdC, clusterListC );

        // When
        resolver.visited( 0, clusterListA );
        resolver.visited( 0, clusterListC );

        // Then
        Token duplicateTokenA1 = findTokenFor( propertyKeyTokenStore, "__DUPLICATE_keyA_1" );
        Token duplicateTokenA2 = findTokenFor( propertyKeyTokenStore, "__DUPLICATE_keyA_2" );
        Token duplicateTokenA3 = findTokenFor( propertyKeyTokenStore, "__DUPLICATE_keyA_3" );
        Token duplicateTokenA4 = findTokenFor( propertyKeyTokenStore, "__DUPLICATE_keyA_4" );
        assertNotNull( duplicateTokenA1 );
        assertNotNull( duplicateTokenA2 );
        assertNotNull( duplicateTokenA3 );
        assertNotNull( duplicateTokenA4 );

        Set<Integer> propertyKeyIdsA = collectPropertyKeyIds( propertyIdA );
        assertThat( propertyKeyIdsA, contains( tokenA.id(), duplicateTokenA1.id() ) );

        Set<Integer> propertyKeyIdsC = collectPropertyKeyIds( propertyIdC );
        assertThat( propertyKeyIdsC, contains(
                tokenA.id(), duplicateTokenA1.id(), duplicateTokenA2.id(),
                duplicateTokenA3.id(), duplicateTokenA4.id() ) );
    }

    private Set<Integer> collectPropertyKeyIds( long propertyId )
    {
        Set<Integer> result = new HashSet<>();
        while ( propertyId != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord record = propertyStore.getRecord( propertyId );
            for ( PropertyBlock propertyBlock : record )
            {
                int propertyKeyId = propertyBlock.getKeyIndexId();
                assertTrue( result.add( propertyKeyId ) );
            }
            propertyId = record.getNextProp();
        }
        return result;
    }
}
