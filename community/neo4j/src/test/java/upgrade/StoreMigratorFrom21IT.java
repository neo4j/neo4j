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
package upgrade;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@SuppressWarnings( "unchecked" )
public class StoreMigratorFrom21IT
{
    @Test
    public void mustMendDuplicatePropertiesWhenUpgradingFromVersion21() throws IOException
    {
        // The rules:
        // If an index is present, all duplicates should be removed and the property set to the value in the index
        // If an index is not present, the property should be set to the value of the last duplicate in the property
        // chain, all duplicates except the first should be removed
        // If an index is not present, the first property in the duplicate chain should be kept for the users
        // benefit, moved to a special property value, `__DUPLICATE_<propkey>`
        //
        // This is the broken store that we are upgrading:
        //
        //   (#0:Label { keyA: "actual", keyA: "phony!", keyA: "phony!" })
        //   (#1 { keyA: "actual", keyA: "actual", keyA: "actual" })
        //   (#2 { keyA: "actual", keyB: "actual", keyC: "actual" })
        //   (#0)-[#0:REL { keyA: "actual", keyA: "actual", keyA: "actual" }]->(#1)
        //   (#2)-[#1:REL { keyA: "actual", keyB: "actual", keyC: "actual" }]->(#0)
        //
        // And this is what we want to end up with, after upgrading:
        //
        //   (#0:Label { keyA: "actual" })
        //   (#1 { keyA: "actual", __DUPLICATE_keyA: "actual" })
        //   (#2 { keyA: "actual", keyB: "actual", keyC: "actual" })
        //   (#0)-[#0:REL { keyA: "actual", __DUPLICATE_keyA: "actual" }]->(#1)
        //   (#2)-[#1:REL { keyA: "actual", keyB: "actual", keyC: "actual" }]->(#0)

        File dir = MigrationTestUtils.find21FormatStoreDirectoryWithDuplicateProperties( storeDir.directory() );

        GraphDatabaseBuilder builder =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir.getAbsolutePath() ).setConfig(
                        GraphDatabaseSettings.allow_store_upgrade, "true" );
        GraphDatabaseService database = builder.newGraphDatabase();

        // Upgrade is now completed. Verify the contents:
        NeoStoreProvider provider = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency(
                NeoStoreProvider
                        .class );
        NeoStore store = provider.evaluate();
        NodeStore nodeStore = store.getNodeStore();
        RelationshipStore relStore = store.getRelationshipStore();
        PropertyStore propertyStore = store.getPropertyStore();

        // Verify that the properties appear correct to the outside world:
        try ( Transaction ignore = database.beginTx() )
        {
            verifyPropertiesEqual( database.getNodeById( 0 ),
                    Pair.of( "keyA", "actual" ) );
            verifyPropertiesEqual( database.getNodeById( 1 ),
                    Pair.of( "keyA", "actual" ),
                    Pair.of( "__DUPLICATE_keyA", "actual" ) );
            verifyPropertiesEqual( database.getNodeById( 2 ),
                    Pair.of( "keyA", "actual" ),
                    Pair.of( "keyB", "actual" ),
                    Pair.of( "keyC", "actual" ) );
            verifyPropertiesEqual( database.getRelationshipById( 0 ),
                    Pair.of( "keyA", "actual" ),
                    Pair.of( "__DUPLICATE_keyA", "actual" ) );
            verifyPropertiesEqual( database.getRelationshipById( 1 ),
                    Pair.of( "keyA", "actual" ),
                    Pair.of( "keyB", "actual" ),
                    Pair.of( "keyC", "actual" ) );
        }

        // Verify that there are no two properties on the entities, that have the same key:
        // (This is important because the verification above cannot tell if we have two keys with the same value)
        verifyNoDuplicatePropertyKeys( propertyStore, nodeStore.getRecord( 0 ).getNextProp() );
        verifyNoDuplicatePropertyKeys( propertyStore, nodeStore.getRecord( 1 ).getNextProp() );
        verifyNoDuplicatePropertyKeys( propertyStore, nodeStore.getRecord( 2 ).getNextProp() );
        verifyNoDuplicatePropertyKeys( propertyStore, relStore.getRecord( 0 ).getNextProp() );
        verifyNoDuplicatePropertyKeys( propertyStore, relStore.getRecord( 1 ).getNextProp() );

        database.shutdown();
    }

    private void verifyNoDuplicatePropertyKeys( PropertyStore propertyStore, long firstPropertyId )
    {
        HashSet<Integer> propertiesInUse = new HashSet<>();
        long nextPropertyId = firstPropertyId;
        while ( nextPropertyId != -1 )
        {
            PropertyRecord propertyRecord = propertyStore.getRecord( nextPropertyId );
            nextPropertyId = propertyRecord.getNextProp();
            if ( !propertyRecord.inUse() )
            {
                continue;
            }

            for ( PropertyBlock propertyBlock : propertyRecord.getPropertyBlocks() )
            {
                if ( !propertiesInUse.add( propertyBlock.getKeyIndexId() ) )
                {
                    throw new AssertionError( String.format(
                            "Found a duplicate property in use: %s", propertyBlock.getKeyIndexId()
                    ) );
                }
            }
        }
    }

    private void verifyPropertiesEqual( PropertyContainer entity, Pair<String,String>... expectedProperties )
    {
        HashMap<String,String> properties = new HashMap<>();
        for ( String propertyKey : entity.getPropertyKeys() )
        {
            properties.put( propertyKey, (String) entity.getProperty( propertyKey ) );
        }
        assertThat( properties, is( IteratorUtil.asMap( Arrays.asList( expectedProperties ) ) ) );
    }

    @Rule
    public final TargetDirectory.TestDirectory storeDir = TargetDirectory.testDirForTest( getClass() );
}

/*
The test data store has been generated by running the following program in the 2.1 code base:

package examples;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreProvider;

public class CreateStoreWithDuplicateProperties
{
    public static void main( String[] args )
    {
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        GraphDatabaseService db = factory.newEmbeddedDatabase( "21-with-duplicate-properties" );
        GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        Label label = DynamicLabel.label( "Label" );
        DynamicRelationshipType relationshipType = DynamicRelationshipType.withName( "REL" );
        String actualValue = "actual";
        String phonyValue = "phony!";
        String keyA = "keyA";
        String keyB = "keyB";
        String keyC = "keyC";

        try ( Transaction transaction = db.beginTx() )
        {
            db.schema().indexFor( label ).on( keyA ).create();
            transaction.success();
        }

        long nodeIndexedId;
        long nodeNotIndexedId;
        long relId;
        try ( Transaction transaction = db.beginTx() )
        {
            // Two nodes and a relationship will have duplicate properties.
            // One node will be in the index, the other not.
            // Another node and another relationship will be fine.
            Node nodeA = db.createNode( label );
            nodeA.setProperty( keyA, actualValue );
            nodeA.setProperty( keyB, phonyValue );
            nodeA.setProperty( keyC, phonyValue );
            nodeIndexedId = nodeA.getId();

            Node nodeB = db.createNode();
            nodeB.setProperty( keyA, actualValue );
            nodeB.setProperty( keyB, actualValue );
            nodeB.setProperty( keyC, actualValue );
            nodeNotIndexedId = nodeB.getId();

            Relationship relA = nodeA.createRelationshipTo( nodeB, relationshipType );
            relA.setProperty( keyA, actualValue );
            relA.setProperty( keyB, actualValue );
            relA.setProperty( keyC, actualValue );
            relId = relA.getId();

            Node nodeC = db.createNode();
            nodeC.setProperty( keyA, actualValue );
            nodeC.setProperty( keyB, actualValue );
            nodeC.setProperty( keyC, actualValue );
            Relationship relB = nodeC.createRelationshipTo( nodeA, relationshipType );
            relB.setProperty( keyA, actualValue );
            relB.setProperty( keyB, actualValue );
            relB.setProperty( keyC, actualValue );
            transaction.success();
        }

        // (#0:Label { keyA: "actual", keyA: "phony!", keyA: "phony!" })
        // (#1 { keyA: "actual", keyA: "actual", keyA: "actual" })
        // (#2 { keyA: "actual", keyB: "actual", keyC: "actual" })
        // (#0)-[#0:REL { keyA: "actual", keyA: "actual", keyA: "actual" }]->(#1)
        // (#2)-[#1:REL { keyA: "actual", keyB: "actual", keyC: "actual" }]->(#0)

        DependencyResolver resolver = api.getDependencyResolver();
        NeoStoreProvider neoStoreProvider = resolver.resolveDependency( NeoStoreProvider.class );
        NeoStore neoStore = neoStoreProvider.evaluate();
        PropertyKeyTokenStore propertyKeyTokenStore = neoStore.getPropertyKeyTokenStore();

        Token tokenA = findPropertyKeyTokenFor( propertyKeyTokenStore, keyA );

        NodeStore nodeStore = neoStore.getNodeStore();
        RelationshipStore relationshipStore = neoStore.getRelationshipStore();
        PropertyStore propertyStore = neoStore.getPropertyStore();

        NodeRecord indexedNode = nodeStore.getRecord( nodeIndexedId );
        NodeRecord nonIndexedNode = nodeStore.getRecord( nodeNotIndexedId );
        long nextPropIndexedNode = indexedNode.getNextProp();
        long nextPropNonIndexedNode = nonIndexedNode.getNextProp();
        long nextPropRelationship = relationshipStore.getRecord( relId ).getNextProp();
        introduceDuplicatesOfProperty( tokenA, propertyStore, nextPropIndexedNode );
        introduceDuplicatesOfProperty( tokenA, propertyStore, nextPropNonIndexedNode );
        introduceDuplicatesOfProperty( tokenA, propertyStore, nextPropRelationship );

        db.shutdown();
    }

    private static Token findPropertyKeyTokenFor( PropertyKeyTokenStore propertyKeyTokenStore, String key )
    {
        long highestPossibleIdInUse = propertyKeyTokenStore.getHighestPossibleIdInUse();
        for ( int i = 0; i < highestPossibleIdInUse; i++ )
        {
            Token token = propertyKeyTokenStore.getToken( i );
            if ( token.name().equals( key ) )
            {
                return token;
            }
        }
        return null;
    }

    private static void introduceDuplicatesOfProperty( Token tokenA, PropertyStore propertyStore, long nextProp )
    {
        while ( nextProp != -1 )
        {
            PropertyRecord propertyRecord = propertyStore.getRecord( nextProp );

            for ( PropertyBlock block : propertyRecord.getPropertyBlocks() )
            {
                block.setKeyIndexId( tokenA.id() );
            }

            propertyStore.updateRecord( propertyRecord );
            nextProp = propertyRecord.getNextProp();
        }
    }
}
 */
