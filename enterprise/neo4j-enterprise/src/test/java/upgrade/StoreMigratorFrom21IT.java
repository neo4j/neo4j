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
package upgrade;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@SuppressWarnings( "unchecked" )
public class StoreMigratorFrom21IT
{
    @Test
    public void mustMendDuplicatePropertiesWhenUpgradingFromVersion21() throws Exception
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
        //   (#2:Label { keyA: "real1", keyA: "phony", keyA: "phony", keyD: "real2", keyD: "phony", keyD: "phony" })
        //   (#3 { keyA: "real1", keyA: "phony", keyA: "phony", keyD: "real2", keyD: "phony", keyD: "phony" })
        //   (#4 { keyA: "actual", keyB: "actual", keyC: "actual" })
        //   (#0)-[#0:REL { keyA: "actual", keyA: "actual", keyA: "actual" }]->(#1)
        //   (#0)-[#1:REL { keyA: "real1", keyA: "phony", keyA: "phony",
        //                  keyD: "real2", keyE: "phony", keyF: "phony" }]->(#1)
        //   (#2)-[#2:REL { keyA: "actual", keyB: "actual", keyC: "actual" }]->(#0)
        //
        // And this is what we want to end up with, after upgrading:
        //
        //   (#0:Label { keyA: "actual" })
        //   (#1 { keyA: "actual", __DUPLICATE_keyA: "actual" })
        //   (#2:Label { keyA: "real1", keyD: "real2" })
        //   (#3 { keyA: "real1", __DUPLICATE_keyA_1: "real1", __DUPLICATE_keyA_2: "real1",
        //         keyD: "real2", __DUPLICATE_keyD_1: "real2", __DUPLICATE_keyD_2: "real2" })
        //   (#4 { keyA: "actual", keyB: "actual", keyC: "actual" })
        //   (#0)-[#0:REL { keyA: "actual", __DUPLICATE_keyA: "actual" }]->(#1)
        //   (#0)-[#1:REL { keyA: "real1", __DUPLICATE_keyA_1: "real1", __DUPLICATE_keyA_2: "real1",
        //                  keyD: "real2", __DUPLICATE_keyD_1: "real2", __DUPLICATE_keyD_2: "real2" }]->(#1)
        //   (#2)-[#2:REL { keyA: "actual", keyB: "actual", keyC: "actual" }]->(#0)

        File dir = MigrationTestUtils.find21FormatStoreDirectoryWithDuplicateProperties( storeDir.directory() );

        GraphDatabaseBuilder builder =
                new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir.getAbsolutePath() ).setConfig(
                        GraphDatabaseSettings.allow_store_upgrade, "true" );
        GraphDatabaseService database = builder.newGraphDatabase();
        database.shutdown();
        ConsistencyCheckService service = new ConsistencyCheckService();

        ConsistencyCheckService.Result result = service.runFullConsistencyCheck(
                dir.getAbsoluteFile(), new Config(), ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false );
        assertTrue( result.isSuccessful() );

        database = builder.newGraphDatabase();
        // Upgrade is now completed. Verify the contents:
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        NeoStoresSupplier supplier = dependencyResolver.resolveDependency( NeoStoresSupplier.class );
        NeoStores store = supplier.get();
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
                    Pair.of( "__DUPLICATE_keyA_1", "actual" ),
                    Pair.of( "__DUPLICATE_keyA_2", "actual" ));
            verifyPropertiesEqual( database.getNodeById( 2 ),
                    Pair.of( "keyA", "real1" ),
                    Pair.of( "keyD", "real2" ) );
            verifyPropertiesEqual( database.getNodeById( 3 ),
                    Pair.of( "keyA", "real1" ),
                    Pair.of( "__DUPLICATE_keyA_1", "real1" ),
                    Pair.of( "__DUPLICATE_keyA_2", "real1" ),
                    Pair.of( "keyD", "real2" ),
                    Pair.of( "__DUPLICATE_keyD_1", "real2" ),
                    Pair.of( "__DUPLICATE_keyD_2", "real2" ) );
            verifyPropertiesEqual( database.getNodeById( 4 ),
                    Pair.of( "keyA", "actual" ),
                    Pair.of( "keyB", "actual" ),
                    Pair.of( "keyC", "actual" ) );
            verifyPropertiesEqual( database.getRelationshipById( 0 ),
                    Pair.of( "keyA", "actual" ),
                    Pair.of( "__DUPLICATE_keyA_1", "actual" ),
                    Pair.of( "__DUPLICATE_keyA_2", "actual" ));
            verifyPropertiesEqual( database.getRelationshipById( 1 ),
                    Pair.of( "keyA", "real1" ),
                    Pair.of( "__DUPLICATE_keyA_1", "real1" ),
                    Pair.of( "__DUPLICATE_keyA_2", "real1" ),
                    Pair.of( "keyD", "real2" ),
                    Pair.of( "__DUPLICATE_keyD_1", "real2" ),
                    Pair.of( "__DUPLICATE_keyD_2", "real2" ) );
            verifyPropertiesEqual( database.getRelationshipById( 2 ),
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

            for ( PropertyBlock propertyBlock : propertyRecord )
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
        Map<String, String> properties = (Map) entity.getAllProperties();
        assertThat( properties, is( IteratorUtil.asMap( Arrays.asList( expectedProperties ) ) ) );
    }

    @Rule
    public final TargetDirectory.TestDirectory storeDir = TargetDirectory.testDirForTest( getClass() );
}

/*
The test data store has been generated by running the following program in the 2.1 code base:

package examples;

import java.io.File;
import java.io.IOException;

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
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreProvider;
import org.neo4j.kernel.impl.util.FileUtils;

public class CreateStoreWithDuplicateProperties
{
    public static void main( String[] args ) throws IOException
    {
        String path = "21-with-duplicate-properties";
        FileUtils.deleteRecursively( new File( path ) );
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        GraphDatabaseService db = factory.newEmbeddedDatabase( path );
        GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        Label label = DynamicLabel.label( "Label" );
        DynamicRelationshipType relationshipType = DynamicRelationshipType.withName( "REL" );
        String actualValue = "actual";
        String phonyValue = "phony!";
        String keyA = "keyA";
        String keyB = "keyB";
        String keyC = "keyC";
        String keyD = "keyD";
        String keyE = "keyE";
        String keyF = "keyF";

        try ( Transaction transaction = db.beginTx() )
        {
            db.schema().indexFor( label ).on( keyA ).create();
            db.schema().indexFor( label ).on( keyD ).create();
            transaction.success();
        }

        long nodeIndexedSingleId;
        long nodeNotIndexedSingleId;
        long nodeIndexedMultiId;
        long nodeNotIndexedMultiId;
        long relSingleId;
        long relMultiId;
        try ( Transaction transaction = db.beginTx() )
        {
            // Four nodes and two relationships will have duplicate properties.
            // Two nodes will be in the index, the others not.
            // An indexed and a non-indexed node will have multiple distinct duplicates.
            // Likewise, one of the relationships will have multiple distinct duplicates.
            // Another node and another relationship will be fine.

            // Indexed single duplicate
            Node nodeA = db.createNode( label );
            nodeA.setProperty( keyA, actualValue );
            nodeA.setProperty( keyB, phonyValue );
            nodeA.setProperty( keyC, phonyValue );
            nodeIndexedSingleId = nodeA.getId();

            // Non-indexed single duplicate
            Node nodeB = db.createNode();
            nodeB.setProperty( keyA, actualValue );
            nodeB.setProperty( keyB, actualValue );
            nodeB.setProperty( keyC, actualValue );
            nodeNotIndexedSingleId = nodeB.getId();

            // Single duplicate
            Relationship relA = nodeA.createRelationshipTo( nodeB, relationshipType );
            relA.setProperty( keyA, actualValue );
            relA.setProperty( keyB, actualValue );
            relA.setProperty( keyC, actualValue );
            relSingleId = relA.getId();

            // Indexed multiple duplicates
            Node nodeC = db.createNode( label );
            nodeC.setProperty( keyA, "real1" );
            nodeC.setProperty( keyB, "phony" );
            nodeC.setProperty( keyC, "phony" );
            nodeC.setProperty( keyD, "real2" );
            nodeC.setProperty( keyE, "phony" );
            nodeC.setProperty( keyF, "phony" );
            nodeIndexedMultiId = nodeC.getId();

            // Non-indexed multiple duplicate
            Node nodeD = db.createNode();
            nodeD.setProperty( keyA, "real1" );
            nodeD.setProperty( keyB, "real1" );
            nodeD.setProperty( keyC, "real1" );
            nodeD.setProperty( keyD, "real2" );
            nodeD.setProperty( keyE, "real2" );
            nodeD.setProperty( keyF, "real2" );
            nodeNotIndexedMultiId = nodeD.getId();

            // Multiple duplicates
            Relationship relB = nodeA.createRelationshipTo( nodeB, relationshipType );
            relB.setProperty( keyA, "real1" );
            relB.setProperty( keyB, "real1" );
            relB.setProperty( keyC, "real1" );
            relB.setProperty( keyD, "real2" );
            relB.setProperty( keyE, "real2" );
            relB.setProperty( keyF, "real2" );
            relMultiId = relB.getId();

            // No duplicates
            Node nodeE = db.createNode();
            nodeE.setProperty( keyA, actualValue );
            nodeE.setProperty( keyB, actualValue );
            nodeE.setProperty( keyC, actualValue );

            // No duplicates
            Relationship relC = nodeD.createRelationshipTo( nodeA, relationshipType );
            relC.setProperty( keyA, actualValue );
            relC.setProperty( keyB, actualValue );
            relC.setProperty( keyC, actualValue );
            transaction.success();
        }

        // (#0:Label { keyA: "actual", keyA: "phony!", keyA: "phony!" })
        // (#1 { keyA: "actual", keyA: "actual", keyA: "actual" })
        // (#2:Label { keyA: "real1", keyA: "phony", keyA: "phony", keyD: "real2", keyE: "phony", keyF: "phony" })
        // (#3 { keyA: "real1", keyA: "phony", keyA: "phony", keyD: "real2", keyE: "phony", keyF: "phony" })
        // (#4 { keyA: "actual", keyB: "actual", keyC: "actual" })
        // (#0)-[#0:REL { keyA: "actual", keyA: "actual", keyA: "actual" }]->(#1)
        // (#0)-[#1:REL { keyA: "real1", keyA: "phony", keyA: "phony", keyD: "real2", keyE: "phony", keyF: "phony" }]->(#1)
        // (#2)-[#2:REL { keyA: "actual", keyB: "actual", keyC: "actual" }]->(#0)

        DependencyResolver resolver = api.getDependencyResolver();
        NeoStoreProvider neoStoreProvider = resolver.resolveDependency( NeoStoreProvider.class );
        NeoStore neoStore = neoStoreProvider.evaluate();
        PropertyKeyTokenStore propertyKeyTokenStore = neoStore.getPropertyKeyTokenStore();

        Token tokenA = findPropertyKeyTokenFor( propertyKeyTokenStore, keyA );
        Token tokenB = findPropertyKeyTokenFor( propertyKeyTokenStore, keyB );
        Token tokenC = findPropertyKeyTokenFor( propertyKeyTokenStore, keyC );
        Token tokenD = findPropertyKeyTokenFor( propertyKeyTokenStore, keyD );
        Token tokenE = findPropertyKeyTokenFor( propertyKeyTokenStore, keyE );
        Token tokenF = findPropertyKeyTokenFor( propertyKeyTokenStore, keyF );

        NodeStore nodeStore = neoStore.getNodeStore();
        RelationshipStore relationshipStore = neoStore.getRelationshipStore();
        PropertyStore propertyStore = neoStore.getPropertyStore();

        NodeRecord indexedNodeSingle = nodeStore.getRecord( nodeIndexedSingleId );
        NodeRecord nonIndexedNodeSingle = nodeStore.getRecord( nodeNotIndexedSingleId );
        NodeRecord indexedNodeMulti = nodeStore.getRecord( nodeIndexedMultiId );
        NodeRecord nonindexedNodeMulti = nodeStore.getRecord( nodeNotIndexedMultiId );
        RelationshipRecord relationshipRecordSingle = relationshipStore.getRecord( relSingleId );
        RelationshipRecord relationshipRecordMulti = relationshipStore.getRecord( relMultiId );

        replacePropertyKey( propertyStore, indexedNodeSingle, tokenB, tokenA );
        replacePropertyKey( propertyStore, indexedNodeSingle, tokenC, tokenA );

        replacePropertyKey( propertyStore, nonIndexedNodeSingle, tokenB, tokenA );
        replacePropertyKey( propertyStore, nonIndexedNodeSingle, tokenC, tokenA );

        replacePropertyKey( propertyStore, indexedNodeMulti, tokenB, tokenA );
        replacePropertyKey( propertyStore, indexedNodeMulti, tokenC, tokenA );
        replacePropertyKey( propertyStore, indexedNodeMulti, tokenE, tokenD );
        replacePropertyKey( propertyStore, indexedNodeMulti, tokenF, tokenD );

        replacePropertyKey( propertyStore, nonindexedNodeMulti, tokenB, tokenA );
        replacePropertyKey( propertyStore, nonindexedNodeMulti, tokenC, tokenA );
        replacePropertyKey( propertyStore, nonindexedNodeMulti, tokenE, tokenD );
        replacePropertyKey( propertyStore, nonindexedNodeMulti, tokenF, tokenD );

        replacePropertyKey( propertyStore, relationshipRecordSingle, tokenB, tokenA );
        replacePropertyKey( propertyStore, relationshipRecordSingle, tokenC, tokenA );

        replacePropertyKey( propertyStore, relationshipRecordMulti, tokenB, tokenA );
        replacePropertyKey( propertyStore, relationshipRecordMulti, tokenC, tokenA );
        replacePropertyKey( propertyStore, relationshipRecordMulti, tokenE, tokenD );
        replacePropertyKey( propertyStore, relationshipRecordMulti, tokenF, tokenD );

        db.shutdown();
    }

    private static void replacePropertyKey(
            PropertyStore propertyStore,
            PrimitiveRecord record,
            Token original,
            Token replacement )
    {
        long nextProp = record.getNextProp();
        while ( nextProp != -1 )
        {
            PropertyRecord propertyRecord = propertyStore.getRecord( nextProp );
            for ( PropertyBlock propertyBlock : propertyRecord.getPropertyBlocks() )
            {
                if ( propertyBlock.getKeyIndexId() == original.id() )
                {
                    propertyBlock.setKeyIndexId( replacement.id() );
                }
            }
            propertyStore.updateRecord( propertyRecord );
            nextProp = propertyRecord.getNextProp();
        }
    }

    private static Token findPropertyKeyTokenFor( PropertyKeyTokenStore propertyKeyTokenStore, String key )
    {
        long highestPossibleIdInUse = propertyKeyTokenStore.getHighestPossibleIdInUse();
        for ( int i = 0; i <= highestPossibleIdInUse; i++ )
        {
            Token token = propertyKeyTokenStore.getToken( i );
            if ( token.name().equals( key ) )
            {
                return token;
            }
        }
        return null;
    }
}
 */
