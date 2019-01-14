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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterables.first;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.NODE_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.RELATIONSHIP_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProceduresTest.array;

public class FulltextIndexConsistencyCheckIT
{
    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final ExpectedException expectedException = ExpectedException.none();
    private final CleanupRule cleanup = new CleanupRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( fs ).around( testDirectory ).around( expectedException ).around( cleanup );

    private GraphDatabaseBuilder builder;

    @Before
    public void before()
    {
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        builder = factory.newEmbeddedDatabaseBuilder( testDirectory.databaseDir() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckEmptyDatabaseWithFulltextIndexingEnabled() throws Exception
    {
        createDatabase().shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeIndexWithOneLabelAndOneProperty() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "Label" ), array( "prop" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            db.createNode( Label.label( "Label" ) ).setProperty( "prop", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeIndexWithOneLabelAndMultipleProperties() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "Label" ), array( "p1", "p2" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node node = db.createNode( Label.label( "Label" ) );
            node.setProperty( "p1", "value" );
            node.setProperty( "p2", "value" );
            db.createNode( Label.label( "Label" ) ).setProperty( "p1", "value" );
            db.createNode( Label.label( "Label" ) ).setProperty( "p2", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeIndexWithMultipleLabelsAndOneProperty() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "L1", "L2" ), array( "prop" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            db.createNode( Label.label( "L1" ), Label.label( "L2" ) ).setProperty( "prop", "value" );
            db.createNode( Label.label( "L2" ) ).setProperty( "prop", "value" );
            db.createNode( Label.label( "L1" ) ).setProperty( "prop", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeIndexWithManyLabelsAndOneProperty() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        // Enough labels to prevent inlining them into the node record, and instead require a dynamic label record to be allocated.
        String[] labels = {"L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10", "L11", "L12", "L13", "L14", "L15", "L16"};
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( labels ), array( "prop" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            db.createNode( Stream.of( labels ).map( Label::label ).toArray( Label[]::new ) ).setProperty( "prop", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeIndexWithMultipleLabelsAndMultipleProperties() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "L1", "L2" ), array( "p1", "p2" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node n1 = db.createNode( Label.label( "L1" ), Label.label( "L2" ) );
            n1.setProperty( "p1", "value" );
            n1.setProperty( "p2", "value" );
            Node n2 = db.createNode( Label.label( "L1" ), Label.label( "L2" ) );
            n2.setProperty( "p1", "value" );
            Node n3 = db.createNode( Label.label( "L1" ), Label.label( "L2" ) );
            n3.setProperty( "p2", "value" );
            Node n4 = db.createNode( Label.label( "L1" ) );
            n4.setProperty( "p1", "value" );
            n4.setProperty( "p2", "value" );
            Node n5 = db.createNode( Label.label( "L1" ) );
            n5.setProperty( "p1", "value" );
            Node n6 = db.createNode( Label.label( "L1" ) );
            n6.setProperty( "p2", "value" );
            Node n7 = db.createNode( Label.label( "L2" ) );
            n7.setProperty( "p1", "value" );
            n7.setProperty( "p2", "value" );
            Node n8 = db.createNode( Label.label( "L2" ) );
            n8.setProperty( "p1", "value" );
            Node n9 = db.createNode( Label.label( "L2" ) );
            n9.setProperty( "p2", "value" );
            db.createNode( Label.label( "L2" ) ).setProperty( "p1", "value" );
            db.createNode( Label.label( "L2" ) ).setProperty( "p2", "value" );
            db.createNode( Label.label( "L1" ) ).setProperty( "p1", "value" );
            db.createNode( Label.label( "L1" ) ).setProperty( "p2", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckRelationshipIndexWithOneRelationshipTypeAndOneProperty() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        RelationshipType relationshipType = RelationshipType.withName( "R1" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( "R1" ), array( "p1" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node node = db.createNode();
            node.createRelationshipTo( node, relationshipType ).setProperty( "p1", "value" );
            node.createRelationshipTo( node, relationshipType ).setProperty( "p1", "value" ); // This relationship will have a different id value than the node.
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckRelationshipIndexWithOneRelationshipTypeAndMultipleProperties() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        RelationshipType relationshipType = RelationshipType.withName( "R1" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( "R1" ), array( "p1", "p2" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node node = db.createNode();
            Relationship r1 = node.createRelationshipTo( node, relationshipType );
            r1.setProperty( "p1", "value" );
            r1.setProperty( "p2", "value" );
            Relationship r2 = node.createRelationshipTo( node, relationshipType ); // This relationship will have a different id value than the node.
            r2.setProperty( "p1", "value" );
            r2.setProperty( "p2", "value" );
            node.createRelationshipTo( node, relationshipType ).setProperty( "p1", "value" );
            node.createRelationshipTo( node, relationshipType ).setProperty( "p2", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckRelationshipIndexWithMultipleRelationshipTypesAndOneProperty() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        RelationshipType relType1 = RelationshipType.withName( "R1" );
        RelationshipType relType2 = RelationshipType.withName( "R2" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( "R1", "R2" ), array( "p1" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node n1 = db.createNode();
            Node n2 = db.createNode();
            n1.createRelationshipTo( n1, relType1 ).setProperty( "p1", "value" );
            n1.createRelationshipTo( n1, relType2 ).setProperty( "p1", "value" );
            n2.createRelationshipTo( n2, relType1 ).setProperty( "p1", "value" );
            n2.createRelationshipTo( n2, relType2 ).setProperty( "p1", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckRelationshipIndexWithMultipleRelationshipTypesAndMultipleProperties() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        RelationshipType relType1 = RelationshipType.withName( "R1" );
        RelationshipType relType2 = RelationshipType.withName( "R2" );
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( "R1", "R2" ), array( "p1", "p2" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node n1 = db.createNode();
            Node n2 = db.createNode();
            Relationship r1 = n1.createRelationshipTo( n1, relType1 );
            r1.setProperty( "p1", "value" );
            r1.setProperty( "p2", "value" );
            Relationship r2 = n1.createRelationshipTo( n1, relType2 );
            r2.setProperty( "p1", "value" );
            r2.setProperty( "p2", "value" );
            Relationship r3 = n2.createRelationshipTo( n2, relType1 );
            r3.setProperty( "p1", "value" );
            r3.setProperty( "p2", "value" );
            Relationship r4 = n2.createRelationshipTo( n2, relType2 );
            r4.setProperty( "p1", "value" );
            r4.setProperty( "p2", "value" );
            n1.createRelationshipTo( n2, relType1 ).setProperty( "p1", "value" );
            n1.createRelationshipTo( n2, relType2 ).setProperty( "p1", "value" );
            n1.createRelationshipTo( n2, relType1 ).setProperty( "p2", "value" );
            n1.createRelationshipTo( n2, relType2 ).setProperty( "p2", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeAndRelationshipIndexesAtTheSameTime() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "L1", "L2", "L3" ), array( "p1", "p2" ) ) ).close();
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( "R1", "R2" ), array( "p1", "p2" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node n1 = db.createNode( Label.label( "L1" ), Label.label( "L3" ) );
            n1.setProperty( "p1", "value" );
            n1.setProperty( "p2", "value" );
            n1.createRelationshipTo( n1, RelationshipType.withName( "R2" ) ).setProperty( "p1", "value" );
            Node n2 = db.createNode( Label.label( "L2" ) );
            n2.setProperty( "p2", "value" );
            Relationship r1 = n2.createRelationshipTo( n2, RelationshipType.withName( "R1" ) );
            r1.setProperty( "p1", "value" );
            r1.setProperty( "p2", "value" );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencyCheckNodeIndexThatIsMissingNodesBecauseTheirPropertyValuesAreNotStrings() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "L1" ), array( "p1" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            db.createNode( Label.label( "L1" ) ).setProperty( "p1", 1 );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustBeAbleToConsistencycheckRelationshipIndexThatIsMissingRelationshipsBecauseTheirPropertyValuesaAreNotStrings() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( "R1" ), array( "p1" ) ) ).close();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node node = db.createNode();
            node.createRelationshipTo( node, RelationshipType.withName( "R1" ) ).setProperty( "p1", 1 );
            tx.success();
        }
        db.shutdown();
        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void consistencyCheckerMustBeAbleToRunOnStoreWithFulltextIndexes() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        Label[] labels = IntStream.range( 1, 7 ).mapToObj( i -> Label.label( "LABEL" + i ) ).toArray( Label[]::new );
        RelationshipType[] relTypes = IntStream.range( 1, 5 ).mapToObj( i -> RelationshipType.withName( "REL" + i ) ).toArray( RelationshipType[]::new );
        String[] propertyKeys = IntStream.range( 1, 7 ).mapToObj( i -> "PROP" + i ).toArray( String[]::new );
        RandomValues randomValues = RandomValues.create();

        try ( Transaction tx = db.beginTx() )
        {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int nodeCount = 1000;
            List<Node> nodes = new ArrayList<>( nodeCount );
            for ( int i = 0; i < nodeCount; i++ )
            {
                Label[] nodeLabels = rng.ints( rng.nextInt( labels.length ), 0, labels.length ).distinct().mapToObj( x -> labels[x] ).toArray( Label[]::new );
                Node node = db.createNode( nodeLabels );
                Stream.of( propertyKeys ).forEach( p -> node.setProperty( p, rng.nextBoolean() ? p : randomValues.nextValue().asObject() ) );
                nodes.add( node );
                int localRelCount = Math.min( nodes.size(), 5 );
                rng.ints( localRelCount, 0, localRelCount ).distinct().mapToObj(
                        x -> node.createRelationshipTo( nodes.get( x ), relTypes[rng.nextInt( relTypes.length )] ) ).forEach(
                        r -> Stream.of( propertyKeys ).forEach( p -> r.setProperty( p, rng.nextBoolean() ? p : randomValues.nextValue().asObject() ) ) );
            }
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 1; i < labels.length; i++ )
            {
                db.execute( format( NODE_CREATE, "nodes" + i, array( Arrays.stream( labels ).limit( i ).map( Label::name ).toArray( String[]::new ) ),
                        array( Arrays.copyOf( propertyKeys, i ) ) ) ).close();
            }
            for ( int i = 1; i < relTypes.length; i++ )
            {
                db.execute( format( RELATIONSHIP_CREATE, "rels" + i,
                        array( Arrays.stream( relTypes ).limit( i ).map( RelationshipType::name ).toArray( String[]::new ) ),
                        array( Arrays.copyOf( propertyKeys, i ) ) ) ).close();
            }
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }

        db.shutdown();

        assertIsConsistent( checkConsistency() );
    }

    @Test
    public void mustDiscoverNodeInStoreMissingFromIndex() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "Label" ), array( "prop" ) ) ).close();
            tx.success();
        }
        StoreIndexDescriptor indexDescriptor;
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            indexDescriptor = getIndexDescriptor( first( db.schema().getIndexes() ) );
            Node node = db.createNode( Label.label( "Label" ) );
            node.setProperty( "prop", "value" );
            nodeId = node.getId();
            tx.success();
        }
        IndexingService indexes = getIndexingService( db );
        IndexProxy indexProxy = indexes.getIndexProxy( indexDescriptor.schema() );
        try ( IndexUpdater updater = indexProxy.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( IndexEntryUpdate.remove( nodeId, indexDescriptor, Values.stringValue( "value" ) ) );
        }

        db.shutdown();

        ConsistencyCheckService.Result result = checkConsistency();
        assertFalse( result.isSuccessful() );
    }

    @Ignore( "Turns out that this is not something that the consistency checker actually looks for, currently. " +
            "The test is disabled until the consistency checker is extended with checks that will discover this sort of inconsistency." )
    @Test
    public void mustDiscoverNodeInIndexMissingFromStore() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( NODE_CREATE, "nodes", array( "Label" ), array( "prop" ) ) ).close();
            tx.success();
        }
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node node = db.createNode( Label.label( "Label" ) );
            nodeId = node.getId();
            node.setProperty( "prop", "value" );
            tx.success();
        }
        NeoStores stores = getNeoStores( db );
        NodeRecord record = stores.getNodeStore().newRecord();
        record = stores.getNodeStore().getRecord( nodeId, record, RecordLoad.NORMAL );
        long propId = record.getNextProp();
        record.setNextProp( AbstractBaseRecord.NO_ID );
        stores.getNodeStore().updateRecord( record );
        PropertyRecord propRecord = stores.getPropertyStore().getRecord( propId, stores.getPropertyStore().newRecord(), RecordLoad.NORMAL );
        propRecord.setInUse( false );
        stores.getPropertyStore().updateRecord( propRecord );
        db.shutdown();

        ConsistencyCheckService.Result result = checkConsistency();
        assertFalse( result.isSuccessful() );
    }

    @Test
    public void mustDiscoverRelationshipInStoreMissingFromIndex() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( "REL" ), array( "prop" ) ) ).close();
            tx.success();
        }
        StoreIndexDescriptor indexDescriptor;
        long relId;
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            indexDescriptor = getIndexDescriptor( first( db.schema().getIndexes() ) );
            Node node = db.createNode();
            Relationship rel = node.createRelationshipTo( node, RelationshipType.withName( "REL" ) );
            rel.setProperty( "prop", "value" );
            relId = rel.getId();
            tx.success();
        }
        IndexingService indexes = getIndexingService( db );
        IndexProxy indexProxy = indexes.getIndexProxy( indexDescriptor.schema() );
        try ( IndexUpdater updater = indexProxy.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( IndexEntryUpdate.remove( relId, indexDescriptor, Values.stringValue( "value" ) ) );
        }

        db.shutdown();

        ConsistencyCheckService.Result result = checkConsistency();
        assertFalse( result.isSuccessful() );
    }

    @Ignore( "Turns out that this is not something that the consistency checker actually looks for, currently. " +
            "The test is disabled until the consistency checker is extended with checks that will discover this sort of inconsistency." )
    @Test
    public void mustDiscoverRelationshipInIndexMissingFromStore() throws Exception
    {
        GraphDatabaseService db = createDatabase();
        try ( Transaction tx = db.beginTx() )
        {
            db.execute( format( RELATIONSHIP_CREATE, "rels", array( "REL" ), array( "prop" ) ) ).close();
            tx.success();
        }
        long relId;
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            Node node = db.createNode();
            Relationship rel = node.createRelationshipTo( node, RelationshipType.withName( "REL" ) );
            relId = rel.getId();
            rel.setProperty( "prop", "value" );
            tx.success();
        }
        NeoStores stores = getNeoStores( db );
        RelationshipRecord record = stores.getRelationshipStore().newRecord();
        record = stores.getRelationshipStore().getRecord( relId, record, RecordLoad.NORMAL );
        long propId = record.getNextProp();
        record.setNextProp( AbstractBaseRecord.NO_ID );
        stores.getRelationshipStore().updateRecord( record );
        PropertyRecord propRecord = stores.getPropertyStore().getRecord( propId, stores.getPropertyStore().newRecord(), RecordLoad.NORMAL );
        propRecord.setInUse( false );
        stores.getPropertyStore().updateRecord( propRecord );
        db.shutdown();

        ConsistencyCheckService.Result result = checkConsistency();
        assertFalse( result.isSuccessful() );
    }

    private GraphDatabaseService createDatabase()
    {
        return cleanup.add( builder.newGraphDatabase() );
    }

    private ConsistencyCheckService.Result checkConsistency() throws ConsistencyCheckIncompleteException
    {
        Config config = Config.defaults();
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService( new Date() );
        ConsistencyFlags checkConsistencyConfig = new ConsistencyFlags( config );
        return consistencyCheckService.runFullConsistencyCheck( testDirectory.databaseLayout(), config, ProgressMonitorFactory.NONE,
                NullLogProvider.getInstance(), true, checkConsistencyConfig );
    }

    private static StoreIndexDescriptor getIndexDescriptor( IndexDefinition definition )
    {
        StoreIndexDescriptor indexDescriptor;
        IndexDefinitionImpl indexDefinition = (IndexDefinitionImpl) definition;
        indexDescriptor = (StoreIndexDescriptor) indexDefinition.getIndexReference();
        return indexDescriptor;
    }

    private static IndexingService getIndexingService( GraphDatabaseService db )
    {
        DependencyResolver dependencyResolver = getDependencyResolver( db );
        return dependencyResolver.resolveDependency( IndexingService.class, DependencyResolver.SelectionStrategy.ONLY );
    }

    private static NeoStores getNeoStores( GraphDatabaseService db )
    {
        DependencyResolver dependencyResolver = getDependencyResolver( db );
        return dependencyResolver.resolveDependency( RecordStorageEngine.class, DependencyResolver.SelectionStrategy.ONLY ).testAccessNeoStores();
    }

    private static DependencyResolver getDependencyResolver( GraphDatabaseService db )
    {
        GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        return api.getDependencyResolver();
    }

    private static void assertIsConsistent( ConsistencyCheckService.Result result ) throws IOException
    {
        if ( !result.isSuccessful() )
        {
            printReport( result );
            fail( "Expected consistency check to be successful." );
        }
    }

    private static void printReport( ConsistencyCheckService.Result result ) throws IOException
    {
        Files.readAllLines( result.reportFile().toPath() ).forEach( System.err::println );
    }
}
