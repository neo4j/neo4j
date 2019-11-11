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
package org.neo4j.batchinsert.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSettingImpl;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexValueTestUtil;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.graphdb.schema.IndexType.BTREE;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.test.TestLabels.LABEL_ONE;

@Neo4jLayoutExtension
class BatchInsertIndexTest
{
    @Inject
    private FileSystemAbstraction  fs;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseLayout databaseLayout;

    private Config.Builder configBuilder;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        configBuilder = Config.newBuilder().set( neo4j_home, testDirectory.homeDir().toPath() );
    }

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaIndex.class )
    void batchInserterShouldUseConfiguredIndexProvider( SchemaIndex schemaIndex ) throws Exception
    {
        configure( schemaIndex );
        BatchInserter inserter = newBatchInserter();
        inserter.createDeferredSchemaIndex( LABEL_ONE ).on( "key" ).create();
        inserter.shutdown();
        GraphDatabaseService db = startGraphDatabaseServiceAndAwaitIndexes();
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction kernelTransaction = ((InternalTransaction) tx).kernelTransaction();
            TokenRead tokenRead = kernelTransaction.tokenRead();
            SchemaRead schemaRead = kernelTransaction.schemaRead();
            int labelId = tokenRead.nodeLabel( LABEL_ONE.name() );
            int propertyId = tokenRead.propertyKey( "key" );
            IndexDescriptor index = single( schemaRead.index( SchemaDescriptor.forLabel( labelId, propertyId ) ) );
            assertTrue( schemaIndex.providerName().contains( index.getIndexProvider().getKey() ), unexpectedIndexProviderMessage( index ) );
            assertTrue( schemaIndex.providerName().contains( index.getIndexProvider().getVersion() ), unexpectedIndexProviderMessage( index ) );
            tx.commit();
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaIndex.class )
    void shouldPopulateIndexWithUniquePointsThatCollideOnSpaceFillingCurve( SchemaIndex schemaIndex ) throws Exception
    {
        configure( schemaIndex );
        BatchInserter inserter = newBatchInserter();
        Pair<PointValue,PointValue> collidingPoints = SpatialIndexValueTestUtil.pointsWithSameValueOnSpaceFillingCurve( configBuilder.build() );
        inserter.createNode( MapUtil.map( "prop", collidingPoints.first() ), LABEL_ONE );
        inserter.createNode( MapUtil.map( "prop", collidingPoints.other() ), LABEL_ONE );
        inserter.createDeferredConstraint( LABEL_ONE ).assertPropertyIsUnique( "prop" ).create();
        inserter.shutdown();

        GraphDatabaseService db = startGraphDatabaseServiceAndAwaitIndexes();
        try ( Transaction tx = db.beginTx() )
        {
            assertSingleCorrectHit( collidingPoints.first(), tx );
            assertSingleCorrectHit( collidingPoints.other(), tx );
            tx.commit();
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaIndex.class )
    void shouldThrowWhenPopulatingWithNonUniquePoints( SchemaIndex schemaIndex ) throws Exception
    {
        configure( schemaIndex );
        BatchInserter inserter = newBatchInserter();
        PointValue point = Values.pointValue( CoordinateReferenceSystem.WGS84, 0.0, 0.0 );
        inserter.createNode( MapUtil.map( "prop", point ), LABEL_ONE );
        inserter.createNode( MapUtil.map( "prop", point ), LABEL_ONE );
        inserter.createDeferredConstraint( LABEL_ONE ).assertPropertyIsUnique( "prop" ).create();
        inserter.shutdown();

        GraphDatabaseService db = startGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalStateException.class, () -> tx.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS ) );
        }
        try ( Transaction tx = db.beginTx() )
        {
            var schema = tx.schema();
            Iterator<IndexDefinition> indexes = schema.getIndexes().iterator();
            assertTrue( indexes.hasNext() );
            IndexDefinition index = indexes.next();
            Schema.IndexState indexState = schema.getIndexState( index );
            assertEquals( Schema.IndexState.FAILED, indexState );
            assertFalse( indexes.hasNext() );
            tx.commit();
        }
    }

    @Test
    void creatingFulltextIndexIsUnsupported() throws Exception
    {
        try ( BatchInserter inserter = newBatchInserter() )
        {
            assertThrows( UnsupportedOperationException.class,
                    () -> inserter.createDeferredSchemaIndex( LABEL_ONE ).on( "key" ).withIndexType( FULLTEXT ).withName( "fts" ).create() );
        }
    }

    @Test
    void creatingIndexesWithCustomConfigurationsIsUnsupported() throws Exception
    {
        try ( BatchInserter inserter = newBatchInserter() )
        {
            assertThrows( UnsupportedOperationException.class, () -> inserter.createDeferredSchemaIndex( LABEL_ONE ).on( "key" )
                    .withIndexConfiguration( Map.of( IndexSettingImpl.SPATIAL_CARTESIAN_MAX, new double[]{0.0, 0.0} ) ).create() );
        }
    }

    @Test
    void creatingUniquenessConstraintWithIndexTypeIsUnsupported() throws Exception
    {
        try ( BatchInserter inserter = newBatchInserter() )
        {
            assertThrows( UnsupportedOperationException.class,
                    () -> inserter.createDeferredConstraint( LABEL_ONE ).assertPropertyIsUnique( "key" ).withIndexType( BTREE ).create() );
        }
    }

    @Test
    void creatingNodeKeyConstraintWithIndexTypeIsUnsupported() throws Exception
    {
        try ( BatchInserter inserter = newBatchInserter() )
        {
            assertThrows( UnsupportedOperationException.class,
                    () -> inserter.createDeferredConstraint( LABEL_ONE ).assertPropertyIsNodeKey( "key" ).withIndexType( BTREE ).create() );
        }
    }

    private void configure( SchemaIndex schemaIndex )
    {
        configBuilder = configBuilder.set( default_schema_provider, schemaIndex.providerName() );
    }

    private static void assertSingleCorrectHit( PointValue point, Transaction tx )
    {
        ResourceIterator<Node> nodes = tx.findNodes( LABEL_ONE, "prop", point );
        assertTrue( nodes.hasNext() );
        Node node = nodes.next();
        Object prop = node.getProperty( "prop" );
        assertEquals( point, prop );
        assertFalse( nodes.hasNext() );
    }

    private BatchInserter newBatchInserter() throws Exception
    {
        return BatchInserters.inserter( databaseLayout, fs, configBuilder.build() );
    }

    private GraphDatabaseService startGraphDatabaseServiceAndAwaitIndexes()
    {
        GraphDatabaseService database = startGraphDatabaseService();
        awaitIndexesOnline( database );
        return database;
    }

    private GraphDatabaseService startGraphDatabaseService()
    {
        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setFileSystem( fs )
                .setConfig( configBuilder.build() )
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void awaitIndexesOnline( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }
    }

    private static String unexpectedIndexProviderMessage( IndexDescriptor index )
    {
        return "Unexpected provider: key=" + index.getIndexProvider().getKey() + ", version=" + index.getIndexProvider().getVersion();
    }
}
