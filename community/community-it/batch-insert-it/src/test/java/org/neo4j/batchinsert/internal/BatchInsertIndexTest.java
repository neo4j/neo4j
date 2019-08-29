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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexValueTestUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

@TestDirectoryExtension
class BatchInsertIndexTest
{
    @Inject
    private FileSystemAbstraction  fs;
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;

    @ParameterizedTest
    @EnumSource( SchemaIndex.class )
    void batchInserterShouldUseConfiguredIndexProvider( SchemaIndex schemaIndex ) throws Exception
    {
        Config config = Config.newBuilder()
                .set( default_schema_provider, schemaIndex.providerName() )
                .set( neo4j_home, testDirectory.absolutePath().toPath() ).build();
        BatchInserter inserter = newBatchInserter( config );
        inserter.createDeferredSchemaIndex( TestLabels.LABEL_ONE ).on( "key" ).create();
        inserter.shutdown();
        GraphDatabaseService db = graphDatabaseService( config );
        awaitIndexesOnline( db );
        try ( Transaction tx = db.beginTx() )
        {
            DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
            ThreadToStatementContextBridge threadToStatementContextBridge = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class );
            KernelTransaction kernelTransaction =
                    threadToStatementContextBridge.getKernelTransactionBoundToThisThread( true, ((GraphDatabaseAPI) db).databaseId() );
            TokenRead tokenRead = kernelTransaction.tokenRead();
            SchemaRead schemaRead = kernelTransaction.schemaRead();
            int labelId = tokenRead.nodeLabel( TestLabels.LABEL_ONE.name() );
            int propertyId = tokenRead.propertyKey( "key" );
            IndexDescriptor index = schemaRead.index( labelId, propertyId );
            assertTrue( schemaIndex.providerName().contains( index.getIndexProvider().getKey() ), unexpectedIndexProviderMessage( index ) );
            assertTrue( schemaIndex.providerName().contains( index.getIndexProvider().getVersion() ), unexpectedIndexProviderMessage( index ) );
            tx.commit();
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaIndex.class )
    void shouldPopulateIndexWithUniquePointsThatCollideOnSpaceFillingCurve( SchemaIndex schemaIndex ) throws Exception
    {
        Config config = Config.newBuilder()
                .set( default_schema_provider, schemaIndex.providerName() )
                .set( neo4j_home, testDirectory.absolutePath().toPath() ).build();
        BatchInserter inserter = newBatchInserter( config );
        Pair<PointValue,PointValue> collidingPoints = SpatialIndexValueTestUtil.pointsWithSameValueOnSpaceFillingCurve( config );
        inserter.createNode( MapUtil.map( "prop", collidingPoints.first() ), TestLabels.LABEL_ONE );
        inserter.createNode( MapUtil.map( "prop", collidingPoints.other() ), TestLabels.LABEL_ONE );
        inserter.createDeferredConstraint( TestLabels.LABEL_ONE ).assertPropertyIsUnique( "prop" ).create();
        inserter.shutdown();

        GraphDatabaseService db = graphDatabaseService( config );
        try
        {
            awaitIndexesOnline( db );
            try ( Transaction tx = db.beginTx() )
            {
                assertSingleCorrectHit( db, collidingPoints.first() );
                assertSingleCorrectHit( db, collidingPoints.other() );
                tx.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaIndex.class )
    void shouldThrowWhenPopulatingWithNonUniquePoints( SchemaIndex schemaIndex ) throws Exception
    {
        Config config = Config.newBuilder()
                .set( default_schema_provider, schemaIndex.providerName() )
                .set( neo4j_home, testDirectory.absolutePath().toPath() ).build();
        BatchInserter inserter = newBatchInserter( config );
        PointValue point = Values.pointValue( CoordinateReferenceSystem.WGS84, 0.0, 0.0 );
        inserter.createNode( MapUtil.map( "prop", point ), TestLabels.LABEL_ONE );
        inserter.createNode( MapUtil.map( "prop", point ), TestLabels.LABEL_ONE );
        inserter.createDeferredConstraint( TestLabels.LABEL_ONE ).assertPropertyIsUnique( "prop" ).create();
        inserter.shutdown();

        GraphDatabaseService db = graphDatabaseService( config );
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<IndexDefinition> indexes = db.schema().getIndexes().iterator();
            assertTrue( indexes.hasNext() );
            IndexDefinition index = indexes.next();
            Schema.IndexState indexState = db.schema().getIndexState( index );
            assertEquals( Schema.IndexState.FAILED, indexState );
            assertFalse( indexes.hasNext() );
            tx.commit();
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static void assertSingleCorrectHit( GraphDatabaseService db, PointValue point )
    {
        ResourceIterator<Node> nodes = db.findNodes( TestLabels.LABEL_ONE, "prop", point );
        assertTrue( nodes.hasNext() );
        Node node = nodes.next();
        Object prop = node.getProperty( "prop" );
        assertEquals( point, prop );
        assertFalse( nodes.hasNext() );
    }

    private BatchInserter newBatchInserter( Config config ) throws Exception
    {
        return BatchInserters.inserter( testDirectory.databaseLayout(), fs, config );
    }

    private GraphDatabaseService graphDatabaseService( Config config )
    {
        managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setFileSystem( fs )
                .setConfig( config )
                .build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static void awaitIndexesOnline( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }
    }

    private static String unexpectedIndexProviderMessage( IndexDescriptor index )
    {
        return "Unexpected provider: key=" + index.getIndexProvider().getKey() + ", version=" + index.getIndexProvider().getVersion();
    }
}
