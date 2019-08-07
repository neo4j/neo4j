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
package org.neo4j.kernel.api.impl.schema;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;

@RunWith( Parameterized.class )
public class DefaultSchemaIndexConfigTest
{
    private static final String KEY = "key";
    private static final TestLabels LABEL = TestLabels.LABEL_ONE;

    @Rule
    public TestDirectory directory = TestDirectory.testDirectory();
    private DatabaseManagementServiceBuilder dbBuilder;

    @Before
    public void setup()
    {
        dbBuilder = new TestDatabaseManagementServiceBuilder( directory.storeDir() );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static List<GraphDatabaseSettings.SchemaIndex> providers()
    {
        List<GraphDatabaseSettings.SchemaIndex> providers = new ArrayList<>( Arrays.asList( GraphDatabaseSettings.SchemaIndex.values() ) );
        providers.add( null ); // <-- to exercise the default option
        return providers;
    }

    @Parameterized.Parameter
    public GraphDatabaseSettings.SchemaIndex provider;

    @Test
    public void shouldUseConfiguredIndexProvider() throws IndexNotFoundKernelException
    {
        // given
        DatabaseManagementServiceBuilder
                databaseManagementServiceBuilder = dbBuilder.setConfig( default_schema_provider, provider == null ? null : provider.providerName() );
        DatabaseManagementService managementService = databaseManagementServiceBuilder.build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            // when
            createIndex( db );

            // then
            assertIndexProvider( db, provider == null ? GenericNativeIndexProvider.DESCRIPTOR.name() : provider.providerName() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    public void indexShouldHaveIndexConfig() throws IndexNotFoundKernelException
    {
        DatabaseManagementServiceBuilder
                databaseManagementServiceBuilder = dbBuilder.setConfig( default_schema_provider, provider == null ? null : provider.providerName() );
        DatabaseManagementService managementService = databaseManagementServiceBuilder.build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            // when
            createIndex( db );

            // then
            validateIndexConfig( db );
        }
        finally
        {
            managementService.shutdown();
        }

        managementService = databaseManagementServiceBuilder.build();
        db = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            validateIndexConfig( db );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private void validateIndexConfig( GraphDatabaseService db ) throws IndexNotFoundKernelException
    {
        try ( Transaction tx = db.beginTx() )
        {
            GraphDatabaseAPI api = (GraphDatabaseAPI) db;
            TokenRead tokenRead = tokenRead( api );
            IndexingService indexingService = getIndexingService( api );
            int labelId = tokenRead.nodeLabel( LABEL.name() );
            int propKeyId = tokenRead.propertyKey( KEY );
            IndexProxy indexProxy = indexingService.getIndexProxy( SchemaDescriptor.forLabel( labelId, propKeyId ) );
            Map<String,Value> indexConfig = indexProxy.indexConfig();

            // Expected default values for schema index
            assertEquals( Values.intValue( 2 ), indexConfig.get( "spatial.cartesian.tableId" ) );
            assertEquals( Values.intValue( 7203 ), indexConfig.get( "spatial.cartesian.code" ) );
            assertEquals( Values.intValue( 2 ), indexConfig.get( "spatial.cartesian.dimensions" ) );
            assertEquals( Values.intValue( 30 ), indexConfig.get( "spatial.cartesian.maxLevels" ) );
            assertEquals( Values.doubleArray( new double[]{-1000000.0, -1000000.0} ), indexConfig.get( "spatial.cartesian.min" ) );
            assertEquals( Values.doubleArray( new double[]{1000000.0, 1000000.0} ), indexConfig.get( "spatial.cartesian.max" ) );
            assertEquals( Values.intValue( 2 ), indexConfig.get( "spatial.cartesian-3d.tableId" ) );
            assertEquals( Values.intValue( 9157 ), indexConfig.get( "spatial.cartesian-3d.code" ) );
            assertEquals( Values.intValue( 3 ), indexConfig.get( "spatial.cartesian-3d.dimensions" ) );
            assertEquals( Values.intValue( 20 ), indexConfig.get( "spatial.cartesian-3d.maxLevels" ) );
            assertEquals( Values.doubleArray( new double[]{-1000000.0, -1000000.0, -1000000.0} ), indexConfig.get( "spatial.cartesian-3d.min" ) );
            assertEquals( Values.doubleArray( new double[]{1000000.0, 1000000.0, 1000000.0} ), indexConfig.get( "spatial.cartesian-3d.max" ) );
            assertEquals( Values.intValue( 1 ), indexConfig.get( "spatial.wgs-84.tableId" ) );
            assertEquals( Values.intValue( 4326 ), indexConfig.get( "spatial.wgs-84.code" ) );
            assertEquals( Values.intValue( 2 ), indexConfig.get( "spatial.wgs-84.dimensions" ) );
            assertEquals( Values.intValue( 30 ), indexConfig.get( "spatial.wgs-84.maxLevels" ) );
            assertEquals( Values.doubleArray( new double[]{-180.0, -90.0} ), indexConfig.get( "spatial.wgs-84.min" ) );
            assertEquals( Values.doubleArray( new double[]{180.0, 90.0} ), indexConfig.get( "spatial.wgs-84.max" ) );
            assertEquals( Values.intValue( 1 ), indexConfig.get( "spatial.wgs-84-3d.tableId" ) );
            assertEquals( Values.intValue( 4979 ), indexConfig.get( "spatial.wgs-84-3d.code" ) );
            assertEquals( Values.intValue( 3 ), indexConfig.get( "spatial.wgs-84-3d.dimensions" ) );
            assertEquals( Values.intValue( 20 ), indexConfig.get( "spatial.wgs-84-3d.maxLevels" ) );
            assertEquals( Values.doubleArray( new double[]{-180.0, -90.0, -1000000.0} ), indexConfig.get( "spatial.wgs-84-3d.min" ) );
            assertEquals( Values.doubleArray( new double[]{180.0, 90.0, 1000000.0} ), indexConfig.get( "spatial.wgs-84-3d.max" ) );
            tx.commit();
        }
    }

    private static IndexingService getIndexingService( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( IndexingService.class );
    }

    private static TokenRead tokenRead( GraphDatabaseAPI db )
    {
        ThreadToStatementContextBridge bridge = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        return bridge.getKernelTransactionBoundToThisThread( false, db.databaseId() ).tokenRead();
    }

    private void assertIndexProvider( GraphDatabaseService db, String expectedProviderIdentifier ) throws IndexNotFoundKernelException
    {
        GraphDatabaseAPI graphDatabaseAPI = (GraphDatabaseAPI) db;
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = graphDatabaseAPI.getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .getKernelTransactionBoundToThisThread( true, graphDatabaseAPI.databaseId() );
            TokenRead tokenRead = ktx.tokenRead();
            int labelId = tokenRead.nodeLabel( LABEL.name() );
            int propertyId = tokenRead.propertyKey( KEY );
            IndexDescriptor index = ktx.schemaRead().index( labelId, propertyId );

            assertEquals( "expected IndexProvider.Descriptor", expectedProviderIdentifier, index.getIndexProvider().name() );
            tx.commit();
        }
    }

    private void createIndex( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL ).on( KEY ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
    }
}
