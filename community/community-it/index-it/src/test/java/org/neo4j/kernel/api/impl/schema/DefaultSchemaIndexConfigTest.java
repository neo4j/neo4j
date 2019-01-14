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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestLabels;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;

@RunWith( Parameterized.class )
public class DefaultSchemaIndexConfigTest
{
    private static final String KEY = "key";
    private static final TestLabels LABEL = TestLabels.LABEL_ONE;
    private static final GraphDatabaseBuilder dbBuilder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder();

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
        GraphDatabaseService db = dbBuilder.setConfig( default_schema_provider, provider == null ? null : provider.providerName() ).newGraphDatabase();
        try
        {
            // when
            createIndex( db );

            // then
            assertIndexProvider( db, provider == null ? GenericNativeIndexProvider.DESCRIPTOR.name() : provider.providerName() );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void assertIndexProvider( GraphDatabaseService db, String expectedProviderIdentifier ) throws IndexNotFoundKernelException
    {
        GraphDatabaseAPI graphDatabaseAPI = (GraphDatabaseAPI) db;
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = graphDatabaseAPI.getDependencyResolver()
                    .resolveDependency( ThreadToStatementContextBridge.class )
                    .getKernelTransactionBoundToThisThread( true );
            TokenRead tokenRead = ktx.tokenRead();
            int labelId = tokenRead.nodeLabel( LABEL.name() );
            int propertyId = tokenRead.propertyKey( KEY );
            IndexReference index = ktx.schemaRead().index( labelId, propertyId );

            assertEquals( "expected IndexProvider.Descriptor", expectedProviderIdentifier,
                    new IndexProviderDescriptor( index.providerKey(), index.providerVersion() ).name() );
            tx.success();
        }
    }

    private void createIndex( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL ).on( KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }
}
