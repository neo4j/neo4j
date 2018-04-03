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
package org.neo4j.kernel.api.impl.schema;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.TestLabels;

import static org.junit.Assert.assertEquals;

public class DefaultSchemaIndexConfigTest
{
    private static final String KEY = "key";
    private static final TestLabels LABEL = TestLabels.LABEL_ONE;
    private static final GraphDatabaseBuilder dbBuilder = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder();

    @Test
    public void shouldUseConfiguredIndexProviderNull() throws IndexNotFoundKernelException
    {
        // given
        GraphDatabaseService db = dbBuilder.setConfig( GraphDatabaseSettings.default_schema_provider, null ).newGraphDatabase();

        // when
        createIndex( db );

        // then
        assertIndexProvider( db, NativeLuceneFusionIndexProviderFactory20.DESCRIPTOR );
    }

    @Test
    public void shouldUseConfiguredIndexProviderLucene() throws IndexNotFoundKernelException
    {
        // given
        GraphDatabaseService db = dbBuilder.setConfig( GraphDatabaseSettings.default_schema_provider,
                GraphDatabaseSettings.SchemaIndex.LUCENE10.providerName() ).newGraphDatabase();

        // when
        createIndex( db );

        // then
        assertIndexProvider( db, LuceneIndexProviderFactory.PROVIDER_DESCRIPTOR );
    }

    @Test
    public void shouldUseConfiguredIndexProviderNative10() throws IndexNotFoundKernelException
    {
        // given
        GraphDatabaseService db = dbBuilder.setConfig( GraphDatabaseSettings.default_schema_provider,
                GraphDatabaseSettings.SchemaIndex.NATIVE10.providerName() ).newGraphDatabase();

        // when
        createIndex( db );

        // then
        assertIndexProvider( db, NativeLuceneFusionIndexProviderFactory10.DESCRIPTOR );
    }

    @Test
    public void shouldUseConfiguredIndexProviderNative20() throws IndexNotFoundKernelException
    {
        // given
        GraphDatabaseService db = dbBuilder.setConfig( GraphDatabaseSettings.default_schema_provider,
                GraphDatabaseSettings.SchemaIndex.NATIVE20.providerName() ).newGraphDatabase();

        // when
        createIndex( db );

        // then
        assertIndexProvider( db, NativeLuceneFusionIndexProviderFactory20.DESCRIPTOR );
    }

    private void assertIndexProvider( GraphDatabaseService db, IndexProvider.Descriptor expected ) throws IndexNotFoundKernelException
    {
        GraphDatabaseAPI graphDatabaseAPI = (GraphDatabaseAPI) db;
        try ( Transaction tx = graphDatabaseAPI.beginTx();
              Statement statement = graphDatabaseAPI.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class ).get() )
        {
            ReadOperations readOperations = statement.readOperations();
            int labelId = readOperations.labelGetForName( LABEL.name() );
            int propertyId = readOperations.propertyKeyGetForName( KEY );
            IndexProvider.Descriptor descriptor = readOperations.indexGetProviderDescriptor( SchemaIndexDescriptorFactory.forLabel( labelId, propertyId ) );
            assertEquals( "expected IndexProvider.Descriptor", expected, descriptor );
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
