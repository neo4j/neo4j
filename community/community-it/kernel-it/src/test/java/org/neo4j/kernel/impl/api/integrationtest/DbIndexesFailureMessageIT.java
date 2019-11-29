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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory.FailureType.POPULATION;
import static org.neo4j.test.TestDatabaseManagementServiceBuilder.INDEX_PROVIDERS_FILTER;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

class DbIndexesFailureMessageIT extends KernelIntegrationTest
{
    private final AtomicBoolean failNextIndexPopulation = new AtomicBoolean();

    @Test
    void listAllIndexesWithFailedIndex() throws Throwable
    {
        // Given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );
        int failedLabel = transaction.tokenWrite().labelGetOrCreateForName( "Fail" );
        int propertyKeyId1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        failNextIndexPopulation.set( true );
        LabelSchemaDescriptor schema = forLabel( failedLabel, propertyKeyId1 );
        IndexDescriptor index = transaction.schemaWrite().indexCreate( schema, "fail foo index" );
        commit();

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalStateException.class, () -> tx.schema().awaitIndexesOnline( 2, MINUTES ) );
        }

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "indexDetails" ) ).id(),
                        new TextValue[]{stringValue( index.getName() )},
                        ProcedureCallContext.EMPTY );
        assertTrue( stream.hasNext() );
        AnyValue[] result = stream.next();
        assertFalse( stream.hasNext() );
        commit(); // Commit procedure transaction

        // Then
        assertEquals( longValue( index.getId() ), result[0] );
        assertEquals( stringValue( "fail foo index" ), result[1] );
        assertEquals( stringValue( "FAILED" ), result[2] );
        assertEquals( doubleValue( 0.0 ), result[3] );
        assertEquals( stringValue( "NONUNIQUE" ), result[4] );
        assertEquals( stringValue( "BTREE" ), result[5] );
        assertEquals( stringValue( "NODE" ), result[6] );
        assertEquals( VirtualValues.list( stringValue( "Fail" ) ), result[7] );
        assertEquals( VirtualValues.list( stringValue( "foo" ) ), result[8] );
        assertEquals( stringValue( NATIVE_BTREE10.providerName() ), result[9] );
        assertMapsEqual( index.getIndexConfig().asMap(), (MapValue)result[10] );
        assertThat( ((TextValue) result[11]).stringValue() ).contains( "java.lang.RuntimeException: Fail on update during population" );
        assertEquals( 12, result.length );
    }

    @Test
    void indexDetailsWithNonExistingIndex()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> {
            procs().procedureCallRead( procs().procedureGet( procedureName( "db", "indexDetails" ) ).id(),
                    new TextValue[]{stringValue( "MyIndex" )},
                    ProcedureCallContext.EMPTY );
        } );
        assertEquals( exception.getMessage(), "Could not find index with name \"MyIndex\"" );
    }

    private void assertMapsEqual( Map<String,Value> expected, MapValue actual )
    {
        assertEquals( expected.size(), actual.size() );
        expected.forEach( ( k, v ) ->
        {
            final AnyValue value = actual.get( k );
            assertNotNull( value );
            assertEquals( v, value );
        } );
        actual.foreach( ( k, v ) ->
        {
            final Value value = expected.get( k );
            assertNotNull( value );
            assertEquals( v, value );
        } );
    }

    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( File databaseRootDir )
    {
        return super.createGraphDatabaseFactory( databaseRootDir )
                .removeExtensions( INDEX_PROVIDERS_FILTER )
                .noOpSystemGraphInitializer()
                .addExtension( new FailingGenericNativeIndexProviderFactory( POPULATION ) );
    }
}
