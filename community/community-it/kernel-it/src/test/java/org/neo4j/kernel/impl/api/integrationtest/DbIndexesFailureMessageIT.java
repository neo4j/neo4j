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

import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.schema.DefaultLabelSchemaDescriptor;
import org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.internal.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory.FailureType.POPULATION;
import static org.neo4j.test.TestDatabaseManagementServiceBuilder.INDEX_PROVIDERS_FILTER;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

public class DbIndexesFailureMessageIT extends KernelIntegrationTest
{
    private AtomicBoolean failNextIndexPopulation = new AtomicBoolean();

    @Test
    public void listAllIndexesWithFailedIndex() throws Throwable
    {
        // Given
        Transaction transaction = newTransaction( AUTH_DISABLED );
        int failedLabel = transaction.tokenWrite().labelGetOrCreateForName( "Fail" );
        int propertyKeyId1 = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" );
        failNextIndexPopulation.set( true );
        DefaultLabelSchemaDescriptor descriptor = forLabel( failedLabel, propertyKeyId1 );
        transaction.schemaWrite().indexCreate( descriptor );
        commit();

        //let indexes come online
        try ( org.neo4j.graphdb.Transaction ignored = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 2, MINUTES );
            fail( "Expected to fail when awaiting for index to come online" );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "indexes" ) ).id(),
                        new AnyValue[0] );
        assertTrue( stream.hasNext() );
        AnyValue[] result = stream.next();
        assertFalse( stream.hasNext() );

        // Then
        StoreIndexDescriptor index = (StoreIndexDescriptor) transaction.schemaRead().index( descriptor );
        assertEquals( stringValue( "INDEX ON :Fail(foo)" ), result[0] );
        assertEquals( stringValue( "index_" + index.getId() ), result[1] );
        assertEquals( VirtualValues.list( stringValue( "Fail" ) ), result[2] );
        assertEquals( VirtualValues.list( stringValue( "foo" ) ), result[3] );
        assertEquals( stringValue( "FAILED" ), result[4] );
        assertEquals( stringValue( "node_label_property" ), result[5] );
        assertEquals( doubleValue( 0.0 ), result[6] );
        MapValue providerDescriptionMap = ValueUtils.asMapValue( MapUtil.map(
                "key", GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerKey(),
                "version", GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerVersion() ) );
        assertEquals( providerDescriptionMap, result[7] );
        assertEquals( longValue( indexingService.getIndexId( descriptor ) ), result[8] );
        assertThat( ((TextValue) result[9]).stringValue(),
                containsString( "java.lang.RuntimeException: Fail on update during population" ) );

        commit();
    }

    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( File databaseRootDir )
    {
        return super.createGraphDatabaseFactory( databaseRootDir )
                .removeExtensions( INDEX_PROVIDERS_FILTER )
                .addExtension( new FailingGenericNativeIndexProviderFactory( POPULATION ) );
    }
}
