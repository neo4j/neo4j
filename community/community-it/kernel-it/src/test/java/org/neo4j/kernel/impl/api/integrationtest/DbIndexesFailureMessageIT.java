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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory.FailureType.POPULATION;
import static org.neo4j.test.TestGraphDatabaseFactory.INDEX_PROVIDERS_FILTER;

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
        LabelSchemaDescriptor descriptor = forLabel( failedLabel, propertyKeyId1 );
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
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "indexes" ) ).id(),
                        new Object[0], ProcedureCallContext.EMPTY );
        assertTrue( stream.hasNext() );
        Object[] result = stream.next();
        assertFalse( stream.hasNext() );

        // Then
        assertEquals( "INDEX ON :Fail(foo)", result[0] );
        assertEquals( "Unnamed index", result[1] );
        assertEquals( Collections.singletonList( "Fail" ), result[2] );
        assertEquals( Collections.singletonList( "foo" ), result[3] );
        assertEquals( "FAILED", result[4] );
        assertEquals( "node_label_property", result[5] );
        assertEquals( 0.0, result[6] );
        Map<String,String> providerDescriptionMap = MapUtil.stringMap(
                "key", GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerKey(),
                "version", GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10.providerVersion() );
        assertEquals( providerDescriptionMap, result[7] );
        assertEquals( indexingService.getIndexId( descriptor ), result[8] );
        assertThat( (String) result[9], containsString( "java.lang.RuntimeException: Fail on update during population" ) );

        commit();
    }

    @Override
    protected TestGraphDatabaseFactory createGraphDatabaseFactory()
    {
        return super.createGraphDatabaseFactory()
                .removeKernelExtensions( INDEX_PROVIDERS_FILTER )
                .addKernelExtension( new FailingGenericNativeIndexProviderFactory( POPULATION ) );
    }
}
