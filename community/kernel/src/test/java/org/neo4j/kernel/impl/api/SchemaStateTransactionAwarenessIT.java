/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.Functions.constant;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.collection.MapUtil;

public class SchemaStateTransactionAwarenessIT
{

    @Test
    public void shouldImplementTestsForSchemaStateFlushingInTx() {
        throw new UnsupportedOperationException(  );
    }

//    private KernelSchemaStateStore schemaStateStore;
//    private TransactionalSchemaStateImpl txSchemaState;
//
//    private Function<String, String> txShouldNotCreate = functionThrows( "tx" );
//
//    @Test
//    public void should_not_create_in_underlying_holder()
//    {
//        // GIVEN
//        txSchemaState.getOrCreate( "key", constant( "created_value" ) );
//
//        // WHEN
//        String txResult = txSchemaState.getOrCreate( "key", txShouldNotCreate );
//
//        // THEN
//        assertEquals( "created_value", txResult );
//        assertEquals( null, schemaStateStore.get( "key" ) );
//    }
//
//    @Test
//    public void should_get_from_underlying_holder()
//    {
//        // GIVEN
//        schemaStateStore.apply( MapUtil.stringMap( "key", "original_value" ) );
//
//        // WHEN
//        String txResult = txSchemaState.getOrCreate( "key", txShouldNotCreate );
//        String kernelResult = schemaStateStore.get( "key" );
//
//        // THEN
//        assertEquals( "original_value", kernelResult );
//        assertEquals( "original_value", txResult );
//    }
//
//    @Test
//    public void flush_does_not_apply_to_underlying_before_commit()
//    {
//        // GIVEN
//        schemaStateStore.apply( MapUtil.stringMap( "key", "original_value" ) );
//        txSchemaState.getOrCreate( "key", constant( "updated_value" ) );
//
//        // WHEN
//        txSchemaState.flush();
//
//        // THEN
//        String kernelResult = schemaStateStore.get( "key" );
//        assertEquals( "original_value", kernelResult );
//
//        // WHEN
//        String txResult = txSchemaState.getOrCreate( "key",constant( "recreated_value" ) );
//
//        // THEN
//        assertEquals( "recreated_value", txResult );
//    }
//
//    @Test
//    public void flush_does_apply_to_underlying_on_commit()
//    {
//        // GIVEN
//        schemaStateStore.apply( MapUtil.stringMap( "key", "original_value" ) );
//        txSchemaState.getOrCreate( "key", txShouldNotCreate );
//
//        // WHEN
//        txSchemaState.flush();
//        txSchemaState.commit();
//
//        // THEN
//        String kernelResult = schemaStateStore.get( "key" );
//        assertEquals( null, kernelResult );
//    }
//
//    public Function<String, String> functionThrows( final String msg )
//    {
//        return new Function<String, String>()
//        {
//            @Override
//            public String apply( String s )
//            {
//                throw new IllegalStateException( msg );
//            }
//        };
//    }
//
//    @Before
//    public void before()
//    {
//        schemaStateStore = new KernelSchemaStateStore();
//        txSchemaState = new TransactionalSchemaStateImpl( schemaStateStore );
//    }
}
