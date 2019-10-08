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

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureHandle;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TextArray;

import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.values.storable.Values.NO_VALUE;

abstract class CommunityProcedureITBase extends KernelIntegrationTest implements ProcedureITBase
{
    @Test
    void listProcedures() throws Throwable
    {
        // When
        ProcedureHandle procedures = procs().procedureGet( procedureName( "dbms", "procedures" ) );
        RawIterator<AnyValue[],ProcedureException> stream = procs().procedureCallRead( procedures.id(), new AnyValue[0], ProcedureCallContext.EMPTY );

        // Then
        List<AnyValue[]> actual = asList( stream );
        List<Object[]> expected = getExpectedCommunityProcs();
        Map<String,AnyValue[]> resultMap = actual.stream().collect( toMap( row -> ((StringValue) row[0]).stringValue(), Function.identity() ) );
        Map<String,Object[]> expectedMap = expected.stream().collect( toMap( row -> ((StringValue) row[0]).stringValue(), Function.identity() ) );
        assertThat( resultMap.keySet(), containsInAnyOrder( expectedMap.keySet().toArray() ) );
        for ( String procName : resultMap.keySet() )
        {
            AnyValue[] actualArray = resultMap.get( procName );
            Object[] expectedArray = expectedMap.get( procName );
            assertNotNull( expectedArray, "Got an unexpected entry for " + procName + " =>\n" + printElementsOfArray( actualArray ) );
            assertEquals( expectedArray.length, actualArray.length, "Count of columns for " + procName + " does not match" );

            for ( int i = 1; i < actualArray.length; i++ )
            {
                Matcher matcher;
                if ( expectedArray[i] instanceof TextArray )
                {
                    // this has to be a list of roles, we ignore those in community and expect a null here
                    matcher = equalTo( NO_VALUE );
                }
                else if ( expectedArray[i] instanceof Matcher )
                {
                    matcher = (Matcher) expectedArray[i];
                }
                else
                {
                    matcher = equalTo( expectedArray[i] );
                }
                assertThat( "Column " + i + " for " + procName + " does not match", actualArray[i], matcher );
            }
        }
        commit();
    }

    private String printElementsOfArray( AnyValue[] array )
    {
        StringBuilder result = new StringBuilder();
        for ( AnyValue anyValue : array )
        {
            result.append( anyValue ).append( "\n" );
        }
        return result.toString();
    }
}
