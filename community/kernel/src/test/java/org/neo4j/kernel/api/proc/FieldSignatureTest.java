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
package org.neo4j.kernel.api.proc;

import org.junit.Test;

import org.neo4j.internal.kernel.api.procs.Neo4jTypes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntString;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;

public class FieldSignatureTest
{
    @Test
    public void equalsShouldConsiderName()
    {
        assertEquals(
                "input without default",
                inputField( "name", Neo4jTypes.NTString ),
                inputField( "name", Neo4jTypes.NTString ) );
        assertNotEquals(
                "input without default",
                inputField( "name", Neo4jTypes.NTString ),
                inputField( "other", Neo4jTypes.NTString ) );

        assertEquals(
                "input with default",
                inputField( "name", Neo4jTypes.NTString, ntString( "hello" ) ),
                inputField( "name", Neo4jTypes.NTString, ntString( "hello" ) ) );
        assertNotEquals(
                "input with default",
                inputField( "name", Neo4jTypes.NTString, ntString( "hello" ) ),
                inputField( "other", Neo4jTypes.NTString, ntString( "hello" ) ) );

        assertEquals(
                "output",
                outputField( "name", Neo4jTypes.NTString, false ),
                outputField( "name", Neo4jTypes.NTString, false ) );
        assertNotEquals(
                "output",
                outputField( "name", Neo4jTypes.NTString, false ),
                outputField( "other", Neo4jTypes.NTString, false ) );

        assertEquals(
                "deprecated output",
                outputField( "name", Neo4jTypes.NTString, true ),
                outputField( "name", Neo4jTypes.NTString, true ) );
        assertNotEquals(
                "deprecated output",
                outputField( "name", Neo4jTypes.NTString, true ),
                outputField( "other", Neo4jTypes.NTString, true ) );
    }

    @Test
    public void shouldTypeCheckDefaultValue()
    {
        // when
        try
        {
            inputField( "name", Neo4jTypes.NTInteger, ntString( "bad" ) );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalArgumentException e )
        {
            assertEquals(
                    e.getMessage(),
                    "Default value does not have a valid type, field type was INTEGER?, but value type was STRING?." );
        }
    }

    @Test
    public void equalsShouldConsiderType()
    {
        assertEquals(
                "input without default",
                inputField( "name", Neo4jTypes.NTString ),
                inputField( "name", Neo4jTypes.NTString ) );
        assertNotEquals(
                "input without default",
                inputField( "name", Neo4jTypes.NTString ),
                inputField( "name", Neo4jTypes.NTInteger ) );

        assertEquals(
                "output",
                outputField( "name", Neo4jTypes.NTString, false ),
                outputField( "name", Neo4jTypes.NTString, false ) );
        assertNotEquals(
                "output",
                outputField( "name", Neo4jTypes.NTString, false ),
                outputField( "name", Neo4jTypes.NTInteger, false ) );

        assertEquals(
                "deprecated output",
                outputField( "name", Neo4jTypes.NTString, true ),
                outputField( "name", Neo4jTypes.NTString, true ) );
        assertNotEquals(
                "deprecated output",
                outputField( "name", Neo4jTypes.NTString, true ),
                outputField( "name", Neo4jTypes.NTInteger, true ) );
    }

    @Test
    public void equalsShouldConsiderDefaultValue()
    {
        assertEquals(
                inputField( "name", Neo4jTypes.NTString, ntString( "foo" ) ),
                inputField( "name", Neo4jTypes.NTString, ntString( "foo" ) ) );
        assertNotEquals(
                inputField( "name", Neo4jTypes.NTString, ntString( "bar" ) ),
                inputField( "name", Neo4jTypes.NTString, ntString( "baz" ) ) );
    }

    @Test
    public void equalsShouldConsiderDeprecation()
    {
        assertEquals(
                outputField( "name", Neo4jTypes.NTString, true ),
                outputField( "name", Neo4jTypes.NTString, true ) );
        assertEquals(
                outputField( "name", Neo4jTypes.NTString, false ),
                outputField( "name", Neo4jTypes.NTString, false ) );
        assertNotEquals(
                outputField( "name", Neo4jTypes.NTString, true ),
                outputField( "name", Neo4jTypes.NTString, false ) );
    }
}
