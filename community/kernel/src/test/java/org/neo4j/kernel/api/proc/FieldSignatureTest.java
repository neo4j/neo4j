/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.procs.Neo4jTypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntString;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;

class FieldSignatureTest
{
    @Test
    void equalsShouldConsiderName()
    {
        assertEquals( inputField( "name", Neo4jTypes.NTString ),
                inputField( "name", Neo4jTypes.NTString ), "input without default" );
        assertNotEquals( inputField( "name", Neo4jTypes.NTString ),
                inputField( "other", Neo4jTypes.NTString ), "input without default" );

        assertEquals( inputField( "name", Neo4jTypes.NTString, ntString( "hello" ) ),
                inputField( "name", Neo4jTypes.NTString, ntString( "hello" ) ), "input with default" );
        assertNotEquals( inputField( "name", Neo4jTypes.NTString, ntString( "hello" ) ),
                inputField( "other", Neo4jTypes.NTString, ntString( "hello" ) ), "input with default" );

        assertEquals( outputField( "name", Neo4jTypes.NTString, false ),
                outputField( "name", Neo4jTypes.NTString, false ), "output" );
        assertNotEquals( outputField( "name", Neo4jTypes.NTString, false ),
                outputField( "other", Neo4jTypes.NTString, false ), "output" );

        assertEquals( outputField( "name", Neo4jTypes.NTString, true ),
                outputField( "name", Neo4jTypes.NTString, true ), "deprecated output" );
        assertNotEquals( outputField( "name", Neo4jTypes.NTString, true ),
                outputField( "other", Neo4jTypes.NTString, true ), "deprecated output" );
    }

    @Test
    void shouldTypeCheckDefaultValue()
    {
        // when
        IllegalArgumentException exception =
                assertThrows( IllegalArgumentException.class, () -> inputField( "name", Neo4jTypes.NTInteger, ntString( "bad" ) ) );
        assertEquals( exception.getMessage(),
                    "Default value does not have a valid type, field type was INTEGER?, but value type was STRING?." );
    }

    @Test
    void equalsShouldConsiderType()
    {
        assertEquals( inputField( "name", Neo4jTypes.NTString ),
                inputField( "name", Neo4jTypes.NTString ), "input without default" );
        assertNotEquals( inputField( "name", Neo4jTypes.NTString ),
                inputField( "name", Neo4jTypes.NTInteger ), "input without default" );

        assertEquals( outputField( "name", Neo4jTypes.NTString, false ),
                outputField( "name", Neo4jTypes.NTString, false ), "output" );
        assertNotEquals( outputField( "name", Neo4jTypes.NTString, false ),
                outputField( "name", Neo4jTypes.NTInteger, false ), "output" );

        assertEquals( outputField( "name", Neo4jTypes.NTString, true ),
                outputField( "name", Neo4jTypes.NTString, true ), "deprecated output" );
        assertNotEquals( outputField( "name", Neo4jTypes.NTString, true ),
                outputField( "name", Neo4jTypes.NTInteger, true ), "deprecated output" );
    }

    @Test
    void equalsShouldConsiderDefaultValue()
    {
        assertEquals(
                inputField( "name", Neo4jTypes.NTString, ntString( "foo" ) ),
                inputField( "name", Neo4jTypes.NTString, ntString( "foo" ) ) );
        assertNotEquals(
                inputField( "name", Neo4jTypes.NTString, ntString( "bar" ) ),
                inputField( "name", Neo4jTypes.NTString, ntString( "baz" ) ) );
    }

    @Test
    void equalsShouldConsiderDeprecation()
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
