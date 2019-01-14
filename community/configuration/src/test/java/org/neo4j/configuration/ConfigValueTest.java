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
package org.neo4j.configuration;

import org.junit.Test;

import java.time.Duration;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.ConfigValue.valueToString;

public class ConfigValueTest
{
    @Test
    public void handlesEmptyValue()
    {
        ConfigValue value = new ConfigValue( "name", Optional.empty(), Optional.empty(), Optional.empty(),
                "description", false, false, false, Optional.empty(), false );

        assertEquals( Optional.empty(), value.value() );
        assertEquals( "null", value.toString() );
        assertFalse( value.deprecated() );
        assertEquals( Optional.empty(), value.replacement() );
        assertFalse( value.internal() );
        assertFalse( value.secret() );
    }

    @Test
    public void handlesInternal()
    {
        ConfigValue value = new ConfigValue( "name", Optional.empty(), Optional.empty(), Optional.empty(),
                "description", true, false, false, Optional.empty(), false );

        assertTrue( value.internal() );
        assertFalse( value.secret() );
    }

    @Test
    public void handlesNonEmptyValue()
    {
        ConfigValue value = new ConfigValue( "name", Optional.empty(), Optional.empty(), Optional.of( 1 ),
                "description", false, false, false, Optional.empty(), false );

        assertEquals( Optional.of( 1 ), value.value() );
        assertEquals( "1", value.toString() );
        assertFalse( value.deprecated() );
        assertEquals( Optional.empty(), value.replacement() );
        assertFalse( value.internal() );
        assertFalse( value.secret() );
    }

    @Test
    public void handlesDeprecationAndReplacement()
    {
        ConfigValue value = new ConfigValue( "old_name", Optional.empty(), Optional.empty(), Optional.of( 1 ),
                "description", false, false, true, Optional.of( "new_name" ), false );

        assertEquals( Optional.of( 1 ), value.value() );
        assertEquals( "1", value.toString() );
        assertTrue( value.deprecated() );
        assertEquals( "new_name", value.replacement().get() );
        assertFalse( value.internal() );
        assertFalse( value.secret() );
    }

    @Test
    public void handlesValueDescription()
    {
        ConfigValue value = new ConfigValue( "old_name", Optional.empty(), Optional.empty(), Optional.of( 1 ),
                "a simple integer", false, false, true, Optional.of( "new_name" ), false );

        assertEquals( Optional.of( 1 ), value.value() );
        assertEquals( "1", value.toString() );
        assertTrue( value.deprecated() );
        assertEquals( "new_name", value.replacement().get() );
        assertFalse( value.internal() );
        assertFalse( value.secret() );
        assertEquals( "a simple integer", value.valueDescription() );
    }

    @Test
    public void handlesSecretValue() throws Exception
    {
        ConfigValue value = new ConfigValue( "name", Optional.empty(), Optional.empty(), Optional.of( "secret" ),
                "description", false, false, false, Optional.empty(), true );

        assertEquals( Optional.of( "secret" ), value.value() );
        assertEquals( Secret.OBSFUCATED, value.toString() );
        assertFalse( value.deprecated() );
        assertEquals( Optional.empty(), value.replacement() );
        assertFalse( value.internal() );
        assertTrue( value.secret() );
    }

    @Test
    public void durationValueIsRepresentedWithUnit()
    {
        assertEquals( "120000ms", valueToString( Duration.ofMinutes( 2 ) ) );
    }

    @Test
    public void stringValueIsRepresentedAsString()
    {
        assertEquals( "bob", valueToString( "bob" ) );
    }

    @Test
    public void intValueIsRepresentedAsInt()
    {
        assertEquals( "7", valueToString( 7 ) );
    }

    @Test
    public void nullIsHandled()
    {
        assertEquals( "null", valueToString( null ) );
    }
}
