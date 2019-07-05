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
package org.neo4j.internal.schema;

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexConfigTest
{
    @Test
    void addingAndGetting()
    {
        IndexConfig config = IndexConfig.empty();
        config = config.withIfAbsent( "a", BooleanValue.TRUE );
        assertTrue( config.<BooleanValue>get( "a" ).booleanValue() );

        config = config.withIfAbsent( "a", BooleanValue.FALSE );
        assertTrue( config.<BooleanValue>get( "a" ).booleanValue() );

        assertNull( config.get( "b" ) );
        assertFalse( config.getOrDefault( "b", BooleanValue.FALSE ).booleanValue() );
    }

    @SuppressWarnings( "ConstantConditions" )
    @Test
    void shouldNotBePossibleToMutateIndexConfigFromAsMap()
    {
        IndexConfig config = IndexConfig.empty();
        config = config.withIfAbsent( "a", BooleanValue.TRUE );
        config = config.withIfAbsent( "b", BooleanValue.TRUE );

        Map<String,Value> map = config.asMap();
        assertThrows( UnsupportedOperationException.class, () -> map.remove( "a" ) );
        assertThrows( UnsupportedOperationException.class, () -> map.put( "b", BooleanValue.FALSE ) );
        assertThrows( UnsupportedOperationException.class, () -> map.put( "c", BooleanValue.TRUE ) );

        assertTrue( config.<BooleanValue>get( "a" ).booleanValue() );
        assertTrue( config.<BooleanValue>get( "b" ).booleanValue() );
        assertNull( config.get( "c" ) );
    }
}
