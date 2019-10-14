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
package org.neo4j.kernel.api.index;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IndexConfigProviderTest
{
    @Test
    void putAllNoOverwriteMustAddToSource()
    {
        Map<String,Value> target = new HashMap<>();
        Map<String,Value> source = new HashMap<>();
        target.put( "a", Values.intValue( 1 ) );
        source.put( "b", Values.intValue( 2 ) );
        IndexConfigProvider.putAllNoOverwrite( target, source );
        assertEquals( 2, target.size() );
        assertEquals( Values.intValue( 1 ), target.get( "a" ) );
        assertEquals( Values.intValue( 2 ), target.get( "b" ) );
        assertEquals( 1, source.size() );
        assertEquals( Values.intValue( 2 ), source.get( "b" ) );
    }

    @Test
    void putAllNoOverwriteMustThrowOnConflict()
    {
        Map<String,Value> target = new HashMap<>();
        Map<String,Value> source = new HashMap<>();
        target.put( "a", Values.intValue( 1 ) );
        source.put( "a", Values.intValue( 2 ) );
        IllegalStateException e = assertThrows( IllegalStateException.class, () -> IndexConfigProvider.putAllNoOverwrite( target, source ) );
        assertEquals( "Adding config would overwrite existing value: key=a, newValue=Int(2), oldValue=Int(1)", e.getMessage() );
    }
}
