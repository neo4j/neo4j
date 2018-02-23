/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.collection.primitive.hopscotch;

import org.junit.jupiter.api.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PrimitiveIntObjectHashMapTest
{

    @Test
    public void doNotComputeValueIfPresent()
    {
        PrimitiveIntObjectMap<Object> intObjectMap = Primitive.intObjectMap();
        intObjectMap.put( 1, "a" );
        assertEquals("a", intObjectMap.computeIfAbsent( 1, value -> "b" ));
    }

    @Test
    public void computeValueIfAbsent()
    {
        PrimitiveIntObjectMap<Object> intObjectMap = Primitive.intObjectMap();
        intObjectMap.put( 1, "a" );
        assertEquals("b", intObjectMap.computeIfAbsent( 2, value -> "b" ));
    }

}
