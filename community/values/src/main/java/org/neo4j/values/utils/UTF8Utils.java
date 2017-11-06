/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values.utils;

import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.UTF8StringValue;

import static org.neo4j.values.storable.Values.utf8Value;

/**
 * Utility class for operations on utf-8 values.
 */
public final class UTF8Utils
{
    private UTF8Utils()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    /**
     * Add two values.
     * @param a value to add
     * @param b value to add
     * @return the value a + b
     */
    public static TextValue add( UTF8StringValue a, UTF8StringValue b )
    {
        byte[] bytesA = a.bytes();
        byte[] bytesB = b.bytes();

        byte[] bytes = new byte[bytesA.length + bytesB.length];
        System.arraycopy( bytesA, 0, bytes, 0, bytesA.length );
        System.arraycopy( bytesB, 0, bytes, bytesA.length, bytesB.length );
        return utf8Value( bytes );
    }
}
