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
package org.neo4j.graphmatching;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Just a temporary utility for dealing with Neo4j properties that are arrays.
 * Since the Neo4j kernel arrays are returned as fundamental types, f.ex. int[],
 * float[], String[] etc... And we'd like to deal with those as objects instead
 * so that an equals method may be used.
 */
@Deprecated
public class ArrayPropertyUtil
{
    /**
     * @param propertyValue node.getProperty value.
     * @return a collection of all the values from a property. If the value is
     *         just a plain "single" value the collection will contain that
     *         single value. If the value is an array of values, all those
     *         values are added to the collection.
     */
    public static Collection<Object> propertyValueToCollection(
            Object propertyValue )
    {
        Set<Object> values = new HashSet<Object>();
        try
        {
            int length = Array.getLength( propertyValue );
            for ( int i = 0; i < length; i++ )
            {
                values.add( Array.get( propertyValue, i ) );
            }
        }
        catch ( IllegalArgumentException e )
        {
            values.add( propertyValue );
        }
        return values;
    }
}
