/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Arrays;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

public interface IndexValueValidator
{
    void validate( long entityId, Value... values );

    static void throwSizeViolationException( IndexDescriptor descriptor, TokenNameLookup tokenNameLookup, long entityId, int size,
            Value... values )
    {
        String valueString = Arrays.toString( values );
        if ( valueString.length() > 100 )
        {
            valueString = valueString.substring( 0, 100 ) + "...";
        }
        throw new IllegalArgumentException( format(
                "Property value is too large to index, please see index documentation for limitations. Index: %s, entity id: %d, property size: %d, value: %s.",
                descriptor.userDescription( tokenNameLookup ), entityId, size, valueString ) );
    }
}
