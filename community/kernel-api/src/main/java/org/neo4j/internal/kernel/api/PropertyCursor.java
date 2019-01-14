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
package org.neo4j.internal.kernel.api;

import java.util.regex.Pattern;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;

/**
 * Cursor for scanning the properties of a node or relationship.
 */
public interface PropertyCursor extends Cursor
{
    int propertyKey();

    ValueGroup propertyType();

    Value propertyValue();

    <E extends Exception> void writeTo( ValueWriter<E> target );

    // typed accessor methods

    boolean booleanValue();

    String stringValue();

    long longValue();

    double doubleValue();

    // Predicates methods that don't require de-serializing the value

    boolean valueEqualTo( long value );

    boolean valueEqualTo( double value );

    boolean valueEqualTo( String value );

    boolean valueMatches( Pattern regex );

    boolean valueGreaterThan( long number );

    boolean valueGreaterThan( double number );

    boolean valueLessThan( long number );

    boolean valueLessThan( double number );

    boolean valueGreaterThanOrEqualTo( long number );

    boolean valueGreaterThanOrEqualTo( double number );

    boolean valueLessThanOrEqualTo( long number );

    boolean valueLessThanOrEqualTo( double number );
}
