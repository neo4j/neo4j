/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.store.prototype.neole;

import java.util.regex.Pattern;

import org.neo4j.internal.store.cursors.ReadCursor;
import org.neo4j.values.storable.ValueWriter;

public abstract class PartialPropertyCursor extends ReadCursor
        implements org.neo4j.internal.kernel.api.PropertyCursor
{
    static final long NO_PROPERTIES = -1;

    PartialPropertyCursor()
    {
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> target )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean booleanValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public String stringValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long longValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public double doubleValue()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueEqualTo( long value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueEqualTo( double value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueEqualTo( String value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueMatches( Pattern regex )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThan( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThan( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThan( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThan( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThanOrEqualTo( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueGreaterThanOrEqualTo( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThanOrEqualTo( long number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean valueLessThanOrEqualTo( double number )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
