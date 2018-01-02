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
package org.neo4j.values.storable;

import org.neo4j.values.virtual.ListValue;

import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.virtual.VirtualValues.fromArray;

public abstract class TextValue extends ScalarValue
{
    protected static final ListValue EMPTY_SPLIT = fromArray( stringArray( "", "" ) );

    TextValue()
    {
    }

    public abstract String stringValue();

    /**
     * The length of a TextValue is the number of Unicode code points in the text.
     *
     * @return The number of Unicode code points.
     */
    public abstract int length();

    public abstract TextValue substring( int start, int length );

    public TextValue substring( int start )
    {
        return substring( start, Math.max( length() - start, start ) );
    }

    public abstract TextValue trim();

    public abstract TextValue ltrim();

    public abstract TextValue rtrim();

    public abstract TextValue toLower();

    public abstract TextValue toUpper();

    public abstract ListValue split( String separator );

    public abstract TextValue replace( String find, String replace );

    public abstract TextValue reverse();

    public abstract int compareTo( TextValue other );

    @Override
    public boolean equals( boolean x )
    {
        return false;
    }

    @Override
    public boolean equals( long x )
    {
        return false;
    }

    @Override
    public boolean equals( double x )
    {
        return false;
    }

    public ValueGroup valueGroup()
    {
        return ValueGroup.TEXT;
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }
}
