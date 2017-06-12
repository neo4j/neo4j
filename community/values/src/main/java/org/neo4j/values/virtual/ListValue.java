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
package org.neo4j.values.virtual;

import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.VirtualValue;

import static org.neo4j.values.virtual.ArrayHelpers.hasNullOrNoValue;

final class ListValue extends VirtualValue
{
    private final AnyValue[] values;

    ListValue( AnyValue[] values )
    {
        assert values != null;
        assert !hasNullOrNoValue( values );

        this.values = values;
    }

    @Override
    public boolean equals( VirtualValue other )
    {
        if ( other == null || !(other instanceof ListValue) )
        {
            return false;
        }
        ListValue that = (ListValue) other;
        return size() == that.size() &&
                Arrays.equals( values, that.values );
    }

    @Override
    public int hash()
    {
        return Arrays.hashCode( values );
    }

    @Override
    public void writeTo( AnyValueWriter writer )
    {
        writer.beginList( values.length );
        for ( AnyValue value : values )
        {
            value.writeTo( writer );
        }
        writer.endList();
    }

    @Override
    public VirtualValueGroup valueGroup()
    {
        return VirtualValueGroup.LIST;
    }

    @Override
    public int compareTo( VirtualValue other, Comparator<AnyValue> comparator )
    {
        if ( !(other instanceof ListValue) )
        {
            throw new IllegalArgumentException( "Cannot compare different virtual values" );
        }
        ListValue otherList = (ListValue) other;
        int x = Integer.compare( this.size(), otherList.size() );

        if ( x == 0 )
        {
            for ( int i = 0; i < size(); i++ )
            {
                x = comparator.compare( this.values[i], otherList.values[i] );
                if ( x != 0 )
                {
                    return x;
                }
            }
        }

        return x;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "List{" );
        int i = 0;
        for ( ; i < values.length - 1; i++ )
        {
            sb.append( values[i] );
            sb.append( ", " );
        }
        if ( values.length > 0 )
        {
            sb.append( values[i] );
        }
        sb.append( '}' );
        return sb.toString();
    }

    public int size()
    {
        return values.length;
    }

    public AnyValue value( int offset )
    {
        return values[offset];
    }
}
