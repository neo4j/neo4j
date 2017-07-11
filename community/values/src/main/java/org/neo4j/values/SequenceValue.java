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
package org.neo4j.values;
/**
 * Values that represent sequences of values (such as Lists or Arrays) need to implement this interface.
 * Thus we can get an equality check that is based on the values (e.g. List.equals(ArrayValue) )
 * Values that implement this interface also need to overwrite isSequence() to return true!
 */
public interface SequenceValue
{
    default boolean equals( SequenceValue other )
    {
        if ( other == null )
        {
            return false;
        }

        if ( this.length() != other.length() )
        {
            return false;
        }

        for ( int i = 0; i < this.length(); i++ )
        {
            AnyValue myValue = this.value( i );
            AnyValue otherValue = other.value( i );
            if ( !myValue.equals( otherValue ) )
            {
                return false;
            }
        }
        return true;
    }

    AnyValue value( int offset );

    int length();
}
