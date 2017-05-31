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
package org.neo4j.kernel.api.properties;

import org.neo4j.helpers.MathUtil;

abstract class FloatingPointNumberProperty extends NumberProperty
{
    FloatingPointNumberProperty( int propertyKeyId )
    {
        super( propertyKeyId );
    }

    @Override
    public final int valueHash()
    {
        long value = (long) doubleValue();
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public final boolean valueEquals( Object other )
    {
        if ( other instanceof Number )
        {
            Number that = (Number) other;
            if ( other instanceof Double || other instanceof Float )
            {
                return this.doubleValue() == that.doubleValue();
            }
            else
            {
                return MathUtil.numbersEqual( this.doubleValue(), that.longValue() );
            }
        }
        return false;
    }

    @Override
    public final boolean hasEqualValue( DefinedProperty other )
    {
        if ( other instanceof FloatingPointNumberProperty )
        {
            FloatingPointNumberProperty that = (FloatingPointNumberProperty) other;
            return this.doubleValue() == that.doubleValue();
        }
        else if ( other instanceof IntegralNumberProperty )
        {
            IntegralNumberProperty that = (IntegralNumberProperty) other;
            return MathUtil.numbersEqual( this.doubleValue(), that.longValue() );
        }
        else
        {
            return false;
        }
    }
}
