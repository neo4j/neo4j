/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.Lookup;

enum PredicateFactory implements Lookup.Transformation<PropertyPredicate>
{
    INSTANCE;

    @Override
    public PropertyPredicate equalTo( Object value )
    {
        return PropertyPredicate.equalTo( value );
    }

    @Override
    public PropertyPredicate startsWith( String prefix )
    {
        return PropertyPredicate.startsWith( prefix );
    }

    @Override
    public PropertyPredicate endsWith( String suffix )
    {
        return PropertyPredicate.endsWith( suffix );
    }

    @Override
    public PropertyPredicate.NumberPredicate lessThan( Number value )
    {
        if ( value instanceof Double || value instanceof Float )
        {
            return PropertyPredicate.lessThan( value.doubleValue() );
        }
        else
        {
            return PropertyPredicate.lessThan( value.longValue() );
        }
    }

    @Override
    public PropertyPredicate.NumberPredicate greaterThan( Number value )
    {
        if ( value instanceof Double || value instanceof Float )
        {
            return PropertyPredicate.greaterThan( value.doubleValue() );
        }
        else
        {
            return PropertyPredicate.greaterThan( value.longValue() );
        }
    }

    @Override
    public PropertyPredicate.NumberPredicate lessThanOrEqualTo( Number value )
    {
        if ( value instanceof Double || value instanceof Float )
        {
            return PropertyPredicate.lessThanOrEqualTo( value.doubleValue() );
        }
        else
        {
            return PropertyPredicate.lessThanOrEqualTo( value.longValue() );
        }
    }

    @Override
    public PropertyPredicate.NumberPredicate greaterThanOrEqualTo( Number value )
    {
        if ( value instanceof Double || value instanceof Float )
        {
            return PropertyPredicate.greaterThanOrEqualTo( value.doubleValue() );
        }
        else
        {
            return PropertyPredicate.greaterThanOrEqualTo( value.longValue() );
        }
    }

    @Override
    public PropertyPredicate range( boolean includeLower, Number lower, Number upper, boolean includeUpper )
    {
        PropertyPredicate.NumberPredicate low = includeLower ? greaterThanOrEqualTo( lower ) : greaterThan( lower );
        PropertyPredicate.NumberPredicate upp = includeUpper ? lessThanOrEqualTo( upper ) : lessThan( upper );
        return PropertyPredicate.and( low, upp );
    }

    @Override
    public PropertyPredicate not( PropertyPredicate lookup )
    {
        return PropertyPredicate.not( lookup );
    }
}
