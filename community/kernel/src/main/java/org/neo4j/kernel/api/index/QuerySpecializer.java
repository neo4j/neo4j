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
package org.neo4j.kernel.api.index;

import org.neo4j.graphdb.Lookup;
import org.neo4j.kernel.api.Specialization;

public enum QuerySpecializer implements Lookup.Transformation<Specialization<Lookup>>
{
    DEFAULT;

    @Override
    public Specialization<Lookup> equalTo( Object value )
    {
        return new Specialization.None<>( Lookup.equalTo( value ) );
    }

    @Override
    public Specialization<Lookup> startsWith( String prefix )
    {
        return new Specialization.None<>( Lookup.startsWith( prefix ) );
    }

    @Override
    public Specialization<Lookup> endsWith( String suffix )
    {
        return new Specialization.None<>( Lookup.endsWith( suffix ) );
    }

//    @Override
//    public Specialization<Lookup> matches( String pattern )
//    {
//        return new Specialization.None<>( Lookup.matches( pattern ) );
//    }

    @Override
    public Specialization<Lookup> lessThan( Number value )
    {
        return new Specialization.None<Lookup>( Lookup.lessThan( value ) );
    }

    @Override
    public Specialization<Lookup> greaterThan( Number value )
    {
        return new Specialization.None<Lookup>( Lookup.greaterThan( value ) );
    }

    @Override
    public Specialization<Lookup> lessThanOrEqualTo( Number value )
    {
        return new Specialization.None<Lookup>( Lookup.lessThanOrEqualTo( value ) );
    }

    @Override
    public Specialization<Lookup> greaterThanOrEqualTo( Number value )
    {
        return new Specialization.None<Lookup>( Lookup.greaterThanOrEqualTo( value ) );
    }

    @Override
    public Specialization<Lookup> range( boolean includeLower, Number lower,
                                         Number upper, boolean includeUpper )
    {
        Lookup.LowerBound bound = includeLower ?
                                  Lookup.greaterThanOrEqualTo( lower ) :
                                  Lookup.greaterThan( lower );
        return new Specialization.None<>( includeUpper ?
                                          bound.andLessThanOrEqualTo( upper ) :
                                          bound.andLessThan( upper ) );
    }

    @Override
    public Specialization<Lookup> not( Specialization<Lookup> lookup )
    {
        return new Specialization.None<>( Lookup.not( lookup.genericForm() ) );
    }
}
