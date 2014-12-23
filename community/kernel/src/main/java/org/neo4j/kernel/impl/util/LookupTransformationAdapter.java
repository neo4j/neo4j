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
package org.neo4j.kernel.impl.util;

import org.neo4j.graphdb.Lookup;

public abstract class LookupTransformationAdapter<FROM, TRANSFORMATION extends Lookup.Transformation<FROM>, TO>
        implements Lookup.Transformation<TO>
{
    private final TRANSFORMATION as;

    public LookupTransformationAdapter( TRANSFORMATION as )
    {
        this.as = as;
    }

    protected final TRANSFORMATION transformation()
    {
        return as;
    }

    protected abstract TO transformed( FROM source );

    @Override
    public final TO equalTo( Object value )
    {
        return transformed( as.equalTo( value ) );
    }

    @Override
    public final TO startsWith( String prefix )
    {
        return transformed( as.startsWith( prefix ) );
    }

    @Override
    public final TO endsWith( String suffix )
    {
        return transformed( as.endsWith( suffix ) );
    }

//    @Override
//    public final TO matches( String pattern )
//    {
//        return transform( as.matches( pattern ) );
//    }

    @Override
    public final TO lessThan( Number value )
    {
        return transformed( as.lessThan( value ) );
    }

    @Override
    public final TO greaterThan( Number value )
    {
        return transformed( as.greaterThan( value ) );
    }

    @Override
    public final TO lessThanOrEqualTo( Number value )
    {
        return transformed( as.lessThanOrEqualTo( value ) );
    }

    @Override
    public final TO greaterThanOrEqualTo( Number value )
    {
        return transformed( as.greaterThanOrEqualTo( value ) );
    }

    @Override
    public TO range( boolean includeLower, Number lower, Number upper, boolean includeUpper )
    {
        return transformed( as.range( includeLower, lower, upper, includeUpper ) );
    }

    protected final FROM negated( FROM lookup )
    {
        return as.not( lookup );
    }
}
