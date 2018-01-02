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
package org.neo4j.test;

import org.neo4j.function.Function;

/**
 * @deprecated This class will be removed in the next major release.
 */
@Deprecated
public abstract class AlgebraicFunction<FROM, TO> implements Function<FROM, TO>
{
    public <NEXT> AlgebraicFunction<FROM, NEXT> then( final Function<TO, NEXT> function )
    {
        return new AlgebraicFunction<FROM, NEXT>( repr + " then " + function.toString() )
        {
            @Override
            public NEXT apply( FROM from )
            {
                return function.apply( AlgebraicFunction.this.apply( from ) );
            }
        };
    }

    public AlgebraicFunction()
    {
        String repr = getClass().getSimpleName();
        if ( getClass().isAnonymousClass() )
        {
            for ( StackTraceElement trace : Thread.currentThread().getStackTrace() )
            {
                if ( trace.getClassName().equals( Thread.class.getName() ) &&
                     trace.getMethodName().equals( "getStackTrace" ) )
                {
                    continue;
                }
                if ( trace.getClassName().equals( AlgebraicFunction.class.getName() ) )
                {
                    continue;
                }
                if ( trace.getClassName().equals( getClass().getName() ) )
                {
                    continue;
                }
                repr = trace.getMethodName();
                break;
            }
        }
        this.repr = repr;
    }

    private final String repr;

    private AlgebraicFunction( String repr )
    {
        this.repr = repr;
    }

    @Override
    public String toString()
    {
        return repr;
    }
}
