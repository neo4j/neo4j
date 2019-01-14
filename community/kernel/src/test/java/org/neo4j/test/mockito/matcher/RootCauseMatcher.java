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
package org.neo4j.test.mockito.matcher;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class RootCauseMatcher<T extends Throwable> extends TypeSafeMatcher<T>
{
    private final Class<T> rootCause;
    private final String message;
    private Throwable cause;

    public RootCauseMatcher( Class<T> rootCause )
    {
        this( rootCause, StringUtils.EMPTY );
    }

    public RootCauseMatcher( Class<T> rootCause, String message )
    {
        this.rootCause = rootCause;
        this.message = message;
    }

    @Override
    protected boolean matchesSafely( T item )
    {
        cause = ExceptionUtils.getRootCause( item );
        return rootCause.isInstance( cause ) && cause.getMessage().startsWith( message );
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( "Expected root cause of " ).appendValue( rootCause ).appendText( " with message: " )
                .appendValue( message ).appendText( ", but " );
        if ( cause != null )
        {
            description.appendText( "was: " ).appendValue( cause.getClass() )
                    .appendText( " with message: " ).appendValue( cause.getMessage() );
        }
        else
        {
            description.appendText( "actual exception was never thrown." );
        }
    }
}
