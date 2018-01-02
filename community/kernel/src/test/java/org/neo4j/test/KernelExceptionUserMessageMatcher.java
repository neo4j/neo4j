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

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;

public class KernelExceptionUserMessageMatcher<T extends SchemaKernelException> extends BaseMatcher<T>
{
    private TokenNameLookup tokenNameLookup;
    private String expectedMessage;
    private String actualMessage;

    public KernelExceptionUserMessageMatcher( TokenNameLookup tokenNameLookup, String expectedMessage )
    {
        this.tokenNameLookup = tokenNameLookup;
        this.expectedMessage = expectedMessage;
    }

    @Override
    public boolean matches( Object item )
    {
        if ( item instanceof SchemaKernelException )
        {
            actualMessage = ((SchemaKernelException) item).getUserMessage( tokenNameLookup );
            return expectedMessage.equals( actualMessage );
        }
        return false;
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( " expected message: '" ).appendText( expectedMessage )
                .appendText( "', but was: '" ).appendText( actualMessage ).appendText( "'" );
    }
}
