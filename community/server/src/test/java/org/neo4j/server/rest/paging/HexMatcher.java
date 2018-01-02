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
package org.neo4j.server.rest.paging;

import java.util.regex.Pattern;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class HexMatcher extends TypeSafeMatcher<String>
{
    private static final Pattern pattern = Pattern.compile( "[a-fA-F0-9]*" );
    private String candidate;

    private HexMatcher()
    {
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( String.format( "[%s] is not a pure hexadecimal string", candidate ) );
    }

    @Override
    public boolean matchesSafely( String candidate )
    {
        this.candidate = candidate;
        return pattern.matcher( candidate )
                .matches();
    }

    @Factory
    public static Matcher<String> containsOnlyHex()
    {
        return new HexMatcher();
    }
}
