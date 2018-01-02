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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.regex.Pattern;

public class RegexMatcher extends TypeSafeMatcher<String>
{
    private final Pattern pattern;

    public RegexMatcher( Pattern pattern )
    {
        this.pattern = pattern;
    }

    public static Matcher<String> pattern( String regex )
    {
        return new RegexMatcher( Pattern.compile( regex ) );
    }

    @Override
    protected boolean matchesSafely( String item )
    {
        return pattern.matcher( item ).matches();
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( "a string matching /" );
        description.appendText( pattern.toString() );
        description.appendText( "/" );
    }
}
