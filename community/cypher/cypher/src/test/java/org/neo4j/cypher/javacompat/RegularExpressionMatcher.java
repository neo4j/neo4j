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
package org.neo4j.cypher.javacompat;

import java.util.regex.Pattern;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class RegularExpressionMatcher extends TypeSafeMatcher<String>
{
    private final Pattern pattern;

    public RegularExpressionMatcher( String pattern )
    {
        this( Pattern.compile( pattern ) );
    }

    public RegularExpressionMatcher( Pattern pattern )
    {
        this.pattern = pattern;
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( "matches regular expression " ).appendValue( pattern );
    }

    @Override
    public boolean matchesSafely( String item )
    {
        return pattern.matcher( item ).find();
    }

    @Factory
    public static Matcher matchesPattern( Pattern pattern )
    {
        return new RegularExpressionMatcher( pattern );
    }

    @Factory
    public static Matcher matchesPattern( String pattern )
    {
        return new RegularExpressionMatcher( pattern );
    }
}