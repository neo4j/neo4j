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
package org.neo4j.server.rest.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class UriPathWildcardMatcherTest
{
    @Test
    public void shouldFailWithoutAsteriskAtStart()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher("/some/uri/path");

        assertFalse(matcher.matches("preamble/some/uri/path"));
    }

    @Test
    public void shouldFailWithoutAsteriskAtEnd()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher("/some/uri/path/and/some/more");

        assertFalse(matcher.matches("/some/uri/path/with/middle/bit/and/some/more"));
    }

    @Test
    public void shouldMatchAsteriskAtStart()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher("*/some/uri/path");

        assertTrue(matcher.matches("anything/i/like/followed/by/some/uri/path"));
        assertFalse(matcher.matches("anything/i/like/followed/by/some/deliberately/changed/to/fail/uri/path"));
    }

    @Test
    public void shouldMatchAsteriskAtEnd()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher("/some/uri/path/*");

        assertTrue(matcher.matches("/some/uri/path/followed/by/anything/i/like"));
    }

    @Test
    public void shouldMatchAsteriskInMiddle()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher("/some/uri/path/*/and/some/more");

        assertTrue(matcher.matches("/some/uri/path/with/middle/bit/and/some/more"));
    }

    @Test
    public void shouldMatchMultipleAsterisksInMiddle()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher("/some/uri/path/*/and/some/more/*/and/a/final/bit");

        assertTrue(matcher.matches(
                "/some/uri/path/with/middle/bit/and/some/more/with/additional/asterisk/part/and/a/final/bit"));
    }

    @Test
    public void shouldMatchMultipleAsterisksAtStartAndInMiddle()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher(
                "*/some/uri/path/*/and/some/more/*/and/a/final/bit");

        assertTrue(matcher.matches(
                "a/bit/of/preamble/and/then/some/uri/path/with/middle/bit/and/some/more/with/additional/asterisk/part/and/a/final/bit"));

    }

    @Test
    public void shouldMatchMultipleAsterisksAtEndAndInMiddle()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher(
                "/some/uri/path/*/and/some/more/*/and/a/final/bit/*");

        assertTrue(matcher.matches(
                "/some/uri/path/with/middle/bit/and/some/more/with/additional/asterisk/part/and/a/final/bit/and/now/some/post/amble"));

    }

    @Test
    public void shouldMatchMultipleAsterisksAtStartAndEndAndInMiddle()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher(
                "*/some/uri/path/*/and/some/more/*/and/a/final/bit/*");

        assertTrue(matcher.matches(
                "a/bit/of/preamble/and/then//some/uri/path/with/middle/bit/and/some/more/with/additional/asterisk/part/and/a/final/bit/and/now/some/post/amble"));
    }

    @Test
    public void shouldMatchMultipleSimpleString()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher("str");

        assertTrue(matcher.matches("str"));
    }

    @Test
    public void shouldMatchMultipleSimpleStringWithALeadingWildcard()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher("*str");

        assertTrue(matcher.matches("my_str"));
    }

    @Test
    public void shouldFailToMatchMultipleSimpleStringWithATrailingWildcard()
    {
        UriPathWildcardMatcher matcher = new UriPathWildcardMatcher("str*");

        assertFalse(matcher.matches("my_str"));
    }
}
