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
package org.neo4j.index.impl.lucene;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class IsEmpty extends TypeSafeMatcher<Iterable<?>>
{
    private Iterable<?> iterable;

    @Override
    public boolean matchesSafely( Iterable<?> iterable )
    {
        this.iterable = iterable;
        return !iterable.iterator().hasNext();
    }

    public void describeTo( Description description )
    {
        description.appendValueList("[", ",", "]", iterable);
    }

    public static Matcher<Iterable<?>> isEmpty() {
         return new IsEmpty();
     }
}
