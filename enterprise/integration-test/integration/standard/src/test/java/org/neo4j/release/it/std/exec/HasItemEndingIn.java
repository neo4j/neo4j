/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.release.it.std.exec;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.junit.internal.matchers.TypeSafeMatcher;

public class HasItemEndingIn extends TypeSafeMatcher<Iterable<String>>{
    private String endingIn;

    public HasItemEndingIn(String endingIn){

        this.endingIn = endingIn;
    }

    @Override
    public boolean matchesSafely(Iterable<String> iterable) {
        for(String current : iterable){
            if(current.endsWith(endingIn))
                return true;
        }

        return false;
    }

    public void describeTo(Description description) {
        description.appendText("Expected at least one item to end in " + endingIn);
    }

    @Factory
    public static HasItemEndingIn hasItemEndingIn(String endingIn) {
            return new HasItemEndingIn(endingIn);
    }
}
