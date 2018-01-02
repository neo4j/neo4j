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
package org.neo4j.test.randomized;

public interface Action<T,F>
{
    /**
     * @return {@code true} if the value was applied properly and all checks verifies,
     * otherwise {@code false}.
     */
    F apply( T target );

    /**
     * For outputting a test case, this is the code that represents this action.
     */
    void printAsCode( T source, LinePrinter out, boolean includeChecks );
    
    public abstract static class Adapter<T,F> implements Action<T,F>
    {
        @Override
        public void printAsCode( T source, LinePrinter out, boolean includeChecks )
        {
            out.println( getClass().getSimpleName() + "#printAsCode not implemented" );
        }
    }
}
