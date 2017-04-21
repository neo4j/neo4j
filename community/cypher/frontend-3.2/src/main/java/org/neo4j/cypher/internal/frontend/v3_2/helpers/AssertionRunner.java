/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2.helpers;

/*
Why is this here!?

assert in Scala does something different to assert in Java. In Scala, it's controlled through a compiler setting,
which means you can't use the same binaries and enable/disable assertions through a JVM configuration.

We want the Java behaviour in Scala, and this is how we achieve that.
 */
public class AssertionRunner
{
    private AssertionRunner()
    {
    }

    public static void runUnderAssertion( Thunk thunk )
    {
        assert runIt(thunk);
    }

    private static boolean runIt( Thunk thunk )
    {
        thunk.apply();
        return true;
    }

    public interface Thunk
    {
        void apply();
    }
}
