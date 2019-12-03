/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.util;

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
        throw new AssertionError( "No instances" );
    }

    /**
     *  <code>true</code> if running with -ea otherwise <code>false</code>.
     *
     * Be careful about refactoring because it is accessed in mysterious way perhaps not known statically
     */
    public static final boolean ASSERTIONS_ENABLED = isAssertionsEnabled();

    public static void runUnderAssertion( Thunk thunk )
    {
        assert runIt(thunk);
    }

    @SuppressWarnings( "SameReturnValue" )
    private static boolean runIt( Thunk thunk )
    {
        thunk.apply();
        return true;
    }

    public interface Thunk
    {
        void apply();
    }

    @SuppressWarnings( {"AssertWithSideEffects", "ConstantConditions"} )
    public static boolean isAssertionsEnabled()
    {
        boolean assertionsEnabled = false;
        assert assertionsEnabled = true;
        return assertionsEnabled;
    }
}
