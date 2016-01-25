/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.proc;

import org.junit.Test;

import java.util.stream.Stream;

import org.neo4j.graphdb.factory.Description;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class ProcedureAsciiDocGeneratorTest
{
    @Test
    public void generatesAsciiTable() throws Throwable
    {
        // When
        String docs = new ProcedureAsciiDocGenerator().generateDocsFor( "myProcedures", "My Procedures", MyDocumentedProcedures.class );

        // Then
        assertThat( docs, containsString(
              "[[myProcedures]]\n" +
              ".My Procedures\n" +
              "ifndef::nonhtmloutput[]\n" +
              "\n" +
              "[options=\"header\"]\n" +
              "|===\n" +
              "|Name|Description\n" +
              "|<<builtinproc_sys_coolstuff_theProcedure,sys.coolstuff.theProcedure(someInput :: STRING?, moreInput :: INTEGER?) :: (someOutput :: " +
              "STRING?, moreOutput :: FLOAT?)>>|This procedure exists solely for the purposes of this test.\n" +
              "|===\n" +
              "endif::nonhtmloutput[]\n" +
              "\n" +
              "ifdef::nonhtmloutput[]\n" +
              "\n" +
              "* <<builtinproc_sys_coolstuff_theProcedure,sys.coolstuff.theProcedure(someInput :: STRING?, moreInput :: INTEGER?) :: (someOutput :: " +
              "STRING?, moreOutput :: FLOAT?)>>: This procedure exists solely for the purposes of this test.\n" +
              "endif::nonhtmloutput[]\n" +
              "\n") );
    }

    @Namespace( {"sys", "coolstuff"} )
    public static class MyDocumentedProcedures
    {
        @ReadOnlyProcedure
        @Description( "This procedure exists solely for the purposes of this test." )
        public Stream<MyOutput> theProcedure( @Name( "someInput" ) String someInput, @Name( "moreInput" ) long moreInput )
        {
            return null;
        }

        public static class MyOutput
        {
            public String someOutput;
            public double moreOutput;
        }
    }
}