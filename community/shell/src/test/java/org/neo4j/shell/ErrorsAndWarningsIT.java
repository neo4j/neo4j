/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.shell;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.neo4j.test.rule.SuppressOutput;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

public class ErrorsAndWarningsIT extends AbstractShellIT
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Before
    public void setup()
    {
        makeServerRemotelyAvailable();
    }

    @Test
    public void unsupportedQueryShouldBeSilent()
    {
        // When
        InputStream realStdin = System.in;
        try
        {
            System.setIn( new ByteArrayInputStream( "CYPHER planner=cost CREATE ();".getBytes() ) );
            StartClient.main( new String[]{"-file", "-"} );

            // Then we should not get a warning
            String output = suppressOutput.getOutputVoice().toString();
            assertThat( output, not( isEmptyString() ) );
            assertThat( output, not( containsString( "Using COST planner is unsupported for this query, please use RULE planner instead" ) ) );
        }
        finally
        {
            System.setIn( realStdin );
        }
    }
}
