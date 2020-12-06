/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.shell.commands;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.cypher.internal.evaluator.EvaluationException;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.ShellParameterMap;
import org.neo4j.shell.StringLinePrinter;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.prettyprint.PrettyConfig;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.shell.prettyprint.OutputFormatter.NEWLINE;

public class CypherShellPlainIntegrationTest extends CypherShellIntegrationTest
{
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private StringLinePrinter linePrinter = new StringLinePrinter();

    @Before
    public void setUp() throws Exception
    {
        linePrinter.clear();
        shell = new CypherShell( linePrinter, new PrettyConfig( Format.PLAIN, true, 1000 ), false, new ShellParameterMap() );
        connect( "neo" );
    }

    @After
    public void tearDown() throws Exception
    {
        shell.execute( "MATCH (n) DETACH DELETE (n)" );
    }

    @Test
    public void periodicCommitWorks() throws CommandException
    {
        shell.execute( "USING PERIODIC COMMIT\n" +
                       "LOAD CSV FROM 'https://neo4j.com/docs/cypher-refcard/3.2/csv/artists.csv' AS line\n" +
                       "CREATE (:Artist {name: line[1], year: toInteger(line[2])});" );
        linePrinter.clear();

        shell.execute( "MATCH (a:Artist) WHERE a.name = 'Europe' RETURN a.name" );

        assertThat( linePrinter.output(), containsString( "a.name" + NEWLINE + "\"Europe\"" ) );
    }

    @Test
    public void cypherWithProfileStatements() throws CommandException
    {
        //when
        shell.execute( "CYPHER RUNTIME=INTERPRETED PROFILE RETURN null" );

        //then
        String actual = linePrinter.output();
        //      This assertion checks everything except for time and cypher
        assertThat( actual, containsString( "Plan: \"PROFILE\"" ) );
        assertThat( actual, containsString( "Statement: \"READ_ONLY\"" ) );
        assertThat( actual, containsString( "Planner: \"COST\"" ) );
        assertThat( actual, containsString( "Runtime: \"INTERPRETED\"" ) );
        assertThat( actual, containsString( "DbHits: 0" ) );
        assertThat( actual, containsString( "Rows: 1" ) );
        assertThat( actual, containsString( "null" ) );
        assertThat( actual, containsString( "NULL" ) );
    }

    @Test
    public void cypherWithExplainStatements() throws CommandException
    {
        //when
        shell.execute( "CYPHER RUNTIME=INTERPRETED EXPLAIN RETURN null" );

        //then
        String actual = linePrinter.output();
        //      This assertion checks everything except for time and cypher
        assertThat( actual, containsString( "Plan: \"EXPLAIN\"" ) );
        assertThat( actual, containsString( "Statement: \"READ_ONLY\"" ) );
        assertThat( actual, containsString( "Planner: \"COST\"" ) );
        assertThat( actual, containsString( "Runtime: \"INTERPRETED\"" ) );
    }

    @Test
    public void shouldUseParamFromCLIArgs() throws EvaluationException, CommandException
    {
        // given a CLI arg
        ShellParameterMap parameterMap = new ShellParameterMap();
        parameterMap.setParameter( "foo", "'bar'" );
        shell = new CypherShell( linePrinter, new PrettyConfig( Format.PLAIN, true, 1000 ), false, parameterMap );
        connect( "neo" );

        //when
        shell.execute( "CYPHER RETURN $foo" );

        //then
        String actual = linePrinter.output();
        assertThat( actual, containsString( "$foo" ) );
        assertThat( actual, containsString( "bar" ) );
    }
}
