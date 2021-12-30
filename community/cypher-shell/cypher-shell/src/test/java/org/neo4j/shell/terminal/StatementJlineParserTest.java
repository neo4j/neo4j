/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.shell.terminal;

import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser.ParseContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.shell.parser.JavaCcCypherLanguageService;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatements;
import org.neo4j.shell.terminal.StatementJlineParser.BlankCompletion;
import org.neo4j.shell.terminal.StatementJlineParser.CommandCompletion;
import org.neo4j.shell.terminal.StatementJlineParser.CypherCompletion;
import org.neo4j.shell.terminal.StatementJlineParser.SimpleParsedStatements;
import org.neo4j.shell.terminal.StatementJlineParser.UnparsedLine;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.jline.reader.Parser.ParseContext.COMPLETE;
import static org.junit.jupiter.api.Assertions.assertThrows;

// More test coverage of StatementJlineParser in JlineCompleterTest
class StatementJlineParserTest
{
    private final StatementJlineParser parser = new StatementJlineParser( new ShellStatementParser(), new JavaCcCypherLanguageService() );

    @BeforeEach
    void setup()
    {
        parser.setEnableStatementParsing( true );
    }

    @Test
    void disableParsing()
    {
        parser.setEnableStatementParsing( false );
        assertThat( parse( "bla; bla; bla;" ), is( new UnparsedLine( "bla; bla; bla;", 14 ) ) );
        assertThat( parse( ":bla" ), is( new UnparsedLine( ":bla", 4 ) ) );
        assertThat( parse( ":bla", COMPLETE ), is( new UnparsedLine( ":bla", 4 ) ) );
        assertThat( parse( "return 1; return 2" ), is( new UnparsedLine( "return 1; return 2", 18 )) );
    }

    @Test
    void parseCommandsForExecution()
    {
        assertSimpleParse( ":bla", command( ":bla", List.of(), false, 0, 3 ) );
        assertSimpleParse( ":param key => 'value' ", command( ":param", List.of( "key => 'value'" ), false, 0, 21 ) );
    }

    @Test
    void parseCypherForExecution()
    {
        assertSimpleParse( "return 1;", cypher( "return 1;" ) );
        assertSimpleParse( "/* comment; */ match (a) return a; // Comment;", new CypherStatement( "match (a) return a;", true, 15, 33 ) );
        assertSimpleParse( "match (a) /* hi */ return a;", cypher( "match (a) /* hi */ return a;" ) );
        assertSimpleParse(
                "return 1; return 2;",
                new CypherStatement( "return 1;", true, 0, 8 ),
                new CypherStatement( "return 2;", true, 10, 18 )
        );
    }

    @Test
    void parseWithLineContinuation()
    {
        assertThrows( EOFError.class, () -> parse( "return 1; return 2" ) );
    }

    @Test
    void parseCommandForCompletion()
    {
        assertThat( parse( ":", COMPLETE ), is( new CommandCompletion( command( ":", List.of(), false, 0, 0 ), ":", 1 ) ) );
        assertThat( parse( ":hel", COMPLETE ), is( new CommandCompletion( command( ":hel", List.of(), false, 0, 3 ), ":hel", 4 ) ) );
    }

    @Test
    void dontCompleteCommandWithParam()
    {
        assertThat( parse( ":param myParam", COMPLETE), is( new BlankCompletion( ":param myParam", 14 ) ) );
        assertThat( parse( ":help :pa", COMPLETE), is( new BlankCompletion( ":help :pa", 9 ) ) );
        assertThat( parse( ":help :param\n", COMPLETE), is( new BlankCompletion( ":help :param\n", 13 ) ) );
    }

    @Test
    void parseCypherForCompletion()
    {
        var query = "match (a)-[r]->(b) ";
        var parsed = (CypherCompletion) parse( query, COMPLETE );

        var expectedStatement = new CypherStatement( query, false, 0, 18 );
        assertThat( parsed.statement(), is( expectedStatement ) );
        assertThat( parsed.word(), is( "" ) );
        assertThat( parsed.line(), is( query ) );
        assertThat( parsed.cursor(), is( query.length() ) );
    }

    @Test
    void completeEmptyStatement()
    {
        assertThat( parse( "", COMPLETE ), is( new BlankCompletion( "", 0 ) ) );
        assertThat( parse( "return 1;", COMPLETE ), is( new BlankCompletion( "return 1;", 9 ) ) );
    }

    private CypherStatement cypher( String cypher )
    {
        return new CypherStatement( cypher, true, 0, cypher.length() - 1 );
    }

    private CommandStatement command( String command, List<String> args, int begin, int end )
    {
        return new CommandStatement( command, args, true, begin, end );
    }

    private CommandStatement command( String command, List<String> args, boolean complete, int begin, int end )
    {
        return new CommandStatement( command, args, complete, begin, end );
    }

    private ParsedLine parse( String line )
    {
        return parser.parse( line, line.length() );
    }

    private ParsedLine parse( String line, ParseContext context )
    {
        return parser.parse( line, line.length(), context );
    }

    private void assertSimpleParse( String line, StatementParser.ParsedStatement... statements )
    {
        assertThat( parse( line ), is( new SimpleParsedStatements( new ParsedStatements( List.of( statements ) ), line, line.length() ) ) );
    }

    private void assertSimpleParse( String line, ParseContext context, StatementParser.ParsedStatement... statements )
    {
        assertThat( parse( line, context ), is( new SimpleParsedStatements( new ParsedStatements( List.of( statements ) ), line, line.length() ) ) );
    }
}
