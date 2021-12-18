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

import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatements;
import org.neo4j.shell.terminal.StatementJlineParser.CompletingCommand;
import org.neo4j.shell.terminal.StatementJlineParser.SimpleParsedStatements;
import org.neo4j.shell.terminal.StatementJlineParser.UnparsedLine;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StatementJlineParserTest
{
    private final StatementJlineParser parser = new StatementJlineParser( new ShellStatementParser() );

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
        assertThat( parse( ":bla", ParseContext.COMPLETE ), is( new UnparsedLine( ":bla", 4 ) ) );
        assertThat( parse( "return 1; return 2" ), is( new UnparsedLine( "return 1; return 2", 18 )) );
    }

    @Test
    void parseCommandsForExecution()
    {
        assertSimpleParse( ":bla", new CommandStatement( ":bla", List.of() ) );
        assertSimpleParse( ":param key => 'value' ", new CommandStatement( ":param", List.of( "key => 'value'" ) ) );
    }

    @Test
    void parseCypherForExecution()
    {
        assertSimpleParse( "return 1;", new CypherStatement( "return 1;" ) );
        assertSimpleParse( "/* comment; */ match (a) return a; // Comment;", new CypherStatement( "match (a) return a;" ) );
        assertSimpleParse( "match (a) /* hi */ return a;", new CypherStatement( "match (a) /* hi */ return a;" ) );
        assertSimpleParse( "return 1; return 2;", new CypherStatement( "return 1;" ), new CypherStatement( "return 2;" ) );
    }

    @Test
    void parseWithLineContinuation()
    {
        assertThrows( EOFError.class, () -> parse( "return 1; return 2" ) );
    }

    @Test
    void parseCommandForCompletion()
    {
        assertThat( parse( ":", ParseContext.COMPLETE ), is( new CompletingCommand( new CommandStatement( ":", List.of() ), ":", 1 ) ) );
        assertThat( parse( ":hel", ParseContext.COMPLETE ), is( new CompletingCommand( new CommandStatement( ":hel", List.of() ), ":hel", 4 ) ) );
    }

    @Test
    void dontCompleteCommandWithParam()
    {
        assertSimpleParse( ":help :param", ParseContext.COMPLETE, new CommandStatement( ":help", List.of( ":param" ) ) );
    }

    @Test
    void dontCompleteEmptyStatement()
    {
        assertSimpleParse( "", ParseContext.COMPLETE );
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
