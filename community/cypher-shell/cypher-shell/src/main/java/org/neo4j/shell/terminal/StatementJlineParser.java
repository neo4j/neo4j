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

import org.jline.reader.CompletingParsedLine;
import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatements;
import org.neo4j.shell.terminal.JlineTerminal.ParsedLineStatements;

import static org.jline.reader.Parser.ParseContext.COMPLETE;

/**
 * Jline Parser that parse cypher shell statements (cypher queries/cypher shell commands).
 */
public class StatementJlineParser implements Parser
{
    private final StatementParser parser;
    private boolean enableStatementParsing;

    public StatementJlineParser( StatementParser parser )
    {
        this.parser = parser;
    }

    @Override
    public ParsedLine parse( String line, int cursor, ParseContext context ) throws SyntaxError
    {
        return enableStatementParsing ? doParse( line, cursor, context ) : new UnparsedLine( line, cursor );
    }

    private ParsedLine doParse( String line, int cursor, ParseContext context )
    {
        return context == COMPLETE ? parseForCompletion( line, cursor ) : parseForExecution( line, cursor );
    }

    private SimpleParsedStatements parseForExecution( String line, int cursor )
    {
        var parsed = parse( line );

        if ( parsed.hasIncompleteStatement() )
        {
            // This will trigger line continuation in jline
            throw new EOFError( -1, cursor, "Incomplete statement" );
        }

        return new SimpleParsedStatements( parsed, line, cursor );
    }

    private ParsedLine parseForCompletion( String line, int cursor )
    {
        var parsed = parse( line );
        var statements = parsed.statements();

        if ( statements.size() == 1
             && statements.get( 0 ) instanceof CommandStatement command && command.args().isEmpty()
             && cursor == line.length() )
        {
            return new CompletingCommand( command, line, cursor );
        }

        // No completion available
        return new SimpleParsedStatements( parsed, line, cursor );
    }

    private ParsedStatements parse( String line )
    {
        try
        {
            return parser.parse( line );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to parse `" + line + "`: " + e.getMessage(), e );
        }
    }

    /** If enable is false this parser will be disabled and pass through all lines without parsing and completion */
    public void setEnableStatementParsing( boolean enable )
    {
        this.enableStatementParsing = enable;
    }

    @Override
    public boolean isEscapeChar( char ch )
    {
        return false;
    }

    @Override
    public boolean validCommandName( String name )
    {
        return false;
    }

    @Override
    public boolean validVariableName( String name )
    {
        return false;
    }

    @Override
    public String getCommand( String line )
    {
        return "";
    }

    @Override
    public String getVariable( String line )
    {
        return null;
    }

    protected record CompletingCommand( CommandStatement statement, String line, int cursor ) implements FirstWordCompletion
    {
        @Override
        public String word()
        {
            return statement.name();
        }

        @Override
        public int rawWordCursor()
        {
            return wordCursor();
        }

        @Override
        public int rawWordLength()
        {
            return statement.name().length();
        }

        @Override
        public int wordCursor()
        {
            return statement.name().length();
        }
    }

    protected record SimpleParsedStatements( ParsedStatements statements, String line, int cursor ) implements ParsedLineStatements, NoWordsParsedLine
    { }
    protected record UnparsedLine( String line, int cursor ) implements NoWordsParsedLine
    { }

    protected interface FirstWordCompletion extends CompletingParsedLine
    {
        @Override
        default CharSequence escape( CharSequence candidate, boolean complete )
        {
            return candidate;
        }

        @Override
        default int rawWordCursor()
        {
            return cursor();
        }

        @Override
        default int rawWordLength()
        {
            return cursor();
        }

        @Override
        String word();

        @Override
        default int wordCursor()
        {
            return cursor();
        }

        @Override
        default int wordIndex()
        {
            return 0;
        }

        @Override
        default List<String> words()
        {
            return List.of( word() );
        }
    }

    protected interface NoWordsParsedLine extends CompletingParsedLine
    {
        @Override
        default String word()
        {
            return "";
        }

        @Override
        default int wordCursor()
        {
            return 0;
        }

        @Override
        default int wordIndex()
        {
            return 0;
        }

        @Override
        default List<String> words()
        {
            return Collections.emptyList();
        }

        @Override
        default CharSequence escape( CharSequence candidate, boolean complete )
        {
            return candidate;
        }

        @Override
        default int rawWordCursor()
        {
            return 0;
        }

        @Override
        default int rawWordLength()
        {
            return 0;
        }

    }
}
