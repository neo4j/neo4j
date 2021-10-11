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

import java.util.Collections;
import java.util.List;

import org.neo4j.shell.parser.StatementParser;

/**
 * Jline Parser that parse cypher shell statements (cypher queries/cypher shell commands).
 */
public class CypherJlineParser implements Parser
{
    private final StatementParser parser;
    private boolean enableStatementParsing;

    public CypherJlineParser( StatementParser parser )
    {
        this.parser = parser;
    }

    @Override
    public ParsedLine parse( String line, int cursor, ParseContext context ) throws SyntaxError
    {
        return enableStatementParsing ? doParse( line, cursor ) : new ParsedLineImpl( line, cursor );
    }

    private ParsedCypher doParse( String line, int cursor )
    {
        parser.reset();
        parser.parseMoreText( line );

        if ( !parser.hasStatements() || parser.incompleteStatement().isPresent() )
        {
            throw new EOFError( -1, cursor, "Incomplete statement" );
        }

        return new ParsedCypher( parser.consumeStatements(), line, cursor );
    }

    /** If enable is false this parser will be disabled and pass through all lines */
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

    public static class ParsedCypher extends ParsedLineImpl implements CypherShellTerminal.ParsedStatement
    {
        private final List<String> statements;

        public ParsedCypher( List<String> statements, String line, int cursor )
        {
            super( line, cursor );
            this.statements = statements;
        }

        @Override
        public String unparsed()
        {
            return line();
        }

        @Override
        public List<String> parsed()
        {
            return statements;
        }
    }

    static class ParsedLineImpl implements CompletingParsedLine
    {
        private final String line;
        private final int cursor;

        ParsedLineImpl( String line, int cursor )
        {
            this.line = line;
            this.cursor = cursor;
        }

        @Override
        public String word()
        {
            return "";
        }

        @Override
        public int wordCursor()
        {
            return 0;
        }

        @Override
        public int wordIndex()
        {
            return 0;
        }

        @Override
        public List<String> words()
        {
            return Collections.emptyList();
        }

        @Override
        public String line()
        {
            return line;
        }

        @Override
        public int cursor()
        {
            return cursor;
        }

        @Override
        public CharSequence escape( CharSequence candidate, boolean complete )
        {
            return candidate;
        }

        @Override
        public int rawWordCursor()
        {
            return 0;
        }

        @Override
        public int rawWordLength()
        {
            return 0;
        }
    }
}
