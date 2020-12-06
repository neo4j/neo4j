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
package org.neo4j.shell.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/**
 * A cypher aware parser which can detect shell commands (:prefixed) or cypher.
 */
public class ShellStatementParser implements StatementParser
{

    private static final Pattern SHELL_CMD_PATTERN = Pattern.compile( "^\\s*:.+\\s*$" );
    private static final char SEMICOLON = ';';
    private static final char BACKSLASH = '\\';
    private static final String LINE_COMMENT_START = "//";
    private static final String LINE_COMMENT_END = "\n";
    private static final String BLOCK_COMMENT_START = "/*";
    private static final String BLOCK_COMMENT_END = "*/";
    private static final char BACKTICK = '`';
    private static final char DOUBLE_QUOTE = '"';
    private static final char SINGLE_QUOTE = '\'';
    private static final int NO_COMMENT = -1;
    private Optional<String> awaitedRightDelimiter;
    private StringBuilder statement;
    private ArrayList<String> parsedStatements;
    private int commentStart = NO_COMMENT;

    public ShellStatementParser()
    {
        parsedStatements = new ArrayList<>();
        statement = new StringBuilder();
        awaitedRightDelimiter = Optional.empty();
    }

    /**
     * Parses text and adds to the list of parsed statements if a statement is found to be completed. Note that it is expected that lines include newlines.
     *
     * @param line to parse (including ending newline)
     */
    @Override
    public void parseMoreText( @Nonnull String line )
    {
        // See if it could possibly be a shell command, only valid if not in a current statement
        if ( statementNotStarted() && SHELL_CMD_PATTERN.matcher( line ).find() )
        {
            parsedStatements.add( line );
            return;
        }

        // We will guess it is cypher then
        boolean skipNext = false;
        char prev, current = (char) 0;
        for ( char c : line.toCharArray() )
        {
            // append current
            statement.append( c );
            // last char shuffling
            prev = current;
            current = c;

            if ( skipNext )
            {
                // This char is escaped so gets no special treatment
                skipNext = false;
                continue;
            }

            if ( handleComments( prev, current ) )
            {
                continue;
            }

            if ( current == BACKSLASH )
            {
                // backslash can escape stuff outside of comments (but inside quotes too!)
                skipNext = true;
                continue;
            }

            if ( handleQuotes( prev, current ) )
            {
                continue;
            }

            // Not escaped, not in a quote, not in a comment
            if ( handleSemicolon( current ) )
            {
                continue;
            }

            // If it's the start of a quote or comment
            awaitedRightDelimiter = getRightDelimiter( prev, current );
        }
    }

    /**
     * @param current character
     * @return true if parsing should go immediately to the next character, false otherwise
     */
    private boolean handleSemicolon( char current )
    {
        if ( current == SEMICOLON )
        {
            // end current statement
            parsedStatements.add( statement.toString() );
            // start a new statement
            statement = new StringBuilder();
            return true;
        }
        return false;
    }

    /**
     * @param prev    character
     * @param current character
     * @return true if parsing should go immediately to the next character, false otherwise
     */
    private boolean handleQuotes( char prev, char current )
    {
        if ( inQuote() )
        {
            if ( isRightDelimiter( prev, current ) )
            {
                // Then end it
                awaitedRightDelimiter = Optional.empty();
                return true;
            }
            // Didn't end the quote, continue
            return true;
        }
        return false;
    }

    /**
     * @param prev    character
     * @param current character
     * @return true if parsing should go immediately to the next character, false otherwise
     */
    private boolean handleComments( char prev, char current )
    {
        if ( inComment() )
        {
            if ( commentStart == NO_COMMENT )
            {
                //find the position of //.. or /*...
                //i.e. currentPos - 1 - 2
                commentStart = statement.length() - 3;
            }
            if ( isRightDelimiter( prev, current ) )
            {
                // Then end it
                awaitedRightDelimiter = Optional.empty();
                statement.delete( commentStart, statement.length() );
                commentStart = NO_COMMENT;
                return true;
            }
            // Didn't end the comment, continue
            return true;
        }
        return false;
    }

    /**
     * @return true if inside a quote, false otherwise
     */
    private boolean inQuote()
    {
        return awaitedRightDelimiter.isPresent() && !inComment();
    }

    /**
     * @param first character
     * @param last  character
     * @return true if the last two chars ends the current comment, false otherwise
     */
    private boolean isRightDelimiter( char first, char last )
    {
        if ( !awaitedRightDelimiter.isPresent() )
        {
            return false;
        }
        final String expectedEnd = awaitedRightDelimiter.get();

        if ( expectedEnd.length() == 1 )
        {
            return expectedEnd.equals( String.valueOf( last ) );
        }
        else
        {
            return expectedEnd.equals( String.valueOf( first ) + last );
        }
    }

    /**
     * @return true if we are currently inside a comment, false otherwise
     */
    private boolean inComment()
    {
        return awaitedRightDelimiter.isPresent() &&
               (awaitedRightDelimiter.get().equals( LINE_COMMENT_END ) ||
                awaitedRightDelimiter.get().equals( BLOCK_COMMENT_END ));
    }

    /**
     * If the last characters start a quote or a comment, this returns the piece of text which will end said quote or comment.
     *
     * @param first character
     * @param last  character
     * @return the matching right delimiter or something empty if not the start of a quote/comment
     */
    @Nonnull
    private Optional<String> getRightDelimiter( char first, char last )
    {
        // double characters
        final String lastTwoChars = String.valueOf( first ) + last;
        switch ( lastTwoChars )
        {
        case LINE_COMMENT_START:
            return Optional.of( LINE_COMMENT_END );
        case BLOCK_COMMENT_START:
            return Optional.of( BLOCK_COMMENT_END );
        default:
            // Do nothing
        }
        // single characters
        switch ( last )
        {
        case BACKTICK:
        case DOUBLE_QUOTE:
        case SINGLE_QUOTE:
            return Optional.of( String.valueOf( last ) );
        default:
            return Optional.empty();
        }
    }

    /**
     * @return false if a statement has not begun (non whitespace has been seen) else true
     */
    private boolean statementNotStarted()
    {
        return statement.toString().trim().isEmpty();
    }

    @Override
    public boolean hasStatements()
    {
        return !parsedStatements.isEmpty();
    }

    @Nonnull
    @Override
    public List<String> consumeStatements()
    {
        ArrayList<String> result = parsedStatements;
        parsedStatements = new ArrayList<>();
        return result;
    }

    @Override
    public boolean containsText()
    {
        return !statement.toString().trim().isEmpty();
    }

    @Override
    public void reset()
    {
        statement = new StringBuilder();
        parsedStatements.clear();
        awaitedRightDelimiter = Optional.empty();
    }
}
