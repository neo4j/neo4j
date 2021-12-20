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

import org.fusesource.jansi.Ansi;
import org.jline.reader.Expander;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.log.AnsiFormattedText;
import org.neo4j.shell.log.Logger;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

/**
 * CypherShellTerminal backed by jline.
 */
public class JlineTerminal implements CypherShellTerminal
{
    static final String NO_CONTINUATION_PROMPT_PATTERN = "  ";

    private final LineReader jLineReader;
    private final Logger logger;
    private final Reader reader;
    private final Writer writer;
    private final boolean isInteractive;

    public JlineTerminal( LineReader jLineReader, boolean isInteractive, Logger logger )
    {
        assert jLineReader.getParser() instanceof CypherJlineParser;
        this.jLineReader = jLineReader;
        this.logger = logger;
        this.isInteractive = isInteractive;
        this.reader = new JLineReader();
        this.writer = new JLineWriter();
    }

    private CypherJlineParser getParser()
    {
        return (CypherJlineParser) jLineReader.getParser();
    }

    @Override
    public Reader read()
    {
        return reader;
    }

    @Override
    public Writer write()
    {
        return writer;
    }

    @Override
    public boolean isInteractive()
    {
        return isInteractive;
    }

    @Override
    public Historian getHistory()
    {
        return new JlineHistorian();
    }

    @Override
    public void setHistoryFile( File file )
    {
        if ( !file.equals( jLineReader.getVariable( LineReader.HISTORY_FILE ) ) )
        {
            jLineReader.setVariable( LineReader.HISTORY_FILE, file );
            //the load here makes sure that history will work right from the start
            loadHistory();
            Runtime.getRuntime().addShutdownHook( new Thread( this::flushHistory ) );
        }
    }

    @Override
    public void bindUserInterruptHandler( UserInterruptHandler handler )
    {
        jLineReader.getTerminal().handle( Terminal.Signal.INT, signal -> handler.handleUserInterrupt() );
    }

    private void flushHistory()
    {
        try
        {
            getHistory().flushHistory();
        }
        catch ( IOException e )
        {
            logger.printError( "Failed to save history: " + e.getMessage() );
        }
    }

    private void loadHistory()
    {
        try
        {
            jLineReader.getHistory().load();
        }
        catch ( IOException e )
        {
            logger.printError( "Failed to load history: " + e.getMessage() );
        }
    }

    private class JlineHistorian implements Historian
    {
        @Override
        public List<String> getHistory()
        {
            loadHistory();
            return stream( jLineReader.getHistory().spliterator(), false ).map( History.Entry::line ).collect( toList() );
        }

        @Override
        public void flushHistory() throws IOException
        {
            jLineReader.getHistory().save();
        }

        @Override
        public void clear() throws IOException
        {
            jLineReader.getHistory().purge();
        }
    }

    private class JLineReader implements Reader
    {
        private String readLine( String prompt, Character mask ) throws NoMoreInputException, UserInterruptException
        {
            try
            {
                return jLineReader.readLine( prompt, mask );
            }
            catch ( org.jline.reader.EndOfFileException e )
            {
                throw new NoMoreInputException();
            }
            catch ( org.jline.reader.UserInterruptException e )
            {
                throw new UserInterruptException( e.getPartialLine() );
            }
        }

        @Override
        public ParsedStatement readStatement( AnsiFormattedText prompt ) throws NoMoreInputException, UserInterruptException
        {
            getParser().setEnableStatementParsing( true );
            jLineReader.setVariable( LineReader.SECONDARY_PROMPT_PATTERN, continuationPromptPattern( prompt ) );

            var line = readLine( prompt.renderedString(), null );
            var parsed = jLineReader.getParsedLine();

            if ( !( parsed instanceof ParsedStatement ) )
            {
                throw new IllegalStateException( "Unexpected type of parsed line " + parsed.getClass().getSimpleName() );
            }

            var parsedStatement = (ParsedStatement) parsed;
            if ( !parsedStatement.unparsed().equals( line ) )
            {
                throw new IllegalStateException( "Parsed and unparsed lines do not match: " + line + " does not equal " + parsedStatement.unparsed() );
            }

            return parsedStatement;
        }

        private String continuationPromptPattern( AnsiFormattedText prompt )
        {
            if ( prompt.textLength() > PROMPT_MAX_LENGTH )
            {
                return NO_CONTINUATION_PROMPT_PATTERN;
            }
            else
            {
                // Note, jline has built in support for this using '%P', but that causes a bug in certain environments
                // https://github.com/jline/jline3/issues/751
                return " ".repeat( prompt.textLength() );
            }
        }

        @Override
        public String simplePrompt( String prompt, Character mask ) throws NoMoreInputException, UserInterruptException
        {
            try
            {
                // Temporarily disable history and cypher parsing for simple prompts
                jLineReader.getVariables().put( LineReader.DISABLE_HISTORY, Boolean.TRUE );
                getParser().setEnableStatementParsing( false );

                return readLine( prompt, mask );
            }
            finally
            {
                jLineReader.getVariables().remove( LineReader.DISABLE_HISTORY );
                getParser().setEnableStatementParsing( true );
            }
        }
    }

    private class JLineWriter implements Writer
    {
        @Override
        public void println( String line )
        {
            jLineReader.printAbove( line + System.lineSeparator() );
        }
    }

    public static class EmptyExpander implements Expander
    {
        @Override
        public String expandHistory( History history, String line )
        {
            return line;
        }

        @Override
        public String expandVar( String word )
        {
            return word;
        }
    }
}
