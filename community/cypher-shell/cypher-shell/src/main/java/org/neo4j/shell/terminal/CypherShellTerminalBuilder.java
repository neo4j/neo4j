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

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.completer.NullCompleter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Attributes;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.util.VisibleForTesting;

/**
 * Builder for CypherShellTerminals
 */
public class CypherShellTerminalBuilder
{
    private Logger logger;
    private OutputStream out;
    private InputStream in;
    private boolean isInteractive = true;
    private boolean dumb;

    /** if enabled is true, this is an interactive terminal that supports user input */
    public CypherShellTerminalBuilder interactive( boolean isInteractive )
    {
        this.isInteractive = isInteractive;
        return this;
    }

    public CypherShellTerminalBuilder logger( Logger logger )
    {
        this.logger = logger;
        return this;
    }

    /** Set explicit streams, for testing purposes */
    @VisibleForTesting
    public CypherShellTerminalBuilder streams( InputStream in, OutputStream out )
    {
        this.in = in;
        this.out = out;
        return this;
    }

    /** Create a dumb terminal, for testing purposes */
    @VisibleForTesting
    public CypherShellTerminalBuilder dumb()
    {
        this.dumb = true;
        return this;
    }

    public CypherShellTerminal build()
    {
        assert logger != null;

        try
        {
            return isInteractive ? buildJlineBasedTerminal() : nonInteractiveTerminal();
        }
        catch ( IOException e )
        {
            if ( isInteractive )
            {
                logger.printError( "Failed to create interactive terminal, fallback to non-interactive mode" );
            }
            return nonInteractiveTerminal();
        }
    }

    private CypherShellTerminal nonInteractiveTerminal()
    {
        return new WriteOnlyCypherShellTerminal( out != null ? new PrintStream( out ) : System.out );
    }

    public CypherShellTerminal buildJlineBasedTerminal() throws IOException
    {
        var jLineTerminal = TerminalBuilder.builder();

        jLineTerminal.nativeSignals( true );

        if ( in != null )
        {
            jLineTerminal.streams( in, out );
        }

        if ( dumb )
        {
            var attributes = new Attributes();
            attributes.setLocalFlag( Attributes.LocalFlag.ECHO, false );
            jLineTerminal.jansi( false ).jna( false ).dumb( true ).type( Terminal.TYPE_DUMB ).attributes( attributes );
        }

        var reader = LineReaderBuilder.builder()
            .terminal( jLineTerminal.build() )
            .parser( new CypherJlineParser( new ShellStatementParser() ) )
            .completer( NullCompleter.INSTANCE )
            .history( new DefaultHistory() ) // The default history is in-memory until we set history file variable
            .expander( new JlineTerminal.EmptyExpander() )
            .option( LineReader.Option.DISABLE_EVENT_EXPANSION, true ) // Disable '!' history expansion
            .option( LineReader.Option.DISABLE_HIGHLIGHTER, true )
            .variable( LineReader.DISABLE_COMPLETION, true )
            .variable( LineReader.SECONDARY_PROMPT_PATTERN, JlineTerminal.INDENT_CONTINUATION_PROMPT_PATTERN ) // Pad continuation prompts
            .build();

        return new JlineTerminal( reader, isInteractive, logger );
    }

    public static CypherShellTerminalBuilder terminalBuilder()
    {
        return new CypherShellTerminalBuilder();
    }
}
