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
package org.neo4j.shell.test;

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.shell.CypherShell;
import org.neo4j.shell.Main;
import org.neo4j.shell.ShellRunner;
import org.neo4j.shell.cli.CliArgs;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.log.AnsiLogger;

import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.shell.Main.EXIT_FAILURE;
import static org.neo4j.shell.Main.EXIT_SUCCESS;
import static org.neo4j.shell.cli.CliArgHelper.parseAndThrow;
import static org.neo4j.shell.terminal.CypherShellTerminalBuilder.terminalBuilder;

public class AssertableMain
{
    private final int exitCode;
    private final ByteArrayOutputStream out;
    private final ByteArrayOutputStream err;
    private final CypherShell shell;

    public AssertableMain( int exitCode, ByteArrayOutputStream out, ByteArrayOutputStream err, CypherShell shell )
    {
        this.exitCode = exitCode;
        this.out = out;
        this.err = err;
        this.shell = shell;
    }

    private Supplier<String> failureSupplier( String description )
    {
        return () -> description + "\nError output:\n" + this.err.toString() + "\n" + "Output:\n" + this.out.toString() + "\n";
    }

    public AssertableMain assertOutputLines( String... expected )
    {
        return assertThatOutput( Matchers.equalTo( stream( expected ).map( l -> l + "\n" ).collect( joining() ) ) );
    }

    @SafeVarargs
    public final AssertableMain assertThatOutput( Matcher<String>... matchers )
    {
        var output = out.toString( UTF_8 ).replace( "\r\n", "\n" );
        stream( matchers ).forEach( matcher -> assertThat( output, matcher ) );
        return this;
    }

    public AssertableMain assertSuccess( boolean isErrorOutputEmpty )
    {
        assertEquals( EXIT_SUCCESS, exitCode, failureSupplier( "Unexpected exit code" ) );
        if ( isErrorOutputEmpty )
        {
            assertEquals( "", err.toString( UTF_8 ), "Error output expected to be empty" );
        }
        return this;
    }

    public AssertableMain assertSuccessAndConnected( boolean isErrorOutputEmpty )
    {
        assertTrue( shell.isConnected(), "Shell is not connected" );
        return assertSuccess( isErrorOutputEmpty );
    }

    public AssertableMain assertSuccessAndConnected()
    {
        return assertSuccessAndConnected( true );
    }

    public AssertableMain assertSuccess()
    {
        return assertSuccess( true );
    }

    @SafeVarargs
    public final AssertableMain assertThatErrorOutput( Matcher<String>... matchers )
    {
        var errorOutput = err.toString( UTF_8 );
        stream( matchers ).forEach( matcher -> assertThat( errorOutput, matcher ) );
        return this;
    }

    public AssertableMain assertFailure( String... expectedErrorOutput )
    {
        assertEquals( EXIT_FAILURE, exitCode, failureSupplier( "Unexpected exit code" ) );
        if ( expectedErrorOutput.length > 0 )
        {
            assertEquals( stream( expectedErrorOutput ).map( l -> l + lineSeparator() ).collect( joining() ), err.toString( UTF_8 ), "Unexpected error ouput" );
        }
        return this;
    }

    public ByteArrayOutputStream getOutput()
    {
        return out;
    }

    public static class AssertableMainBuilder
    {
        public ByteArrayInputStream in = new ByteArrayInputStream( new byte[0] );
        public List<String> args = new ArrayList<>();
        public Boolean isOutputInteractive;
        public CypherShell shell;
        public ShellRunner.Factory runnerFactory;
        public final ByteArrayOutputStream out = new ByteArrayOutputStream();
        public final ByteArrayOutputStream err = new ByteArrayOutputStream();
        public File historyFile;

        public AssertableMainBuilder shell( CypherShell shell )
        {
            this.shell = shell;
            return this;
        }

        public AssertableMainBuilder runnerFactory( ShellRunner.Factory factory )
        {
            this.runnerFactory = factory;
            return this;
        }

        public AssertableMainBuilder args( String whiteSpaceSeparatedArgs )
        {
            this.args = stream( whiteSpaceSeparatedArgs.split( "\\s+" ) ).collect( Collectors.toList() );
            return this;
        }

        public AssertableMainBuilder addArgs( String... args )
        {
            this.args.addAll( asList( args ) );
            return this;
        }

        public AssertableMainBuilder outputInteractive( boolean isOutputInteractive )
        {
            this.isOutputInteractive = isOutputInteractive;
            return this;
        }

        public AssertableMainBuilder userInputLines( String... input )
        {
            this.in = new ByteArrayInputStream( ( stream( input ).map( l -> l + "\n" ).collect( joining() ) ).getBytes() );
            return this;
        }

        public AssertableMainBuilder userInput( String input )
        {
            this.in = new ByteArrayInputStream( input.getBytes() );
            return this;
        }

        public AssertableMainBuilder historyFile( File file )
        {
            this.historyFile = file;
            return this;
        }

        public AssertableMain run() throws ArgumentParserException, IOException
        {
            var outPrintStream = new PrintStream( out );
            var errPrintStream = new PrintStream( err );
            var args = parseArgs();
            var logger = new AnsiLogger( false, Format.VERBOSE, outPrintStream, errPrintStream );
            var terminal = terminalBuilder().dumb().streams( in, outPrintStream ).interactive( !args.getNonInteractive() ).logger( logger ).build();
            var main = new Main( args, logger, shell, isOutputInteractive, runnerFactory, terminal );
            var exitCode = main.startShell();
            return new AssertableMain( exitCode, out, err, shell );
        }

        protected CliArgs parseArgs() throws ArgumentParserException, IOException
        {
            var parsedArgs = parseAndThrow( args.toArray( String[]::new ) );
            var history = historyFile != null ? historyFile : Files.createTempFile( "temp-history", null ).toFile();
            parsedArgs.setHistoryFile( history );
            return parsedArgs;
        }
    }
}
