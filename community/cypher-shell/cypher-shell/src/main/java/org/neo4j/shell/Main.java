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
package org.neo4j.shell;

import java.io.PrintStream;

import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.shell.build.Build;
import org.neo4j.shell.cli.CliArgHelper;
import org.neo4j.shell.cli.CliArgs;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.log.AnsiFormattedText;
import org.neo4j.shell.log.AnsiLogger;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.terminal.CypherShellTerminal;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.shell.ShellRunner.shouldBeInteractive;
import static org.neo4j.shell.terminal.CypherShellTerminalBuilder.terminalBuilder;
import static org.neo4j.shell.util.Versions.isPasswordChangeRequiredException;

public class Main
{
    public static final int EXIT_FAILURE = 1;
    public static final int EXIT_SUCCESS = 0;
    static final String NEO_CLIENT_ERROR_SECURITY_UNAUTHORIZED = "Neo.ClientError.Security.Unauthorized";
    private final CliArgs args;
    private final Logger logger;
    private final CypherShell shell;
    private final boolean isOutputInteractive;
    private final ShellRunner.Factory runnerFactory;
    private final CypherShellTerminal terminal;
    private final StatementParser statementParser = new ShellStatementParser();
    private final ParameterService parameters;

    public Main( CliArgs args )
    {
        boolean isInteractive = !args.getNonInteractive() && ShellRunner.isInputInteractive();
        this.logger = new AnsiLogger( args.getDebugMode(), Format.VERBOSE, System.out, System.err );
        this.terminal = terminalBuilder().interactive( isInteractive ).logger( logger ).build();
        this.args = args;
        var boltStateHandler = new BoltStateHandler( shouldBeInteractive( args, terminal.isInteractive() ) );
        this.parameters = ParameterService.create( boltStateHandler );
        this.shell = new CypherShell( logger, boltStateHandler, new PrettyPrinter( new PrettyConfig( args ) ), parameters );
        this.isOutputInteractive = !args.getNonInteractive() && ShellRunner.isOutputInteractive();
        this.runnerFactory = new ShellRunner.Factory();
    }

    @VisibleForTesting
    public Main( CliArgs args, PrintStream out, PrintStream err, boolean outputInteractive, CypherShellTerminal terminal )
    {
        this.terminal = terminal;
        this.args = args;
        this.logger = new AnsiLogger( args.getDebugMode(), Format.VERBOSE, out, err );
        var boltStateHandler = new BoltStateHandler( shouldBeInteractive( args, terminal.isInteractive() ) );
        this.parameters = ParameterService.create( boltStateHandler );
        this.shell = new CypherShell( logger, boltStateHandler, new PrettyPrinter( new PrettyConfig( args ) ), parameters );
        this.isOutputInteractive = outputInteractive;
        this.runnerFactory = new ShellRunner.Factory();
    }

    @VisibleForTesting
    public Main( CliArgs args, AnsiLogger logger, CypherShell shell, ParameterService parameters,
                 boolean outputInteractive, ShellRunner.Factory runnerFactory, CypherShellTerminal terminal )
    {
        this.terminal = terminal;
        this.args = args;
        this.logger = logger;
        this.shell = shell;
        this.isOutputInteractive = outputInteractive;
        this.runnerFactory = runnerFactory;
        this.parameters = parameters;
    }

    public static void main( String[] args )
    {
        CliArgs cliArgs = CliArgHelper.parse( args );

        // if null, then command line parsing went wrong
        // CliArgs has already printed errors.
        if ( cliArgs == null )
        {
            System.exit( 1 );
        }

        System.exit( new Main( cliArgs ).startShell() );
    }

    public int startShell()
    {
        if ( args.getVersion() )
        {
            terminal.write().println( "Cypher-Shell " + Build.version() );
            return EXIT_SUCCESS;
        }
        if ( args.getDriverVersion() )
        {
            terminal.write().println( "Neo4j Driver " + Build.driverVersion() );
            return EXIT_SUCCESS;
        }
        if ( args.getChangePassword() )
        {
            return runSetNewPassword();
        }

        return runShell();
    }

    private int runSetNewPassword()
    {
        try
        {
            promptAndChangePassword( args.connectionConfig(), null );
        }
        catch ( Exception e )
        {
            logger.printError( "Failed to change password: " + e.getMessage() );
            return EXIT_FAILURE;
        }
        return EXIT_SUCCESS;
    }

    private int runShell()
    {
        ConnectionConfig connectionConfig = args.connectionConfig();
        try
        {
            //If user is passing in a cypher statement just run that and be done with it
            if ( args.getCypher().isPresent() )
            {
                // Can only prompt for password if input has not been redirected
                connectMaybeInteractively( connectionConfig );
                shell.execute( statementParser.parse( args.getCypher().get() ).statements() );
                return EXIT_SUCCESS;
            }
            else
            {
                // Can only prompt for password if input has not been redirected
                var newConnectionConfig = connectMaybeInteractively( connectionConfig );

                if ( !newConnectionConfig.driverUrl().equals( connectionConfig.driverUrl() ) )
                {
                    var fallbackWarning = "Failed to connect to " + connectionConfig.driverUrl() + ", fallback to " + newConnectionConfig.driverUrl();
                    logger.printIfVerbose( AnsiFormattedText.s().colorOrange().append( fallbackWarning ).formattedString() );
                }

                // Construct shellrunner after connecting, due to interrupt handling
                ShellRunner shellRunner = runnerFactory.create( args, shell, logger, newConnectionConfig, terminal );
                CommandHelper commandHelper = new CommandHelper( logger, shellRunner.getHistorian(), shell, newConnectionConfig, terminal, parameters );

                shell.setCommandHelper( commandHelper );

                return shellRunner.runUntilEnd();
            }
        }
        catch ( Throwable e )
        {
            logger.printError( e );
            return EXIT_FAILURE;
        }
    }

    /**
     * Connect the shell to the server, and try to handle missing passwords and such.
     *
     * @return connection configuration used to connect (can be different from the supplied)
     */
    private ConnectionConfig connectMaybeInteractively( ConnectionConfig connectionConfig ) throws Exception
    {
        boolean didPrompt = false;

        // Prompt directly in interactive mode if user provided username but not password
        if ( terminal.isInteractive() && !connectionConfig.username().isEmpty() && connectionConfig.password().isEmpty() )
        {
            promptForUsernameAndPassword( connectionConfig );
            didPrompt = true;
        }

        while ( true )
        {
            try
            {
                // Try to connect
                var newConfig = shell.connect( connectionConfig );
                setArgumentParameters();
                return newConfig;
            }
            catch ( AuthenticationException e )
            {
                // Fail if we already prompted,
                // or do not have interactive input,
                // or already tried with both username and password
                if ( didPrompt || !terminal.isInteractive() || !connectionConfig.username().isEmpty() && !connectionConfig.password().isEmpty() )
                {
                    throw e;
                }

                // Otherwise we prompt for username and password, and try to connect again
                promptForUsernameAndPassword( connectionConfig );
                didPrompt = true;
            }
            catch ( Neo4jException e )
            {
                if ( terminal.isInteractive() && isPasswordChangeRequiredException( e ) )
                {
                    promptAndChangePassword( connectionConfig, "Password change required" );
                    didPrompt = true;
                }
                else
                {
                    throw e;
                }
            }
        }
    }

    private void setArgumentParameters() throws CommandException
    {
        for ( var parameter : args.getParameters() )
        {
            parameters.setParameter( parameters.evaluate( parameter ) );
        }
    }

    private void promptForUsernameAndPassword( ConnectionConfig connectionConfig ) throws Exception
    {
        if ( connectionConfig.username().isEmpty() )
        {
            String username = isOutputInteractive ?
                    promptForNonEmptyText( "username", null ) :
                    promptForText( "username", null );
            connectionConfig.setUsername( username );
        }
        if ( connectionConfig.password().isEmpty() )
        {
            connectionConfig.setPassword( promptForText( "password", '*' ) );
        }
    }

    private ConnectionConfig promptAndChangePassword( ConnectionConfig connectionConfig, String message ) throws Exception
    {
        if ( message != null )
        {
            terminal.write().println( message );
        }
        if ( connectionConfig.username().isEmpty() )
        {
            String username = isOutputInteractive ?
                    promptForNonEmptyText( "username", null ) :
                    promptForText( "username", null );
            connectionConfig.setUsername( username );
        }
        if ( connectionConfig.password().isEmpty() )
        {
            connectionConfig.setPassword( promptForText( "password", '*' ) );
        }
        String newPassword = isOutputInteractive ?
                             promptForNonEmptyText( "new password", '*' ) :
                             promptForText( "new password", '*' );
        String reenteredNewPassword = promptForText( "confirm password", '*' );

        if ( !reenteredNewPassword.equals( newPassword ) )
        {
            throw new CommandException( "Passwords are not matching." );
        }

        shell.changePassword( connectionConfig, newPassword );
        connectionConfig.setPassword( newPassword );
        return connectionConfig;
    }

    @VisibleForTesting
    protected CypherShell getCypherShell()
    {
        return shell;
    }

    private String promptForNonEmptyText( String prompt, Character mask ) throws Exception
    {
        String text = promptForText( prompt, mask );
        if ( !text.isEmpty() )
        {
            return text;
        }
        terminal.write().println( prompt + " cannot be empty" );
        terminal.write().println();
        return promptForNonEmptyText( prompt, mask );
    }

    private String promptForText( String prompt, Character mask ) throws CommandException
    {
        try
        {
            return terminal.read().simplePrompt( prompt + ": ", mask );
        }
        catch ( NoMoreInputException | UserInterruptException e )
        {
            throw new CommandException( "No text could be read, exiting..." );
        }
    }
}
