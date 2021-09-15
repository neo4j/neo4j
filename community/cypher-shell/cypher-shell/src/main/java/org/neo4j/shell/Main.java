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

import jline.console.ConsoleReader;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.shell.build.Build;
import org.neo4j.shell.cli.CliArgHelper;
import org.neo4j.shell.cli.CliArgs;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ThrowingAction;
import org.neo4j.shell.log.AnsiFormattedText;
import org.neo4j.shell.log.AnsiLogger;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.shell.util.Versions.isPasswordChangeRequiredException;

public class Main
{
    public static final int EXIT_FAILURE = 1;
    public static final int EXIT_SUCCESS = 0;
    static final String NEO_CLIENT_ERROR_SECURITY_UNAUTHORIZED = "Neo.ClientError.Security.Unauthorized";
    private final CliArgs args;
    private final InputStream in;
    private final OutputStream out;
    private final Logger logger;
    private final CypherShell shell;
    private final boolean isInputInteractive;
    private final boolean isOutputInteractive;
    private final ShellRunner.Factory runnerFactory;

    public Main( CliArgs args )
    {
        this( args, System.in, ShellRunner.getOutputStreamForInteractivePrompt(), System.err,
              !args.getNonInteractive() && ShellRunner.isInputInteractive(),
              !args.getNonInteractive() && ShellRunner.isOutputInteractive() );
    }

    @VisibleForTesting
    public Main( CliArgs args, InputStream in, OutputStream out, OutputStream err, boolean inputInteractive, boolean outputInteractive )
    {
        this.args = args;
        this.in = in;
        this.out = out;
        this.logger = new AnsiLogger( args.getDebugMode(), Format.VERBOSE, new PrintStream( out ), new PrintStream( err ) );
        this.shell = new CypherShell( logger, new PrettyConfig( args ), ShellRunner.shouldBeInteractive( args, inputInteractive ), args.getParameters() );
        this.isInputInteractive = inputInteractive;
        this.isOutputInteractive = outputInteractive;
        this.runnerFactory = new ShellRunner.Factory();
    }

    @VisibleForTesting
    public Main( CliArgs args, InputStream in, OutputStream out, AnsiLogger logger, CypherShell shell,
          boolean inputInteractive, boolean outputInteractive, ShellRunner.Factory runnerFactory )
    {
        this.args = args;
        this.in = in;
        this.out = out;
        this.logger = logger;
        this.shell = shell;
        this.isInputInteractive = inputInteractive;
        this.isOutputInteractive = outputInteractive;
        this.runnerFactory = runnerFactory;
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
            logger.printOut( "Cypher-Shell " + Build.version() );
            return EXIT_SUCCESS;
        }
        if ( args.getDriverVersion() )
        {
            logger.printOut( "Neo4j Driver " + Build.driverVersion() );
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
                connectMaybeInteractively( connectionConfig, () -> shell.execute( args.getCypher().get() ) );
                return EXIT_SUCCESS;
            }
            else
            {
                // Can only prompt for password if input has not been redirected
                var newConnectionConfig = connectMaybeInteractively( connectionConfig, null );

                if ( !newConnectionConfig.driverUrl().equals( connectionConfig.driverUrl() ) )
                {
                    var fallbackWarning = "Failed to connect to " + connectionConfig.driverUrl() + ", fallback to " + newConnectionConfig.driverUrl();
                    logger.printIfVerbose( AnsiFormattedText.s().colorOrange().append( fallbackWarning ).formattedString() );
                }

                // Construct shellrunner after connecting, due to interrupt handling
                ShellRunner shellRunner = runnerFactory.create( args, shell, logger, newConnectionConfig, in, isInputInteractive );
                CommandHelper commandHelper = new CommandHelper( logger, shellRunner.getHistorian(), shell );

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
    private ConnectionConfig connectMaybeInteractively( ConnectionConfig connectionConfig, ThrowingAction<CommandException> command ) throws Exception
    {
        boolean didPrompt = false;

        // Prompt directly in interactive mode if user provided username but not password
        if ( isInputInteractive && !connectionConfig.username().isEmpty() && connectionConfig.password().isEmpty() )
        {
            promptForUsernameAndPassword( connectionConfig );
            didPrompt = true;
        }

        while ( true )
        {
            try
            {
                // Try to connect
                return shell.connect( connectionConfig, command );
            }
            catch ( AuthenticationException e )
            {
                // Fail if we already prompted,
                // or do not have interactive input,
                // or already tried with both username and password
                if ( didPrompt || !isInputInteractive || !connectionConfig.username().isEmpty() && !connectionConfig.password().isEmpty() )
                {
                    throw e;
                }

                // Otherwise we prompt for username and password, and try to connect again
                promptForUsernameAndPassword( connectionConfig );
                didPrompt = true;
            }
            catch ( Neo4jException e )
            {
                if ( !args.getNonInteractive() && isPasswordChangeRequiredException( e ) )
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

    private void promptForUsernameAndPassword( ConnectionConfig connectionConfig ) throws Exception
    {
        try ( ConsoleReader consoleReader = new ConsoleReader( in, out ) )
        {
            // Disable expansion of bangs: !
            consoleReader.setExpandEvents( false );
            // Ensure Reader does not handle user input for ctrl+C behaviour
            consoleReader.setHandleUserInterrupt( false );
            if ( connectionConfig.username().isEmpty() )
            {
                String username = isOutputInteractive ?
                        promptForNonEmptyText( "username", consoleReader, null ) :
                        promptForText( "username", consoleReader, null );
                connectionConfig.setUsername( username );
            }
            if ( connectionConfig.password().isEmpty() )
            {
                connectionConfig.setPassword( promptForText( "password", consoleReader, '*' ) );
            }
        }
    }

    private ConnectionConfig promptAndChangePassword( ConnectionConfig connectionConfig, String message ) throws Exception
    {
        try ( ConsoleReader consoleReader = new ConsoleReader( in, out ) )
        {
            // Disable expansion of bangs: !
            consoleReader.setExpandEvents( false );
            // Ensure Reader does not handle user input for ctrl+C behaviour
            consoleReader.setHandleUserInterrupt( false );

            if ( message != null )
            {
                consoleReader.println( message );
            }
            if ( connectionConfig.username().isEmpty() )
            {
                String username = isOutputInteractive ?
                        promptForNonEmptyText( "username", consoleReader, null ) :
                        promptForText( "username", consoleReader, null );
                connectionConfig.setUsername( username );
            }
            if ( connectionConfig.password().isEmpty() )
            {
                connectionConfig.setPassword( promptForText( "password", consoleReader, '*' ) );
            }
            String newPassword = isOutputInteractive ?
                    promptForNonEmptyText( "new password", consoleReader, '*' ) :
                    promptForText( "new password", consoleReader, '*' );

            shell.changePassword( connectionConfig, newPassword );
            connectionConfig.setPassword( newPassword );
        }
        return connectionConfig;
    }

    @VisibleForTesting
    protected CypherShell getCypherShell()
    {
        return shell;
    }

    /**
     * @param prompt to display to the user
     * @param mask   single character to display instead of what the user is typing, use null if text is not secret
     * @return the text which was entered
     * @throws Exception in case of errors
     */
    private static String promptForNonEmptyText( String prompt, ConsoleReader consoleReader, Character mask ) throws Exception
    {
        String text = promptForText( prompt, consoleReader, mask );
        if ( !text.isEmpty() )
        {
            return text;
        }
        consoleReader.println( prompt + " cannot be empty" );
        consoleReader.println();
        return promptForNonEmptyText( prompt, consoleReader, mask );
    }

    /**
     * @param prompt        to display to the user
     * @param mask          single character to display instead of what the user is typing, use null if text is not secret
     * @param consoleReader the reader
     * @return the text which was entered
     * @throws Exception in case of errors
     */
    private static String promptForText( String prompt, ConsoleReader consoleReader, Character mask ) throws Exception
    {
        String line = consoleReader.readLine( prompt + ": ", mask );
        if ( line == null )
        {
            throw new CommandException( "No text could be read, exiting..." );
        }
        return line;
    }
}
