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
package org.neo4j.shell.cli;

import jline.console.ConsoleReader;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nonnull;

import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.DatabaseManager;
import org.neo4j.shell.Historian;
import org.neo4j.shell.Main;
import org.neo4j.shell.ShellRunner;
import org.neo4j.shell.StatementExecuter;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.UserMessagesHandler;
import org.neo4j.shell.commands.Exit;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.log.AnsiFormattedText;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.prettyprint.OutputFormatter;

import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.DATABASE_UNAVAILABLE_ERROR_CODE;

/**
 * A shell runner intended for interactive sessions where lines are input one by one and execution should happen along the way.
 */
public class InteractiveShellRunner implements ShellRunner, SignalHandler
{
    static final String INTERRUPT_SIGNAL = "INT";
    static final String UNRESOLVED_DEFAULT_DB_PROPMPT_TEXT = "<default_database>";
    static final String DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT = "[UNAVAILABLE]";
    private static final String FRESH_PROMPT = "> ";
    private static final String TRANSACTION_PROMPT = "# ";
    private static final String USERNAME_DB_DELIMITER = "@";
    private static final int ONELINE_PROMPT_MAX_LENGTH = 50;
    // Need to know if we are currently executing when catch Ctrl-C, needs to be atomic due to
    // being called from different thread
    private final AtomicBoolean currentlyExecuting;

    @Nonnull
    private final Logger logger;
    @Nonnull
    private final ConsoleReader reader;
    @Nonnull
    private final Historian historian;
    @Nonnull
    private final StatementParser statementParser;
    @Nonnull
    private final TransactionHandler txHandler;
    @Nonnull
    private final DatabaseManager databaseManager;
    @Nonnull
    private final StatementExecuter executer;
    @Nonnull
    private final UserMessagesHandler userMessagesHandler;
    @Nonnull
    private final ConnectionConfig connectionConfig;

    private AnsiFormattedText continuationPrompt;

    public InteractiveShellRunner( @Nonnull StatementExecuter executer,
                                   @Nonnull TransactionHandler txHandler,
                                   @Nonnull DatabaseManager databaseManager,
                                   @Nonnull Logger logger,
                                   @Nonnull StatementParser statementParser,
                                   @Nonnull InputStream inputStream,
                                   @Nonnull File historyFile,
                                   @Nonnull UserMessagesHandler userMessagesHandler,
                                   @Nonnull ConnectionConfig connectionConfig ) throws IOException
    {
        this.userMessagesHandler = userMessagesHandler;
        this.currentlyExecuting = new AtomicBoolean( false );
        this.executer = executer;
        this.txHandler = txHandler;
        this.databaseManager = databaseManager;
        this.logger = logger;
        this.statementParser = statementParser;
        this.reader = setupConsoleReader( logger, inputStream );
        this.historian = FileHistorian.setupHistory( reader, logger, historyFile );
        this.connectionConfig = connectionConfig;

        // Catch ctrl-c
        Signal.handle( new Signal( INTERRUPT_SIGNAL ), this );
    }

    private ConsoleReader setupConsoleReader( @Nonnull Logger logger,
                                              @Nonnull InputStream inputStream ) throws IOException
    {
        ConsoleReader reader = new ConsoleReader( inputStream, logger.getOutputStream() );
        // Disable expansion of bangs: !
        reader.setExpandEvents( false );
        // Ensure Reader does not handle user input for ctrl+C behaviour
        reader.setHandleUserInterrupt( false );
        return reader;
    }

    @Override
    public int runUntilEnd()
    {
        int exitCode = Main.EXIT_SUCCESS;
        boolean running = true;

        logger.printIfVerbose( userMessagesHandler.getWelcomeMessage() );

        while ( running )
        {
            try
            {
                for ( String statement : readUntilStatement() )
                {
                    currentlyExecuting.set( true );
                    executer.execute( statement );
                    currentlyExecuting.set( false );
                }
            }
            catch ( ExitException e )
            {
                exitCode = e.getCode();
                running = false;
            }
            catch ( NoMoreInputException e )
            {
                // User pressed Ctrl-D and wants to exit
                running = false;
            }
            catch ( Throwable e )
            {
                logger.printError( e );
            }
            finally
            {
                currentlyExecuting.set( false );
            }
        }
        logger.printIfVerbose( userMessagesHandler.getExitMessage() );
        return exitCode;
    }

    @Nonnull
    @Override
    public Historian getHistorian()
    {
        return historian;
    }

    /**
     * Reads from the InputStream until one or more statements can be found.
     *
     * @return a list of command statements
     * @throws IOException
     * @throws NoMoreInputException
     */
    @Nonnull
    public List<String> readUntilStatement() throws IOException, NoMoreInputException
    {
        while ( true )
        {
            String line = reader.readLine( updateAndGetPrompt().renderedString() );
            if ( line == null )
            {
                // User hit CTRL-D, or file ended
                throw new NoMoreInputException();
            }

            // Empty lines are ignored if nothing has been read yet
            if ( line.trim().isEmpty() && !statementParser.containsText() )
            {
                continue;
            }

            statementParser.parseMoreText( line + "\n" );

            if ( statementParser.hasStatements() )
            {
                return statementParser.consumeStatements();
            }
        }
    }

    /**
     * @return suitable prompt depending on current parsing state
     */
    AnsiFormattedText updateAndGetPrompt()
    {
        if ( statementParser.containsText() )
        {
            return continuationPrompt;
        }

        String databaseName = databaseManager.getActualDatabaseAsReportedByServer();
        if ( databaseName == null || ABSENT_DB_NAME.equals( databaseName ) )
        {
            // We have failed to get a successful response from the connection ping query
            // Build the prompt from the db name as set by the user + a suffix indicating that we are in a disconnected state
            String dbNameSetByUser = databaseManager.getActiveDatabaseAsSetByUser();
            databaseName = ABSENT_DB_NAME.equals( dbNameSetByUser ) ? UNRESOLVED_DEFAULT_DB_PROPMPT_TEXT : dbNameSetByUser;
        }

        String errorSuffix = getErrorPrompt( executer.lastNeo4jErrorCode() );

        int promptIndent = connectionConfig.username().length() +
                           USERNAME_DB_DELIMITER.length() +
                           databaseName.length() +
                           errorSuffix.length() +
                           FRESH_PROMPT.length();

        AnsiFormattedText prePrompt = AnsiFormattedText.s().bold()
                                                       .append( connectionConfig.username() )
                                                       .append( "@" )
                                                       .append( databaseName );

        // If we encountered an error with the connection ping query we display it in the prompt in RED
        if ( !errorSuffix.isEmpty() )
        {
            prePrompt.colorRed().append( errorSuffix ).colorDefault();
        }

        if ( promptIndent <= ONELINE_PROMPT_MAX_LENGTH )
        {
            continuationPrompt = AnsiFormattedText.s().bold().append( OutputFormatter.repeat( ' ', promptIndent ) );
            return prePrompt
                    .append( txHandler.isTransactionOpen() ? TRANSACTION_PROMPT : FRESH_PROMPT );
        }
        else
        {
            continuationPrompt = AnsiFormattedText.s().bold();
            return prePrompt
                    .appendNewLine()
                    .append( txHandler.isTransactionOpen() ? TRANSACTION_PROMPT : FRESH_PROMPT );
        }
    }

    private String getErrorPrompt( String errorCode )
    {
        // NOTE: errorCode can be null
        String errorPromptSuffix;
        if ( DATABASE_UNAVAILABLE_ERROR_CODE.equals( errorCode ) )
        {
            errorPromptSuffix = DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT;
        }
        else
        {
            errorPromptSuffix = "";
        }
        return errorPromptSuffix;
    }

    /**
     * Catch Ctrl-C from user and handle it nicely
     *
     * @param signal to handle
     */
    @Override
    public void handle( final Signal signal )
    {
        // Stop any running cypher statements
        if ( currentlyExecuting.get() )
        {
            executer.reset();
        }
        else
        {
            // Print a literal newline here to get around us being in the middle of the prompt
            logger.printError(
                    AnsiFormattedText.s().colorRed()
                                     .append( "\nInterrupted (Note that Cypher queries must end with a " )
                                     .bold().append( "semicolon. " ).boldOff()
                                     .append( "Type " )
                                     .bold().append( Exit.COMMAND_NAME ).append( " " ).boldOff()
                                     .append( "to exit the shell.)" )
                                     .formattedString() );
            // Clear any text which has been inputted
            resetPrompt();
        }
    }

    /**
     * Clears the prompt of any text which has been inputted and redraws it.
     */
    private void resetPrompt()
    {
        try
        {
            // Clear whatever text has currently been inputted
            boolean more = true;
            while ( more )
            {
                more = reader.delete();
            }
            more = true;
            while ( more )
            {
                more = reader.backspace();
            }
            // Clear parser state
            statementParser.reset();

            // Redraw the prompt now because the error message has changed the terminal text
            reader.setPrompt( updateAndGetPrompt().renderedString() );
            reader.redrawLine();
            reader.flush();
        }
        catch ( IOException e )
        {
            logger.printError( e );
        }
    }
}
