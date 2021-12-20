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
package org.neo4j.shell.cli;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.Connector;
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
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.log.AnsiFormattedText;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.terminal.CypherShellTerminal;
import org.neo4j.shell.terminal.CypherShellTerminal.UserInterruptHandler;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.DATABASE_UNAVAILABLE_ERROR_CODE;
import static org.neo4j.shell.terminal.CypherShellTerminal.PROMPT_MAX_LENGTH;

/**
 * A shell runner intended for interactive sessions where lines are input one by one and execution should happen along the way.
 */
public class InteractiveShellRunner implements ShellRunner, UserInterruptHandler
{
    static final String INTERRUPT_SIGNAL = "INT";
    static final String UNRESOLVED_DEFAULT_DB_PROPMPT_TEXT = "<default_database>";
    static final String DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT = "[UNAVAILABLE]";
    private static final String FRESH_PROMPT = "> ";
    private static final String TRANSACTION_PROMPT = "# ";
    private static final String USERNAME_DB_DELIMITER = "@";
    // Need to know if we are currently executing when catch Ctrl-C, needs to be atomic due to
    // being called from different thread
    private final AtomicBoolean currentlyExecuting;

    private final Logger logger;
    private final CypherShellTerminal terminal;
    private final TransactionHandler txHandler;
    private final DatabaseManager databaseManager;
    private final StatementExecuter executer;
    private final UserMessagesHandler userMessagesHandler;
    private final ConnectionConfig connectionConfig;
    private final Connector connector;

    public InteractiveShellRunner( StatementExecuter executer,
                                   TransactionHandler txHandler,
                                   DatabaseManager databaseManager,
                                   Connector connector,
                                   Logger logger,
                                   CypherShellTerminal terminal,
                                   UserMessagesHandler userMessagesHandler,
                                   ConnectionConfig connectionConfig,
                                   File historyFile )
    {
        this.userMessagesHandler = userMessagesHandler;
        this.currentlyExecuting = new AtomicBoolean( false );
        this.executer = executer;
        this.txHandler = txHandler;
        this.databaseManager = databaseManager;
        this.connector = connector;
        this.logger = logger;
        this.terminal = terminal;
        this.connectionConfig = connectionConfig;
        setupHistory( historyFile );

        // Catch ctrl-c
        terminal.bindUserInterruptHandler( this );
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
        logger.printIfVerbose( UserMessagesHandler.getExitMessage() );
        flushHistory();
        return exitCode;
    }

    @Override
    public Historian getHistorian()
    {
        return terminal.getHistory();
    }

    /**
     * Reads from the InputStream until one or more statements can be found.
     *
     * @return a list of command statements
     * @throws NoMoreInputException if there is no more input
     */
    @VisibleForTesting
    protected List<String> readUntilStatement() throws NoMoreInputException
    {
        while ( true )
        {
            try
            {
                return terminal.read().readStatement( updateAndGetPrompt() ).parsed();
            }
            catch ( UserInterruptException e )
            {
                handleUserInterrupt();
            }
        }
    }

    /**
     * @return suitable prompt depending on current parsing state
     */
    private AnsiFormattedText updateAndGetPrompt()
    {
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

        AnsiFormattedText prePrompt = getPrePrompt( databaseName );

        // If we encountered an error with the connection ping query we display it in the prompt in RED
        if ( !errorSuffix.isEmpty() )
        {
            prePrompt.colorRed().append( errorSuffix ).colorDefault();
        }

        if ( promptIndent <= PROMPT_MAX_LENGTH )
        {
            return prePrompt
                    .append( txHandler.isTransactionOpen() ? TRANSACTION_PROMPT : FRESH_PROMPT );
        }
        else
        {
            return prePrompt
                    .appendNewLine()
                    .append( txHandler.isTransactionOpen() ? TRANSACTION_PROMPT : FRESH_PROMPT );
        }
    }

    private AnsiFormattedText getPrePrompt( String databaseName )
    {
        AnsiFormattedText prePrompt = AnsiFormattedText.s().bold();

        if ( connector.isConnected() )
        {
            prePrompt
                    .append( connectionConfig.username() )
                    .append( "@" )
                    .append( databaseName );
        }
        else
        {
            prePrompt.append( "Disconnected" );
        }
        return prePrompt;
    }

    private static String getErrorPrompt( String errorCode )
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

    private void setupHistory( File historyFile )
    {
        var dir = historyFile.getParentFile();

        if ( !dir.isDirectory() && !dir.mkdir() )
        {
            logger.printError( "Could not load history file. Falling back to session-based history.\n" );
        }
        else
        {
            terminal.setHistoryFile( historyFile );
        }
    }

    private void flushHistory()
    {
        try
        {
            getHistorian().flushHistory();
        }
        catch ( IOException e )
        {
            logger.printError( "Failed to save history: " + e.getMessage() );
        }
    }

    @Override
    public void handleUserInterrupt()
    {
        // Stop any running cypher statements
        if ( currentlyExecuting.get() )
        {
            logger.printError( "Stopping query..." ); // Stopping execution can take some time
            executer.reset();
        }
        else
        {
            logger.printError(
                AnsiFormattedText.s().colorRed()
                    .append( "Interrupted (Note that Cypher queries must end with a " ).bold( "semicolon" )
                    .append( ". Type " ).bold( Exit.COMMAND_NAME ).append( " to exit the shell.)" )
                    .formattedString() );
        }
    }
}
