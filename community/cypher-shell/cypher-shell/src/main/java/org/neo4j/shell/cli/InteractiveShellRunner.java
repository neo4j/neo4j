/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.cli;

import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.DATABASE_UNAVAILABLE_ERROR_CODE;
import static org.neo4j.shell.terminal.CypherShellTerminal.PROMPT_MAX_LENGTH;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.shell.Connector;
import org.neo4j.shell.DatabaseManager;
import org.neo4j.shell.Historian;
import org.neo4j.shell.Main;
import org.neo4j.shell.ShellRunner;
import org.neo4j.shell.StatementExecuter;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.UserMessagesHandler;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.StatementParser.ParsedStatement;
import org.neo4j.shell.printer.AnsiFormattedText;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.terminal.CypherShellTerminal;
import org.neo4j.shell.terminal.CypherShellTerminal.UserInterruptHandler;
import org.neo4j.util.VisibleForTesting;

/**
 * A shell runner intended for interactive sessions where lines are input one by one and execution should happen along the way.
 */
public class InteractiveShellRunner implements ShellRunner, UserInterruptHandler {
    private static final Logger log = Logger.create();
    static final String INTERRUPT_SIGNAL = "INT";
    static final String UNRESOLVED_DEFAULT_DB_PROPMPT_TEXT = "<default_database>";
    static final String DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT = "[UNAVAILABLE]";
    private static final String FRESH_PROMPT = "> ";
    private static final String TRANSACTION_PROMPT = "# ";
    private static final String USERNAME_DB_DELIMITER = "@";
    // Need to know if we are currently executing when catch Ctrl-C, needs to be atomic due to
    // being called from different thread
    private final AtomicBoolean currentlyExecuting;

    private final Printer printer;
    private final CypherShellTerminal terminal;
    private final TransactionHandler txHandler;
    private final DatabaseManager databaseManager;
    private final StatementExecuter executer;
    private final UserMessagesHandler userMessagesHandler;
    private final Connector connector;

    public InteractiveShellRunner(
            StatementExecuter executer,
            TransactionHandler txHandler,
            DatabaseManager databaseManager,
            Connector connector,
            Printer printer,
            CypherShellTerminal terminal,
            UserMessagesHandler userMessagesHandler,
            CypherShellTerminal.HistoryBehaviour historyBehaviour) {
        this.userMessagesHandler = userMessagesHandler;
        this.currentlyExecuting = new AtomicBoolean(false);
        this.executer = executer;
        this.txHandler = txHandler;
        this.databaseManager = databaseManager;
        this.connector = connector;
        this.printer = printer;
        this.terminal = terminal;
        setupHistory(historyBehaviour);

        // Catch ctrl-c
        terminal.bindUserInterruptHandler(this);
    }

    @Override
    public int runUntilEnd() {
        int exitCode = Main.EXIT_SUCCESS;
        boolean running = true;

        printer.printIfVerbose(userMessagesHandler.getWelcomeMessage());

        while (running) {
            try {
                for (ParsedStatement statement : readUntilStatement()) {
                    currentlyExecuting.set(true);
                    executer.execute(statement);
                    currentlyExecuting.set(false);
                }
            } catch (ExitException e) {
                log.info("ExitException code=" + e.getCode() + ", message=" + e.getMessage());
                exitCode = e.getCode();
                running = false;
            } catch (NoMoreInputException e) {
                log.info("No more user input.");
                // User pressed Ctrl-D and wants to exit
                running = false;
            } catch (Throwable e) {
                log.error(e);
                printer.printError(e);
            } finally {
                currentlyExecuting.set(false);
            }
        }
        printer.printIfVerbose(UserMessagesHandler.getExitMessage());
        flushHistory();
        return exitCode;
    }

    @Override
    public Historian getHistorian() {
        return terminal.getHistory();
    }

    @Override
    public boolean isInteractive() {
        return true;
    }

    /**
     * Reads from the InputStream until one or more statements can be found.
     *
     * @return a list of command statements
     * @throws NoMoreInputException if there is no more input
     */
    @VisibleForTesting
    protected List<ParsedStatement> readUntilStatement() throws NoMoreInputException {
        while (true) {
            try {
                return terminal.read().readStatement(updateAndGetPrompt()).statements();
            } catch (UserInterruptException e) {
                log.info("User interrupt.");
                handleUserInterrupt();
            }
        }
    }

    /**
     * @return suitable prompt depending on current parsing state
     */
    private AnsiFormattedText updateAndGetPrompt() {
        String databaseName = databaseManager.getActualDatabaseAsReportedByServer();
        if (databaseName == null || ABSENT_DB_NAME.equals(databaseName)) {
            // We have failed to get a successful response from the connection ping query
            // Build the prompt from the db name as set by the user + a suffix indicating that we are in a disconnected
            // state
            String dbNameSetByUser = databaseManager.getActiveDatabaseAsSetByUser();
            databaseName =
                    ABSENT_DB_NAME.equals(dbNameSetByUser) ? UNRESOLVED_DEFAULT_DB_PROPMPT_TEXT : dbNameSetByUser;
        }

        String errorSuffix = getErrorPrompt(executer.lastNeo4jErrorCode());

        int promptIndent = connector.username().length()
                + USERNAME_DB_DELIMITER.length()
                + databaseName.length()
                + errorSuffix.length()
                + FRESH_PROMPT.length();

        AnsiFormattedText prePrompt = getPrePrompt(databaseName);

        // If we encountered an error with the connection ping query we display it in the prompt in RED
        if (!errorSuffix.isEmpty()) {
            prePrompt.brightRed().append(errorSuffix).colorDefault();
        }

        if (promptIndent <= PROMPT_MAX_LENGTH) {
            return prePrompt.append(txHandler.isTransactionOpen() ? TRANSACTION_PROMPT : FRESH_PROMPT);
        } else {
            return prePrompt.newLine().append(txHandler.isTransactionOpen() ? TRANSACTION_PROMPT : FRESH_PROMPT);
        }
    }

    private AnsiFormattedText getPrePrompt(String databaseName) {
        final var prompt = new AnsiFormattedText();

        if (connector.isConnected()) {
            prompt.bold(connector.username());
            connector.impersonatedUser().ifPresent(impersonated -> prompt.append("(")
                    .bold(impersonated)
                    .append(")"));
            prompt.bold("@" + databaseName);
        } else {
            prompt.append("Disconnected");
        }
        return prompt;
    }

    private static String getErrorPrompt(String errorCode) {
        // NOTE: errorCode can be null
        String errorPromptSuffix;
        if (DATABASE_UNAVAILABLE_ERROR_CODE.equals(errorCode)) {
            errorPromptSuffix = DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT;
        } else {
            errorPromptSuffix = "";
        }
        return errorPromptSuffix;
    }

    private void setupHistory(CypherShellTerminal.HistoryBehaviour behaviour) {
        try {
            terminal.setHistoryBehaviour(behaviour);
        } catch (Exception e) {
            final var message = AnsiFormattedText.s()
                    .brightRed()
                    .append(String.format(
                            "Could not load history file, falling back to session-based history: %s%n", e.getMessage()))
                    .resetAndRender();
            printer.printError(message);
            log.error(e);
        }
    }

    private void flushHistory() {
        try {
            getHistorian().flushHistory();
        } catch (IOException e) {
            log.error(e);
            printer.printError("Failed to save history: " + e.getMessage());
        }
    }

    @Override
    public void handleUserInterrupt() {
        // Stop any running cypher statements
        if (currentlyExecuting.get()) {
            printer.printError("Stopping query..."); // Stopping execution can take some time
            executer.reset();
        } else {
            printer.printError(AnsiFormattedText.s()
                    .brightRed()
                    .append("Interrupted (Note that Cypher queries must end with a ")
                    .bold("semicolon")
                    .append(". Type ")
                    .bold(":exit")
                    .append(" to exit the shell.)")
                    .resetAndRender());
        }
    }
}
