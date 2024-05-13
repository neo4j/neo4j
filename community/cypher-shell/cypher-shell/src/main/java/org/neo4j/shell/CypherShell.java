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
package org.neo4j.shell;

import static java.lang.String.format;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.DiscoveryException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.shell.cli.AccessMode;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatement;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.printer.AnsiFormattedText;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.state.BoltResult;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.state.LicenseDetails;

/**
 * A possibly interactive shell for evaluating cypher statements.
 */
public class CypherShell implements StatementExecuter, Connector, TransactionHandler, DatabaseManager {
    private static final Logger log = Logger.create();
    private static final String LICENSE_EXPIRED_WARNING =
            """
            Thank you for installing Neo4j. This is a time limited trial, and the
            %d days have expired. Please contact https://neo4j.com/contact-us/
            to continue using the software. Use of this Software without
            a proper commercial or evaluation license with Neo4j, Inc. or
            its affiliates is prohibited.
            """;
    private static final String LICENSE_DAYS_LEFT_WARNING =
            """
            Thank you for installing Neo4j. This is a time limited trial.
            You have %d days remaining out of %d days. Please
            contact https://neo4j.com/contact-us/ if you require more time.
            """;
    private static final String LICENSE_NOT_ACCEPTED_WARNING =
            """
            A Neo4j license has not been accepted. To accept the commercial license agreement, run
                neo4j-admin server license --accept-commercial.
            To accept the terms of the evaluation agreement, run
                neo4j-admin server license --accept-evaluation.

            (c) Neo4j Sweden AB. All Rights Reserved.
            Use of this Software without a proper commercial license, or evaluation license
            with Neo4j, Inc. or its affiliates is prohibited.
            Neo4j has the right to terminate your usage if you are not compliant.

            Please contact us about licensing via https://neo4j.com/contact-us/
            """;

    private final ParameterService parameters;
    private final Printer printer;
    private final BoltStateHandler boltStateHandler;
    private final PrettyPrinter prettyPrinter;
    private CommandHelper commandHelper;
    private String lastNeo4jErrorCode;

    public CypherShell(
            Printer printer,
            BoltStateHandler boltStateHandler,
            PrettyPrinter prettyPrinter,
            ParameterService parameters) {
        this.printer = printer;
        this.boltStateHandler = boltStateHandler;
        this.prettyPrinter = prettyPrinter;
        this.parameters = parameters;
        addRuntimeHookToResetShell();
    }

    @Override
    public void execute(final ParsedStatement statement) throws ExitException, CommandException {
        if (statement instanceof CommandStatement commandStatement) {
            executeCommand(commandStatement);
        } else if (!statement.statement().isBlank()) {
            executeCypher(statement.statement());
        }
    }

    @Override
    public void execute(List<ParsedStatement> statements) throws ExitException, CommandException {
        for (var statement : statements) {
            execute(statement);
        }
    }

    @Override
    public String lastNeo4jErrorCode() {
        return lastNeo4jErrorCode;
    }

    /**
     * Executes a piece of text as if it were Cypher. By default, all of the cypher is executed in single statement (with an implicit transaction).
     *
     * @param cypher non-empty cypher text to executeLine
     */
    private void executeCypher(final String cypher) throws CommandException {
        log.info("Executing cypher: " + cypher);

        if (!isConnected()) {
            throw new CommandException("Not connected to Neo4j");
        }

        try {
            final Optional<BoltResult> result = boltStateHandler.runUserCypher(cypher, parameters.parameters());
            result.ifPresent(boltResult -> {
                prettyPrinter.format(boltResult, printer);
                boltStateHandler.updateActualDbName(boltResult.getSummary());
            });
            lastNeo4jErrorCode = null;
        } catch (Neo4jException e) {
            log.error(e);
            lastNeo4jErrorCode = getErrorCode(e);
            throw boltStateHandler.handleException(e);
        }
    }

    @Override
    public boolean isConnected() {
        return boltStateHandler.isConnected();
    }

    private void executeCommand(final CommandStatement statement) throws CommandException {
        log.info("Executing command: " + statement.statement());
        var command = commandHelper.getCommand(statement.name());
        if (command == null) {
            throw new CommandException(
                    "Could not find command " + statement.name() + ", use :help to see available commands");
        }
        command.execute(statement.args());
    }

    /**
     * Open a session to Neo4j
     */
    @Override
    public void connect(ConnectionConfig connectionConfig) throws CommandException {
        boltStateHandler.connect(connectionConfig);
    }

    @Override
    public void connect(String user, String password, String database) throws CommandException {
        boltStateHandler.connect(user, password, database);
    }

    @Override
    public void impersonate(String impersonatedUser) throws CommandException {
        boltStateHandler.impersonate(impersonatedUser);
    }

    @Override
    public void reconnect() throws CommandException {
        boltStateHandler.reconnect();
    }

    @Override
    public void reconnect(AccessMode accessMode) throws CommandException {
        boltStateHandler.reconnect(accessMode);
    }

    @Override
    public String getServerVersion() {
        return boltStateHandler.getServerVersion();
    }

    @Override
    public String getProtocolVersion() {
        return boltStateHandler.getProtocolVersion();
    }

    @Override
    public String username() {
        return boltStateHandler.username();
    }

    @Override
    public ConnectionConfig connectionConfig() {
        return boltStateHandler.connectionConfig();
    }

    @Override
    public Optional<String> impersonatedUser() {
        return boltStateHandler.impersonatedUser();
    }

    @Override
    public void beginTransaction() throws CommandException {
        boltStateHandler.beginTransaction();
    }

    @Override
    public void commitTransaction() throws CommandException {
        try {
            boltStateHandler.commitTransaction();
            lastNeo4jErrorCode = null;
        } catch (Neo4jException e) {
            log.error(e);
            lastNeo4jErrorCode = getErrorCode(e);
            throw e;
        }
    }

    @Override
    public void rollbackTransaction() throws CommandException {
        boltStateHandler.rollbackTransaction();
    }

    @Override
    public boolean isTransactionOpen() {
        return boltStateHandler.isTransactionOpen();
    }

    @Override
    public Optional<BoltResult> runUserCypher(String cypher, Map<String, Value> queryParams) throws CommandException {
        return boltStateHandler.runUserCypher(cypher, queryParams);
    }

    @Override
    public Optional<BoltResult> runCypher(String cypher, Map<String, Value> queryParams, TransactionType type)
            throws CommandException {
        return boltStateHandler.runCypher(cypher, queryParams, type);
    }

    public void setCommandHelper(CommandHelper commandHelper) {
        this.commandHelper = commandHelper;
    }

    @Override
    public void reset() {
        boltStateHandler.reset();
    }

    protected void addRuntimeHookToResetShell() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::reset));
    }

    @Override
    public void setActiveDatabase(String databaseName) throws CommandException {
        try {
            boltStateHandler.setActiveDatabase(databaseName);
            lastNeo4jErrorCode = null;
        } catch (Neo4jException e) {
            log.error(e);
            lastNeo4jErrorCode = getErrorCode(e);
            throw e;
        }
    }

    @Override
    public String getActiveDatabaseAsSetByUser() {
        return boltStateHandler.getActiveDatabaseAsSetByUser();
    }

    @Override
    public String getActualDatabaseAsReportedByServer() {
        return boltStateHandler.getActualDatabaseAsReportedByServer();
    }

    public void changePassword(ConnectionConfig connectionConfig, String newPassword) {
        boltStateHandler.changePassword(connectionConfig, newPassword);
    }

    @Override
    public void disconnect() {
        boltStateHandler.disconnect();
    }

    private static String getErrorCode(Neo4jException e) {
        Neo4jException statusException = e;

        // If we encountered a later suppressed Neo4jException we use that as the basis for the status instead
        Throwable[] suppressed = e.getSuppressed();
        for (Throwable s : suppressed) {
            if (s instanceof Neo4jException) {
                statusException = (Neo4jException) s;
                break;
            }
        }

        if (statusException instanceof ServiceUnavailableException || statusException instanceof DiscoveryException) {
            // Treat this the same way as a DatabaseUnavailable error for now.
            return DATABASE_UNAVAILABLE_ERROR_CODE;
        }
        return statusException.code();
    }

    public void printFallbackWarning(URI originalUri) {
        final var newUri = connectionConfig().uri();
        if (!newUri.equals(originalUri)) {
            var fallbackWarning = format("Failed to connect to %s, fallback to %s", originalUri, newUri);
            printer.printIfVerbose(AnsiFormattedText.s().orange(fallbackWarning).resetAndRender());
        }
    }

    public void printLicenseWarnings() {
        final var license = boltStateHandler.licenseDetails();
        if (license.status() == LicenseDetails.Status.NO) {
            printer.printOut(AnsiFormattedText.s()
                    .orange(format(LICENSE_NOT_ACCEPTED_WARNING))
                    .resetAndRender());
        } else if (license.status() == LicenseDetails.Status.EXPIRED
                && license.trialDays().isPresent()) {
            printer.printOut(AnsiFormattedText.s()
                    .orange(format(LICENSE_EXPIRED_WARNING, license.trialDays().get()))
                    .resetAndRender());
        } else if (license.status() == LicenseDetails.Status.EVAL
                && license.daysLeft().isPresent()
                && license.trialDays().isPresent()) {
            printer.printOut(format(
                    LICENSE_DAYS_LEFT_WARNING,
                    license.daysLeft().get(),
                    license.trialDays().get()));
        }
    }

    public AccessMode accessMode() {
        return boltStateHandler.accessMode();
    }
}
