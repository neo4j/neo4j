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
package org.neo4j.shell.state;

import static org.neo4j.shell.util.Versions.isPasswordChangeRequiredException;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.LogManager;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.Scheme;
import org.neo4j.driver.internal.logging.DevNullLogging;
import org.neo4j.driver.summary.DatabaseInfo;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.Connector;
import org.neo4j.shell.DatabaseManager;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.TriFunction;
import org.neo4j.shell.build.Build;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ThrowingAction;
import org.neo4j.shell.log.Logger;
import org.neo4j.util.VisibleForTesting;

/**
 * Handles interactions with the driver
 */
public class BoltStateHandler implements TransactionHandler, Connector, DatabaseManager {
    private static final Logger log = Logger.create();
    private static final String USER_AGENT = "neo4j-cypher-shell/v" + Build.version();
    private static final TransactionConfig USER_DIRECT_TX_CONF = txConfig(TransactionType.USER_DIRECT);
    private static final TransactionConfig SYSTEM_TX_CONF = txConfig(TransactionType.SYSTEM);
    private final TriFunction<URI, AuthToken, Config, Driver> driverProvider;
    private final boolean isInteractive;
    private final Map<String, Bookmark> bookmarks = new HashMap<>();
    protected Driver driver;
    Session session;
    private String protocolVersion;
    private String activeDatabaseNameAsSetByUser;
    private String actualDatabaseNameAsReportedByServer;
    private Transaction tx;
    private ConnectionConfig connectionConfig;
    private LicenseDetails licenseDetails = LicenseDetailsImpl.YES;
    private org.neo4j.shell.cli.AccessMode accessMode;

    public BoltStateHandler(boolean isInteractive, org.neo4j.shell.cli.AccessMode accessMode) {
        this(GraphDatabase::driver, isInteractive, accessMode);
    }

    @VisibleForTesting
    BoltStateHandler(TriFunction<URI, AuthToken, Config, Driver> driverProvider, boolean isInteractive) {
        this(driverProvider, isInteractive, org.neo4j.shell.cli.AccessMode.WRITE);
    }

    private BoltStateHandler(
            TriFunction<URI, AuthToken, Config, Driver> driverProvider,
            boolean isInteractive,
            org.neo4j.shell.cli.AccessMode accessMode) {
        this.driverProvider = driverProvider;
        this.accessMode = accessMode;
        activeDatabaseNameAsSetByUser = ABSENT_DB_NAME;
        this.isInteractive = isInteractive;
    }

    @Override
    public void setActiveDatabase(String databaseName) throws CommandException {
        if (isTransactionOpen()) {
            throw new CommandException(
                    "There is an open transaction. You need to close it before you can switch database.");
        }
        String previousDatabaseName = activeDatabaseNameAsSetByUser;
        activeDatabaseNameAsSetByUser = databaseName;
        try {
            if (isConnected()) {
                reconnectAndPing(databaseName, previousDatabaseName);
            }
        } catch (ClientException e) {
            if (isInteractive) {
                // We want to try to connect to the previous database
                activeDatabaseNameAsSetByUser = previousDatabaseName;
                try {
                    reconnectAndPing(previousDatabaseName, previousDatabaseName);
                } catch (Exception e2) {
                    e.addSuppressed(e2);
                }
            }
            throw e;
        }
    }

    @Override
    public String getActiveDatabaseAsSetByUser() {
        return activeDatabaseNameAsSetByUser;
    }

    @Override
    public String getActualDatabaseAsReportedByServer() {
        return actualDatabaseNameAsReportedByServer;
    }

    @Override
    public void beginTransaction() throws CommandException {
        if (!isConnected()) {
            throw new CommandException("Not connected to Neo4j");
        }
        if (isTransactionOpen()) {
            throw new CommandException("There is already an open transaction");
        }
        tx = session.beginTransaction(USER_DIRECT_TX_CONF);
    }

    @Override
    public void commitTransaction() throws CommandException {
        if (!isConnected()) {
            throw new CommandException("Not connected to Neo4j");
        }
        if (!isTransactionOpen()) {
            throw new CommandException("There is no open transaction to commit");
        }

        try {
            tx.commit();
            tx.close();
        } finally {
            tx = null;
        }
    }

    @Override
    public void rollbackTransaction() throws CommandException {
        if (!isConnected()) {
            throw new CommandException("Not connected to Neo4j");
        }
        if (!isTransactionOpen()) {
            throw new CommandException("There is no open transaction to rollback");
        }

        try {
            tx.rollback();
            tx.close();
        } finally {
            tx = null;
        }
    }

    /**
     * Handle an exception while getting or consuming the result. If not in TX, return the given exception. If in a TX, terminate the TX and return a more
     * verbose error message.
     *
     * @param e the thrown exception.
     * @return a suitable exception to rethrow.
     */
    public Neo4jException handleException(Neo4jException e) {
        if (isTransactionOpen()) {
            tx.close();
            tx = null;
            return new ErrorWhileInTransactionException(
                    "An error occurred while in an open transaction. The transaction will be rolled back and terminated. Error: "
                            + e.getMessage(),
                    e);
        } else {
            return e;
        }
    }

    @Override
    public boolean isTransactionOpen() {
        return tx != null;
    }

    @Override
    public boolean isConnected() {
        return session != null && session.isOpen();
    }

    @Override
    public void connect(String user, String password, String database) throws CommandException {
        connect(connectionConfig.withUsernameAndPasswordAndDatabase(user, password, database));
    }

    @Override
    public void impersonate(String impersonatedUser) throws CommandException {
        if (isTransactionOpen()) {
            throw new CommandException(
                    "There is an open transaction. You need to close it before starting impersonation.");
        }
        if (isConnected()) {
            disconnect();
        }
        connect(connectionConfig.withImpersonatedUser(impersonatedUser));
    }

    @Override
    public void reconnect(org.neo4j.shell.cli.AccessMode accessMode) throws CommandException {
        this.accessMode = accessMode;
        reconnect();
    }

    @Override
    public void reconnect() throws CommandException {
        if (!isConnected()) {
            throw new CommandException("Can't reconnect when unconnected.");
        }
        if (isTransactionOpen()) {
            throw new CommandException("There is an open transaction. You need to close it before you can reconnect.");
        }

        var config = this.connectionConfig;
        disconnect();
        connect(config);
    }

    @Override
    public void connect(ConnectionConfig incomingConfig) throws CommandException {
        if (isConnected()) {
            throw new CommandException("Already connected");
        }
        this.connectionConfig = clean(incomingConfig);
        final AuthToken authToken = AuthTokens.basic(connectionConfig.username(), connectionConfig.password());
        try {
            String previousDatabaseName = activeDatabaseNameAsSetByUser;
            try {
                activeDatabaseNameAsSetByUser = connectionConfig.database();
                driver = getDriver(connectionConfig, authToken);
                reconnectAndPing(activeDatabaseNameAsSetByUser, previousDatabaseName);
            } catch (ServiceUnavailableException | SessionExpiredException e) {
                String fallbackScheme =
                        switch (connectionConfig.uri().getScheme()) {
                            case Scheme.NEO4J_URI_SCHEME -> Scheme.BOLT_URI_SCHEME;
                            case Scheme.NEO4J_LOW_TRUST_URI_SCHEME -> Scheme.BOLT_LOW_TRUST_URI_SCHEME;
                            case Scheme.NEO4J_HIGH_TRUST_URI_SCHEME -> Scheme.BOLT_HIGH_TRUST_URI_SCHEME;
                            default -> throw e;
                        };
                this.connectionConfig = connectionConfig.withScheme(fallbackScheme);

                try {
                    driver = getDriver(connectionConfig, authToken);
                    log.info("Connecting with fallback scheme: " + fallbackScheme);
                    reconnectAndPing(activeDatabaseNameAsSetByUser, previousDatabaseName);
                } catch (Throwable fallbackThrowable) {
                    log.warn("Fallback scheme failed", fallbackThrowable);
                    // Throw the original exception to not cause confusion.
                    throw e;
                }
            }
        } catch (Throwable t) {
            try {
                silentDisconnect();
            } catch (Exception e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    private void reconnectAndPing(String databaseToConnectTo, String previousDatabase) throws CommandException {
        reconnect(databaseToConnectTo, previousDatabase);
        getPing().apply();
        licenseDetails = getTrialStatus();
    }

    private void reconnect(String databaseToConnectTo, String previousDatabase) {
        log.info("Connecting to database " + databaseToConnectTo + "...");
        SessionConfig.Builder builder = SessionConfig.builder();

        switch (accessMode) {
            case READ -> builder.withDefaultAccessMode(AccessMode.READ);
            case WRITE -> builder.withDefaultAccessMode(AccessMode.WRITE);
        }

        if (!ABSENT_DB_NAME.equals(databaseToConnectTo)) {
            builder.withDatabase(databaseToConnectTo);
        }
        closeSession(previousDatabase);
        final Bookmark bookmarkForDBToConnectTo = bookmarks.get(databaseToConnectTo);
        if (bookmarkForDBToConnectTo != null) {
            builder.withBookmarks(bookmarkForDBToConnectTo);
        }

        impersonatedUser().ifPresent(builder::withImpersonatedUser);

        session = driver.session(builder.build());

        resetActualDbName(); // Set this to null first in case run throws an exception
    }

    /**
     * Closes the session, if there is any. Saves a bookmark for the database currently connected to.
     *
     * @param databaseName the name of the database currently connected to
     */
    private void closeSession(String databaseName) {
        if (session != null) {
            // Save the last bookmark and close the session
            final Bookmark bookmarkForPreviousDB = session.lastBookmark();
            session.close();
            bookmarks.put(databaseName, bookmarkForPreviousDB);
        }
    }

    private ThrowingAction<CommandException> getPing() {
        return () -> {
            try {
                Result run = session.run("CALL db.ping()", SYSTEM_TX_CONF);
                ResultSummary summary = run.consume();
                BoltStateHandler.this.protocolVersion = summary.server().protocolVersion();
                updateActualDbName(summary);
            } catch (ClientException e) {
                log.warn("Ping failed", e);
                // In older versions there is no db.ping procedure, use legacy method.
                if (procedureNotFound(e)) {
                    Result run = session.run(isSystemDb() ? "CALL db.indexes()" : "RETURN 1", SYSTEM_TX_CONF);
                    ResultSummary summary = run.consume();
                    BoltStateHandler.this.protocolVersion = summary.server().protocolVersion();
                    updateActualDbName(summary);
                } else {
                    throw e;
                }
            }
        };
    }

    private LicenseDetails getTrialStatus() {
        try {
            final var record = session.run("CALL dbms.licenseAgreementDetails()", SYSTEM_TX_CONF)
                    .single();
            return LicenseDetails.parse(
                    record.get("status").asString(),
                    record.get("daysLeftOnTrial", 0L),
                    record.get("totalTrialDays", 0L));
        } catch (Exception e) {
            log.warn("Failed to fetch trial status", e);
            return LicenseDetailsImpl.YES;
        }
    }

    @Override
    public String getServerVersion() {
        try {
            return runCypher("CALL dbms.components() YIELD versions", Collections.emptyMap(), SYSTEM_TX_CONF)
                    .flatMap(recordOpt -> recordOpt.getRecords().stream().findFirst())
                    .map(record -> record.get("versions"))
                    .filter(value -> !value.isNull())
                    .map(value -> value.get(0).asString())
                    .orElse("");
        } catch (CommandException e) {
            log.warn("Failed to get server version", e);
            // On versions before 3.1.0-M09
            return "";
        }
    }

    @Override
    public String getProtocolVersion() {
        if (isConnected()) {
            if (protocolVersion == null) {
                // On versions before 3.1.0-M09
                protocolVersion = "";
            }

            return protocolVersion;
        }
        return "";
    }

    @Override
    public String username() {
        return connectionConfig != null ? connectionConfig.username() : "";
    }

    @Override
    public ConnectionConfig connectionConfig() {
        return connectionConfig;
    }

    @Override
    public Optional<String> impersonatedUser() {
        return Optional.ofNullable(connectionConfig).flatMap(ConnectionConfig::impersonatedUser);
    }

    @Override
    public Optional<BoltResult> runUserCypher(String cypher, Map<String, Value> queryParams) throws CommandException {
        return runCypher(cypher, queryParams, USER_DIRECT_TX_CONF);
    }

    @Override
    public Optional<BoltResult> runCypher(String cypher, Map<String, Value> queryParams, TransactionType type)
            throws CommandException {
        return runCypher(cypher, queryParams, txConfig(type));
    }

    private Optional<BoltResult> runCypher(String cypher, Map<String, Value> queryParams, TransactionConfig config)
            throws CommandException {
        if (!isConnected()) {
            throw new CommandException("Not connected to Neo4j");
        }
        if (isTransactionOpen()) {
            // If this fails, don't try any funny business - just let it die
            return getBoltResult(cypher, queryParams, config);
        } else {
            try {
                // Note that CALL IN TRANSACTIONS can't execute in an explicit transaction, so if the user has not typed
                // BEGIN, then
                // the statement should NOT be executed in a transaction.
                return getBoltResult(cypher, queryParams, config);
            } catch (SessionExpiredException e) {
                log.warn("Failed to execute query, re-trying", e);
                // Server is no longer accepting writes, reconnect and try again.
                // If it still fails, leave it up to the user
                reconnectAndPing(activeDatabaseNameAsSetByUser, activeDatabaseNameAsSetByUser);
                return getBoltResult(cypher, queryParams, config);
            }
        }
    }

    public void updateActualDbName(ResultSummary resultSummary) {
        actualDatabaseNameAsReportedByServer = getActualDbName(resultSummary);
    }

    public void changePassword(ConnectionConfig connectionConfig, String newPassword) {
        if (isConnected()) {
            silentDisconnect();
        }

        final AuthToken authToken = AuthTokens.basic(connectionConfig.username(), connectionConfig.password());

        try {
            driver = getDriver(connectionConfig, authToken);

            activeDatabaseNameAsSetByUser = SYSTEM_DB_NAME;
            reconnect(SYSTEM_DB_NAME, SYSTEM_DB_NAME);

            try {
                String command = "ALTER CURRENT USER SET PASSWORD FROM $o TO $n";
                Value parameters = Values.parameters("o", connectionConfig.password(), "n", newPassword);
                Result run = session.run(new Query(command, parameters), txConfig(TransactionType.USER_ACTION));
                run.consume();
            } catch (Neo4jException e) {
                if (isPasswordChangeRequiredException(e)) {
                    log.info("Password change failed, fallback to legacy method", e);
                    // In < 4.0 versions use legacy method.
                    String oldCommand = "CALL dbms.security.changePassword($n)";
                    Value oldParameters = Values.parameters("n", newPassword);
                    Result run =
                            session.run(new Query(oldCommand, oldParameters), txConfig(TransactionType.USER_ACTION));
                    run.consume();
                } else {
                    throw e;
                }
            }

            silentDisconnect();
        } catch (Throwable t) {
            try {
                silentDisconnect();
            } catch (Exception e) {
                t.addSuppressed(e);
            }
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            // The only checked exception is CommandException and we know that
            // we cannot get that since we supply an empty command.
            throw new RuntimeException(t);
        }
    }

    /**
     * @throws SessionExpiredException when server no longer serves writes anymore
     */
    private Optional<BoltResult> getBoltResult(String cypher, Map<String, Value> queryParams, TransactionConfig config)
            throws SessionExpiredException {
        Result statementResult;

        if (isTransactionOpen()) {
            statementResult = tx.run(new Query(cypher, Values.value(queryParams)));
        } else {
            statementResult = session.run(new Query(cypher, Values.value(queryParams)), config);
        }

        if (statementResult == null) {
            return Optional.empty();
        }

        return Optional.of(new StatementBoltResult(statementResult));
    }

    private static String getActualDbName(ResultSummary resultSummary) {
        DatabaseInfo dbInfo = resultSummary.database();
        return dbInfo.name() == null ? ABSENT_DB_NAME : dbInfo.name();
    }

    private void resetActualDbName() {
        actualDatabaseNameAsReportedByServer = null;
    }

    /**
     * Disconnect from Neo4j, clearing up any session resources, but don't give any output. Intended only to be used if connect fails.
     */
    void silentDisconnect() {
        try {
            closeSession(activeDatabaseNameAsSetByUser);
            if (driver != null) {
                driver.close();
            }
        } finally {
            session = null;
            driver = null;
            resetActualDbName();
        }
    }

    /**
     * Reset the current session. This rolls back any open transactions.
     */
    @SuppressWarnings("deprecation")
    public void reset() {
        if (isConnected()) {
            if (session instanceof org.neo4j.driver.internal.InternalSession internalSession) {
                internalSession.reset(); // Temporary private API to cancel queries
            }
            // Clear current state
            if (isTransactionOpen()) {
                // Bolt has already rolled back the transaction but it doesn't close it properly
                tx.rollback();
                tx = null;
            }
        }
    }

    @Override
    public void disconnect() {
        reset();
        silentDisconnect();
        protocolVersion = null;
    }

    private Driver getDriver(ConnectionConfig connectionConfig, AuthToken authToken) {
        Config.ConfigBuilder configBuilder = Config.builder()
                .withLogging(driverLogger())
                .withTelemetryDisabled(true)
                .withUserAgent(USER_AGENT);
        switch (connectionConfig.encryption()) {
            case TRUE -> configBuilder = configBuilder.withEncryption();
            case FALSE -> configBuilder = configBuilder.withoutEncryption();
            default -> {}
                // Do nothing
        }
        return driverProvider.apply(connectionConfig.uri(), authToken, configBuilder.build());
    }

    private boolean isSystemDb() {
        return activeDatabaseNameAsSetByUser.compareToIgnoreCase(SYSTEM_DB_NAME) == 0;
    }

    private static boolean procedureNotFound(ClientException e) {
        return "Neo.ClientError.Procedure.ProcedureNotFound".compareToIgnoreCase(e.code()) == 0;
    }

    private static Logging driverLogger() {
        var level = LogManager.getLogManager().getLogger("").getLevel();
        if (level == Level.OFF) {
            return DevNullLogging.DEV_NULL_LOGGING;
        }

        return Logging.javaUtilLogging(level);
    }

    private static ConnectionConfig clean(ConnectionConfig config) {
        if (config.impersonatedUser().filter(i -> i.equals(config.username())).isPresent()) {
            return config.withImpersonatedUser(null);
        }
        return config;
    }

    private static TransactionConfig txConfig(TransactionType type) {
        return TransactionConfig.builder()
                .withMetadata(Map.of("type", type.value(), "app", "cypher-shell_v" + Build.version()))
                .build();
    }

    public LicenseDetails licenseDetails() {
        return licenseDetails;
    }

    public org.neo4j.shell.cli.AccessMode accessMode() {
        return accessMode;
    }
}
