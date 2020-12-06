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
package org.neo4j.shell.state;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Query;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.Scheme;
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
import org.neo4j.shell.log.NullLogging;

import static org.neo4j.shell.util.Versions.isPasswordChangeRequiredException;
import static org.neo4j.shell.util.Versions.majorVersion;

/**
 * Handles interactions with the driver
 */
public class BoltStateHandler implements TransactionHandler, Connector, DatabaseManager
{
    private static final String USER_AGENT = "neo4j-cypher-shell/v" + Build.version();
    private final TriFunction<String, AuthToken, Config, Driver> driverProvider;
    private final boolean isInteractive;
    private final Map<String, Bookmark> bookmarks = new HashMap<>();
    protected Driver driver;
    Session session;
    private String version;
    private String activeDatabaseNameAsSetByUser;
    private String actualDatabaseNameAsReportedByServer;
    private Transaction tx;

    public BoltStateHandler( boolean isInteractive )
    {
        this( GraphDatabase::driver, isInteractive );
    }

    BoltStateHandler( TriFunction<String, AuthToken, Config, Driver> driverProvider,
                      boolean isInteractive )
    {
        this.driverProvider = driverProvider;
        activeDatabaseNameAsSetByUser = ABSENT_DB_NAME;
        this.isInteractive = isInteractive;
    }

    @Override
    public void setActiveDatabase( String databaseName ) throws CommandException
    {
        if ( isTransactionOpen() )
        {
            throw new CommandException( "There is an open transaction. You need to close it before you can switch database." );
        }
        String previousDatabaseName = activeDatabaseNameAsSetByUser;
        activeDatabaseNameAsSetByUser = databaseName;
        try
        {
            if ( isConnected() )
            {
                reconnect( databaseName, previousDatabaseName );
            }
        }
        catch ( ClientException e )
        {
            if ( isInteractive )
            {
                // We want to try to connect to the previous database
                activeDatabaseNameAsSetByUser = previousDatabaseName;
                try
                {
                    reconnect( previousDatabaseName, previousDatabaseName );
                }
                catch ( Exception e2 )
                {
                    e.addSuppressed( e2 );
                }
            }
            throw e;
        }
    }

    @Override
    public String getActiveDatabaseAsSetByUser()
    {
        return activeDatabaseNameAsSetByUser;
    }

    @Override
    public String getActualDatabaseAsReportedByServer()
    {
        return actualDatabaseNameAsReportedByServer;
    }

    @Override
    public void beginTransaction() throws CommandException
    {
        if ( !isConnected() )
        {
            throw new CommandException( "Not connected to Neo4j" );
        }
        if ( isTransactionOpen() )
        {
            throw new CommandException( "There is already an open transaction" );
        }
        tx = session.beginTransaction();
    }

    @Override
    public void commitTransaction() throws CommandException
    {
        if ( !isConnected() )
        {
            throw new CommandException( "Not connected to Neo4j" );
        }
        if ( !isTransactionOpen() )
        {
            throw new CommandException( "There is no open transaction to commit" );
        }
        tx.commit();
        tx.close();
        tx = null;
    }

    @Override
    public void rollbackTransaction() throws CommandException
    {
        if ( !isConnected() )
        {
            throw new CommandException( "Not connected to Neo4j" );
        }
        if ( !isTransactionOpen() )
        {
            throw new CommandException( "There is no open transaction to rollback" );
        }
        tx.rollback();
        tx.close();
        tx = null;
    }

    /**
     * Handle an exception while getting or consuming the result. If not in TX, return the given exception. If in a TX, terminate the TX and return a more
     * verbose error message.
     *
     * @param e the thrown exception.
     * @return a suitable exception to rethrow.
     */
    public Neo4jException handleException( Neo4jException e )
    {
        if ( isTransactionOpen() )
        {
            tx.close();
            tx = null;
            return new ErrorWhileInTransactionException(
                    "An error occurred while in an open transaction. The transaction will be rolled back and terminated. Error: " + e.getMessage(), e );
        }
        else
        {
            return e;
        }
    }

    @Override
    public boolean isTransactionOpen()
    {
        return tx != null;
    }

    @Override
    public boolean isConnected()
    {
        return session != null && session.isOpen();
    }

    @Override
    public void connect( @Nonnull ConnectionConfig connectionConfig, ThrowingAction<CommandException> command ) throws CommandException
    {
        if ( isConnected() )
        {
            throw new CommandException( "Already connected" );
        }
        final AuthToken authToken = AuthTokens.basic( connectionConfig.username(), connectionConfig.password() );
        try
        {
            String previousDatabaseName = activeDatabaseNameAsSetByUser;
            try
            {
                activeDatabaseNameAsSetByUser = connectionConfig.database();
                driver = getDriver( connectionConfig, authToken );
                reconnect( activeDatabaseNameAsSetByUser, previousDatabaseName, command );
            }
            catch ( org.neo4j.driver.exceptions.ServiceUnavailableException e )
            {
                String scheme = connectionConfig.scheme();
                String fallbackScheme;
                switch ( scheme )
                {
                case Scheme.NEO4J_URI_SCHEME:
                    fallbackScheme = Scheme.BOLT_URI_SCHEME;
                    break;
                case Scheme.NEO4J_LOW_TRUST_URI_SCHEME:
                    fallbackScheme = Scheme.BOLT_LOW_TRUST_URI_SCHEME;
                    break;
                case Scheme.NEO4J_HIGH_TRUST_URI_SCHEME:
                    fallbackScheme = Scheme.BOLT_HIGH_TRUST_URI_SCHEME;
                    break;
                default:
                    throw e;
                }
                connectionConfig = new ConnectionConfig(
                        fallbackScheme,
                        connectionConfig.host(),
                        connectionConfig.port(),
                        connectionConfig.username(),
                        connectionConfig.password(),
                        connectionConfig.encryption(),
                        connectionConfig.database() );
                driver = getDriver( connectionConfig, authToken );
                reconnect( activeDatabaseNameAsSetByUser, previousDatabaseName, command );
            }
        }
        catch ( Throwable t )
        {
            try
            {
                silentDisconnect();
            }
            catch ( Exception e )
            {
                t.addSuppressed( e );
            }
            throw t;
        }
    }

    private void reconnect( String databaseToConnectTo, String previousDatabase ) throws CommandException
    {
        reconnect( databaseToConnectTo, previousDatabase, null );
    }

    private void reconnect( String databaseToConnectTo,
                            String previousDatabase,
                            ThrowingAction<CommandException> command ) throws CommandException
    {
        SessionConfig.Builder builder = SessionConfig.builder();
        builder.withDefaultAccessMode( AccessMode.WRITE );
        if ( !ABSENT_DB_NAME.equals( databaseToConnectTo ) )
        {
            builder.withDatabase( databaseToConnectTo );
        }
        closeSession( previousDatabase );
        final Bookmark bookmarkForDBToConnectTo = bookmarks.get( databaseToConnectTo );
        if ( bookmarkForDBToConnectTo != null )
        {
            builder.withBookmarks( bookmarkForDBToConnectTo );
        }

        session = driver.session( builder.build() );

        resetActualDbName(); // Set this to null first in case run throws an exception
        connect( command );
    }

    /**
     * Closes the session, if there is any. Saves a bookmark for the database currently connected to.
     *
     * @param databaseName the name of the database currently connected to
     */
    private void closeSession( String databaseName )
    {
        if ( session != null )
        {
            // Save the last bookmark and close the session
            final Bookmark bookmarkForPreviousDB = session.lastBookmark();
            session.close();
            bookmarks.put( databaseName, bookmarkForPreviousDB );
        }
    }

    private void connect( ThrowingAction<CommandException> command ) throws CommandException
    {
        ThrowingAction<CommandException> toCall = command == null ? getPing() : () ->
        {
            try
            {
                command.apply();
            }
            catch ( Neo4jException e )
            {
                //If we need to update password we need to call the apply
                //to set the server version and such.
                if ( isPasswordChangeRequiredException( e ) )
                {
                    getPing().apply();
                }
                throw e;
            }
        };

        //execute
        toCall.apply();
    }

    private ThrowingAction<CommandException> getPing()
    {

        return () ->
        {
            ResultSummary summary = null;
            Result run = session.run( "CALL db.ping()" );
            try
            {
                summary = run.consume();
            }
            catch ( ClientException e )
            {
                //In older versions there is no db.ping procedure, use legacy method.
                if ( procedureNotFound( e ) )
                {
                    run = session.run( isSystemDb() ? "CALL db.indexes()" : "RETURN 1" );
                }
                else
                {
                    throw e;
                }
            }
            finally
            {
                // Since run.consume() can throw the first time we have to go through this extra hoop to get the summary
                if ( summary == null )
                {
                    summary = run.consume();
                }
                BoltStateHandler.this.version = summary.server().version();
                updateActualDbName( summary );
            }
        };
    }

    @Nonnull
    @Override
    public String getServerVersion()
    {
        if ( isConnected() )
        {
            if ( version == null )
            {
                // On versions before 3.1.0-M09
                version = "";
            }
            if ( version.startsWith( "Neo4j/" ) )
            {
                // Want to return '3.1.0' and not 'Neo4j/3.1.0'
                version = version.substring( 6 );
            }
            return version;
        }
        return "";
    }

    @Nonnull
    public Optional<BoltResult> runCypher( @Nonnull String cypher,
                                           @Nonnull Map<String, Object> queryParams ) throws CommandException
    {
        if ( !isConnected() )
        {
            throw new CommandException( "Not connected to Neo4j" );
        }
        if ( isTransactionOpen() )
        {
            // If this fails, don't try any funny business - just let it die
            return getBoltResult( cypher, queryParams );
        }
        else
        {
            try
            {
                // Note that PERIODIC COMMIT can't execute in a transaction, so if the user has not typed BEGIN, then
                // the statement should NOT be executed in a transaction.
                return getBoltResult( cypher, queryParams );
            }
            catch ( SessionExpiredException e )
            {
                // Server is no longer accepting writes, reconnect and try again.
                // If it still fails, leave it up to the user
                reconnect( activeDatabaseNameAsSetByUser, activeDatabaseNameAsSetByUser );
                return getBoltResult( cypher, queryParams );
            }
        }
    }

    public void updateActualDbName( @Nonnull ResultSummary resultSummary )
    {
        actualDatabaseNameAsReportedByServer = getActualDbName( resultSummary );
    }

    public void changePassword( @Nonnull ConnectionConfig connectionConfig )
    {
        if ( !connectionConfig.passwordChangeRequired() )
        {
            return;
        }

        if ( isConnected() )
        {
            silentDisconnect();
        }

        final AuthToken authToken = AuthTokens.basic( connectionConfig.username(), connectionConfig.password() );

        try
        {
            driver = getDriver( connectionConfig, authToken );

            activeDatabaseNameAsSetByUser = SYSTEM_DB_NAME;
            // Supply empty command, so that we do not run ping.
            reconnect( SYSTEM_DB_NAME, SYSTEM_DB_NAME, () ->
            {
            } );

            String command;
            Value parameters;
            if ( majorVersion( getServerVersion() ) >= 4 )
            {
                command = "ALTER CURRENT USER SET PASSWORD FROM $o TO $n";
                parameters = Values.parameters( "o", connectionConfig.password(), "n", connectionConfig.newPassword() );
            }
            else
            {
                command = "CALL dbms.security.changePassword($n)";
                parameters = Values.parameters( "n", connectionConfig.newPassword() );
            }

            Result run = session.run( command, parameters );
            run.consume();

            // If successful, use the new password when reconnecting
            connectionConfig.setPassword( connectionConfig.newPassword() );
            connectionConfig.setNewPassword( null );

            silentDisconnect();
        }
        catch ( Throwable t )
        {
            try
            {
                silentDisconnect();
            }
            catch ( Exception e )
            {
                t.addSuppressed( e );
            }
            if ( t instanceof RuntimeException )
            {
                throw (RuntimeException) t;
            }
            // The only checked exception is CommandException and we know that
            // we cannot get that since we supply an empty command.
            throw new RuntimeException( t );
        }
    }

    /**
     * @throws SessionExpiredException when server no longer serves writes anymore
     */
    @Nonnull
    private Optional<BoltResult> getBoltResult( @Nonnull String cypher, @Nonnull Map<String, Object> queryParams ) throws SessionExpiredException
    {
        Result statementResult;

        if ( isTransactionOpen() )
        {
            statementResult = tx.run( new Query( cypher, queryParams ) );
        }
        else
        {
            statementResult = session.run( new Query( cypher, queryParams ) );
        }

        if ( statementResult == null )
        {
            return Optional.empty();
        }

        return Optional.of( new StatementBoltResult( statementResult ) );
    }

    private String getActualDbName( @Nonnull ResultSummary resultSummary )
    {
        DatabaseInfo dbInfo = resultSummary.database();
        return dbInfo.name() == null ? ABSENT_DB_NAME : dbInfo.name();
    }

    private void resetActualDbName()
    {
        actualDatabaseNameAsReportedByServer = null;
    }

    /**
     * Disconnect from Neo4j, clearing up any session resources, but don't give any output. Intended only to be used if connect fails.
     */
    void silentDisconnect()
    {
        try
        {
            closeSession( activeDatabaseNameAsSetByUser );
            if ( driver != null )
            {
                driver.close();
            }
        }
        finally
        {
            session = null;
            driver = null;
            resetActualDbName();
        }
    }

    /**
     * Reset the current session. This rolls back any open transactions.
     */
    public void reset()
    {
        if ( isConnected() )
        {
            session.reset();

            // Clear current state
            if ( isTransactionOpen() )
            {
                // Bolt has already rolled back the transaction but it doesn't close it properly
                tx.rollback();
                tx.close();
                tx = null;
            }
        }
    }

    /**
     * Used for testing purposes
     */
    public void disconnect()
    {
        reset();
        silentDisconnect();
        version = null;
    }

    private Driver getDriver( @Nonnull ConnectionConfig connectionConfig, @Nullable AuthToken authToken )
    {
        Config.ConfigBuilder configBuilder = Config.builder()
                                                   .withLogging( NullLogging.NULL_LOGGING )
                                                   .withUserAgent( USER_AGENT );
        switch ( connectionConfig.encryption() )
        {
        case TRUE:
            configBuilder = configBuilder.withEncryption();
            break;
        case FALSE:
            configBuilder = configBuilder.withoutEncryption();
            break;
        default:
            // Do nothing
        }
        return driverProvider.apply( connectionConfig.driverUrl(), authToken, configBuilder.build() );
    }

    private List<BoltResult> executeWithRetry( List<Query> transactionStatements, BiFunction<Query, Transaction, BoltResult> biFunction )
    {
        return session.writeTransaction( tx ->
                                                 transactionStatements.stream()
                                                                      .map( transactionStatement -> biFunction.apply( transactionStatement, tx ) )
                                                                      .collect( Collectors.toList() ) );
    }

    private boolean isSystemDb()
    {
        return activeDatabaseNameAsSetByUser.compareToIgnoreCase( SYSTEM_DB_NAME ) == 0;
    }

    private boolean procedureNotFound( ClientException e )
    {
        return e.code().compareToIgnoreCase( "Neo.ClientError.Procedure.ProcedureNotFound" ) == 0;
    }
}
