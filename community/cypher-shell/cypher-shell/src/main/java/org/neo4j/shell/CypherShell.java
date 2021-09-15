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

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.driver.exceptions.DiscoveryException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.shell.commands.Command;
import org.neo4j.shell.commands.CommandExecutable;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.exception.ThrowingAction;
import org.neo4j.shell.prettyprint.LinePrinter;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.state.BoltResult;
import org.neo4j.shell.state.BoltStateHandler;

/**
 * A possibly interactive shell for evaluating cypher statements.
 */
public class CypherShell implements StatementExecuter, Connector, TransactionHandler, DatabaseManager
{
    // Final space to catch newline
    private static final Pattern cmdNamePattern = Pattern.compile( "^\\s*(?<name>[^\\s]+)\\b(?<args>.*)\\s*$" );
    private static final Pattern emptyStatementPattern = Pattern.compile( "^\\s*;$" );
    private final ParameterMap parameterMap;
    private final LinePrinter linePrinter;
    private final BoltStateHandler boltStateHandler;
    private final PrettyPrinter prettyPrinter;
    private CommandHelper commandHelper;
    private String lastNeo4jErrorCode;

    public CypherShell( LinePrinter linePrinter,
                        PrettyConfig prettyConfig,
                        boolean isInteractive,
                        ParameterMap parameterMap )
    {
        this( linePrinter, new BoltStateHandler( isInteractive ), new PrettyPrinter( prettyConfig ), parameterMap );
    }

    protected CypherShell( LinePrinter linePrinter,
                           BoltStateHandler boltStateHandler,
                           PrettyPrinter prettyPrinter,
                           ParameterMap parameterMap )
    {
        this.linePrinter = linePrinter;
        this.boltStateHandler = boltStateHandler;
        this.prettyPrinter = prettyPrinter;
        this.parameterMap = parameterMap;
        addRuntimeHookToResetShell();
    }

    /**
     * @param text to trim
     * @return text without trailing semicolons
     */
    protected static String stripTrailingSemicolons( String text )
    {
        int end = text.length();
        while ( end > 0 && text.substring( 0, end ).endsWith( ";" ) )
        {
            end -= 1;
        }
        return text.substring( 0, end );
    }

    @Override
    public void execute( final String cmdString ) throws ExitException, CommandException
    {
        if ( isEmptyStatement( cmdString ) )
        {
            return;
        }

        // See if it's a shell command
        final Optional<CommandExecutable> cmd = getCommandExecutable( cmdString );
        if ( cmd.isPresent() )
        {
            executeCmd( cmd.get() );
            return;
        }

        // Else it will be parsed as Cypher, but for that we need to be connected
        if ( !isConnected() )
        {
            throw new CommandException( "Not connected to Neo4j" );
        }

        executeCypher( cmdString );
    }

    private static boolean isEmptyStatement( final String statement )
    {
        return emptyStatementPattern.matcher( statement ).matches();
    }

    @Override
    public String lastNeo4jErrorCode()
    {
        return lastNeo4jErrorCode;
    }

    /**
     * Executes a piece of text as if it were Cypher. By default, all of the cypher is executed in single statement (with an implicit transaction).
     *
     * @param cypher non-empty cypher text to executeLine
     */
    private void executeCypher( final String cypher ) throws CommandException
    {
        try
        {
            final Optional<BoltResult> result = boltStateHandler.runCypher( cypher, parameterMap.allParameterValues() );
            result.ifPresent( boltResult ->
                              {
                                  prettyPrinter.format( boltResult, linePrinter );
                                  boltStateHandler.updateActualDbName( boltResult.getSummary() );
                              } );
            lastNeo4jErrorCode = null;
        }
        catch ( Neo4jException e )
        {
            lastNeo4jErrorCode = getErrorCode( e );
            throw boltStateHandler.handleException( e );
        }
    }

    @Override
    public boolean isConnected()
    {
        return boltStateHandler.isConnected();
    }

    protected Optional<CommandExecutable> getCommandExecutable( final String line )
    {
        Matcher m = cmdNamePattern.matcher( line );
        if ( commandHelper == null || !m.matches() )
        {
            return Optional.empty();
        }

        String name = m.group( "name" );
        String args = m.group( "args" );

        Command cmd = commandHelper.getCommand( name );

        if ( cmd == null )
        {
            return Optional.empty();
        }

        return Optional.of( () -> cmd.execute( stripTrailingSemicolons( args ) ) );
    }

    protected static void executeCmd( final CommandExecutable cmdExe ) throws ExitException, CommandException
    {
        cmdExe.execute();
    }

    /**
     * Open a session to Neo4j
     *
     * @param connectionConfig
     * @param command
     * @return connection configuration used to connect (can be different from the supplied)
     */
    @Override
    public ConnectionConfig connect( ConnectionConfig connectionConfig,
                                     ThrowingAction<CommandException> command ) throws CommandException
    {
        return boltStateHandler.connect( connectionConfig, command );
    }

    @Override
    public String getServerVersion()
    {
        return boltStateHandler.getServerVersion();
    }

    @Override
    public String getProtocolVersion()
    {
        return boltStateHandler.getProtocolVersion();
    }

    @Override
    public void beginTransaction() throws CommandException
    {
        boltStateHandler.beginTransaction();
    }

    @Override
    public void commitTransaction() throws CommandException
    {
        try
        {
            boltStateHandler.commitTransaction();
            lastNeo4jErrorCode = null;
        }
        catch ( Neo4jException e )
        {
            lastNeo4jErrorCode = getErrorCode( e );
            throw e;
        }
    }

    @Override
    public void rollbackTransaction() throws CommandException
    {
        boltStateHandler.rollbackTransaction();
    }

    @Override
    public boolean isTransactionOpen()
    {
        return boltStateHandler.isTransactionOpen();
    }

    public void setCommandHelper( CommandHelper commandHelper )
    {
        this.commandHelper = commandHelper;
    }

    @Override
    public void reset()
    {
        boltStateHandler.reset();
    }

    protected void addRuntimeHookToResetShell()
    {
        Runtime.getRuntime().addShutdownHook( new Thread( this::reset ) );
    }

    @Override
    public void setActiveDatabase( String databaseName ) throws CommandException
    {
        try
        {
            boltStateHandler.setActiveDatabase( databaseName );
            lastNeo4jErrorCode = null;
        }
        catch ( Neo4jException e )
        {
            lastNeo4jErrorCode = getErrorCode( e );
            throw e;
        }
    }

    @Override
    public String getActiveDatabaseAsSetByUser()
    {
        return boltStateHandler.getActiveDatabaseAsSetByUser();
    }

    @Override
    public String getActualDatabaseAsReportedByServer()
    {
        return boltStateHandler.getActualDatabaseAsReportedByServer();
    }

    /**
     * @return the parameter map.
     */
    public ParameterMap getParameterMap()
    {
        return parameterMap;
    }

    public void changePassword( ConnectionConfig connectionConfig, String newPassword )
    {
        boltStateHandler.changePassword( connectionConfig, newPassword );
    }

    /**
     * Used for testing purposes
     */
    public void disconnect()
    {
        boltStateHandler.disconnect();
    }

    private static String getErrorCode( Neo4jException e )
    {
        Neo4jException statusException = e;

        // If we encountered a later suppressed Neo4jException we use that as the basis for the status instead
        Throwable[] suppressed = e.getSuppressed();
        for ( Throwable s : suppressed )
        {
            if ( s instanceof Neo4jException )
            {
                statusException = (Neo4jException) s;
                break;
            }
        }

        if ( statusException instanceof ServiceUnavailableException || statusException instanceof DiscoveryException )
        {
            // Treat this the same way as a DatabaseUnavailable error for now.
            return DATABASE_UNAVAILABLE_ERROR_CODE;
        }
        return statusException.code();
    }
}
