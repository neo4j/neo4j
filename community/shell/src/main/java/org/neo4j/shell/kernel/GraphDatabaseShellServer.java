/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.shell.kernel;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.shell.Output;
import org.neo4j.shell.Response;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.Variables;
import org.neo4j.shell.Welcome;
import org.neo4j.shell.impl.AbstractAppServer;
import org.neo4j.shell.impl.BashVariableInterpreter.Replacer;
import org.neo4j.shell.kernel.apps.TransactionProvidingApp;

import static org.neo4j.shell.Variables.PROMPT_KEY;

/**
 * A {@link ShellServer} which contains common methods to use with a
 * graph database service.
 */
public class GraphDatabaseShellServer extends AbstractAppServer
{
    private final GraphDatabaseAPI graphDb;
    private boolean graphDbCreatedHere;
    protected final Map<Serializable, TopLevelTransaction> clients = new ConcurrentHashMap<>();

    /**
     * @param path the path to the directory where the database should be created
     * @param readOnly make the instance read-only
     * @param configFileOrNull path to a configuration file or <code>null</code>
     * @throws RemoteException if an RMI error occurs.
     */
    public GraphDatabaseShellServer( String path, boolean readOnly, String configFileOrNull )
            throws RemoteException
    {
        this( instantiateGraphDb( new GraphDatabaseFactory(), path, readOnly, configFileOrNull ), readOnly );
        this.graphDbCreatedHere = true;
    }

    public GraphDatabaseShellServer( GraphDatabaseFactory factory, String path, boolean readOnly, String configFileOrNull )
            throws RemoteException
    {
        this( instantiateGraphDb(  factory, path, readOnly, configFileOrNull ), readOnly );
        this.graphDbCreatedHere = true;
    }

    public GraphDatabaseShellServer( GraphDatabaseAPI graphDb )
            throws RemoteException
    {
        this( graphDb, false );
    }

    public GraphDatabaseShellServer( GraphDatabaseAPI graphDb, boolean readOnly )
            throws RemoteException
    {
        super();
        this.graphDb = readOnly ? new ReadOnlyGraphDatabaseProxy( graphDb ) : graphDb;
        this.bashInterpreter.addReplacer( "W", new WorkingDirReplacer() );
        this.graphDbCreatedHere = false;
    }

    /*
    * Since we don't know which thread we might happen to run on, we can't trust transactions
    * to be stored in threads. Instead, we keep them around here, and suspend/resume in
    * transactions before apps get to run.
    */
    @Override
    public Response interpretLine( Serializable clientId, String line, Output out ) throws ShellException
    {
        bindTransaction( clientId );
        try
        {
            return super.interpretLine( clientId, line, out );
        }
        finally
        {
            unbindAndRegisterTransaction( clientId );
        }
    }

    @Override
    public void terminate( Serializable clientId )
    {
        TopLevelTransaction tx = clients.get( clientId );
        if ( tx != null )
        {
            tx.terminate();
        }
    }

    public void registerTopLevelTransactionInProgress( Serializable clientId ) throws ShellException
    {
        if ( !clients.containsKey( clientId ) )
        {
            ThreadToStatementContextBridge threadToStatementContextBridge = getThreadToStatementContextBridge();
            TopLevelTransaction tx = threadToStatementContextBridge.getTopLevelTransactionBoundToThisThread( false );
            clients.put( clientId, tx );
        }
    }

    public Statement getStatement()
    {
        return getThreadToStatementContextBridge().get();
    }

    private ThreadToStatementContextBridge getThreadToStatementContextBridge()
    {
        return getDb().getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }

    public void unbindAndRegisterTransaction( Serializable clientId ) throws ShellException
    {
        try
        {
            ThreadToStatementContextBridge threadToStatementContextBridge = getThreadToStatementContextBridge();
            TopLevelTransaction tx = threadToStatementContextBridge.getTopLevelTransactionBoundToThisThread( false );
            threadToStatementContextBridge.unbindTransactionFromCurrentThread();
            if ( tx == null )
            {
                clients.remove( clientId );
            }
            else
            {
                clients.put( clientId, tx );
            }
        }
        catch ( Exception e )
        {
            throw wrapException( e );
        }
    }

    public void bindTransaction( Serializable clientId ) throws ShellException
    {
        TopLevelTransaction tx = clients.get( clientId );
        if ( tx != null )
        {
            try
            {
                ThreadToStatementContextBridge threadToStatementContextBridge = getThreadToStatementContextBridge();
                threadToStatementContextBridge.bindTransactionToCurrentThread( tx );
            }
            catch ( Exception e )
            {
                throw wrapException( e );
            }
        }
    }

    @Override
    protected void initialPopulateSession( Session session ) throws ShellException
    {
        session.set( Variables.TITLE_KEYS_KEY, ".*name.*,.*title.*" );
        session.set( Variables.TITLE_MAX_LENGTH, "40" );
    }

    @Override
    protected String getPrompt( Session session ) throws ShellException
    {
        try ( org.neo4j.graphdb.Transaction transaction = this.getDb().beginTx() )
        {
            Object rawCustomPrompt = session.get( PROMPT_KEY );
            String customPrompt = rawCustomPrompt != null ? rawCustomPrompt.toString() : getDefaultPrompt();
            String output = bashInterpreter.interpret( customPrompt, this, session );
            transaction.success();
            return output;
        }
    }

    private static GraphDatabaseAPI instantiateGraphDb( GraphDatabaseFactory factory, String path, boolean readOnly,
            String configFileOrNull )
    {
        GraphDatabaseBuilder builder = factory.
                newEmbeddedDatabaseBuilder( path ).
                setConfig( GraphDatabaseSettings.read_only, Boolean.toString( readOnly ) );
        if ( configFileOrNull != null )
        {
            builder.loadPropertiesFromFile( configFileOrNull );
        }
        return (GraphDatabaseAPI) builder.newGraphDatabase();
    }

    @Override
    protected String getDefaultPrompt()
    {
        String name = "neo4j-sh";
        if ( this.graphDb instanceof ReadOnlyGraphDatabaseProxy )
        {
            name += "[readonly]";
        }
        name += " \\W$ ";
        return name;
    }

    @Override
    protected String getWelcomeMessage()
    {
        return "Welcome to the Neo4j Shell! Enter 'help' for a list of commands";
    }

    /**
     * @return the {@link GraphDatabaseAPI} instance given in the
     * constructor.
     */
    public GraphDatabaseAPI getDb()
    {
        return this.graphDb;
    }

    /**
     * A {@link Replacer} for the variable "w"/"W" which returns the current
     * working directory (Bash), i.e. the current node.
     */
    public static class WorkingDirReplacer implements Replacer
    {
        @Override
        public String getReplacement( ShellServer server, Session session )
                throws ShellException
        {
            try
            {
                return TransactionProvidingApp.getDisplayName(
                        (GraphDatabaseShellServer) server, session,
                        TransactionProvidingApp.getCurrent(
                                (GraphDatabaseShellServer) server, session ),
                        false );
            }
            catch ( ShellException e )
            {
                return TransactionProvidingApp.getDisplayNameForNonExistent();
            }
        }
    }

    @Override
    public void shutdown() throws RemoteException
    {
        if ( graphDbCreatedHere )
        {
            this.graphDb.shutdown();
        }
        super.shutdown();
    }

    @Override
    public Welcome welcome( Map<String, Serializable> initialSession ) throws RemoteException, ShellException
    {
        try ( org.neo4j.graphdb.Transaction transaction = graphDb.beginTx() )
        {
            Welcome welcome = super.welcome( initialSession );
            transaction.success();
            return welcome;
        }
    }
}
