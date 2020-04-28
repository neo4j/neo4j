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
package org.neo4j.test.rule;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public abstract class DbmsRule extends ExternalResource implements GraphDatabaseAPI
{
    private DatabaseManagementServiceBuilder databaseBuilder;
    private GraphDatabaseAPI database;
    private DatabaseLayout databaseLayout;
    private boolean startEagerly = true;
    private final Map<Setting<?>, Object> globalConfig = new HashMap<>();
    private final Monitors monitors = new Monitors();
    private DatabaseManagementService managementService;

    /**
     * Means the database will be started on first {@link #getGraphDatabaseAPI()}}
     * or {@link #ensureStarted()} call.
     */
    public DbmsRule startLazily()
    {
        startEagerly = false;
        return this;
    }

    public <T> T executeAndCommit( Function<Transaction, T> function )
    {
        return transaction( function, true );
    }

    public <T> T executeAndRollback( Function<Transaction, T> function )
    {
        return transaction( function, false );
    }

    public <FROM, TO> Function<FROM,TO> tx( Function<FROM,TO> function )
    {
        return from ->
        {
            Function<Transaction,TO> inner = graphDb -> function.apply( from );
            return executeAndCommit( inner );
        };
    }

    private <T> T transaction( Function<Transaction, T> function, boolean commit )
    {
        return tx( getGraphDatabaseAPI(), commit, function );
    }

    /**
     * Perform a transaction, with the option to automatically retry on failure.
     *
     * @param db {@link GraphDatabaseService} to apply the transaction on.
     * @param transaction {@link Consumer} containing the transaction logic.
     */
    public static void tx( GraphDatabaseService db, Consumer<Transaction> transaction )
    {
        Function<Transaction,Void> voidFunction = tx ->
        {
            transaction.accept( tx );
            return null;
        };
        tx( db, true, voidFunction );
    }

    /**
     * Perform a transaction, with the option to automatically retry on failure.
     * Also returning a result from the supplied transaction function.
     *
     * @param db {@link GraphDatabaseService} to apply the transaction on.
     * @param commit whether or not to call {@link Transaction#commit()} in the end.
     * @param transaction {@link Function} containing the transaction logic and returning a result.
     * @return result from transaction {@link Function}.
     */
    public static <T> T tx( GraphDatabaseService db, boolean commit, Function<Transaction,T> transaction )
    {
        try ( Transaction tx = db.beginTx() )
        {
            T result = transaction.apply( tx );
            if ( commit )
            {
                tx.commit();
            }
            return result;
        }
    }

    @Override
    public void executeTransactionally( String query ) throws QueryExecutionException
    {
        getGraphDatabaseAPI().executeTransactionally( query );
    }

    @Override
    public void executeTransactionally( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        getGraphDatabaseAPI().executeTransactionally( query, parameters );
    }

    @Override
    public <T> T executeTransactionally( String query, Map<String,Object> parameters, ResultTransformer<T> resultTransformer ) throws QueryExecutionException
    {
        return getGraphDatabaseAPI().executeTransactionally( query, parameters, resultTransformer );
    }

    @Override
    public <T> T executeTransactionally( String query, Map<String,Object> parameters, ResultTransformer<T> resultTransformer, Duration timeout )
            throws QueryExecutionException
    {
        return getGraphDatabaseAPI().executeTransactionally( query, parameters, resultTransformer, timeout );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext )
    {
        return getGraphDatabaseAPI().beginTransaction( type, loginContext );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo )
    {
        return getGraphDatabaseAPI().beginTransaction( type, loginContext, connectionInfo );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo, long timeout,
            TimeUnit unit )
    {
        return getGraphDatabaseAPI().beginTransaction( type, loginContext, connectionInfo, timeout, unit );
    }

    @Override
    public Transaction beginTx()
    {
        return getGraphDatabaseAPI().beginTx();
    }

    @Override
    public Transaction beginTx( long timeout, TimeUnit timeUnit )
    {
        return getGraphDatabaseAPI().beginTx( timeout, timeUnit );
    }

    @Override
    protected void before()
    {
        create();
        if ( startEagerly )
        {
            ensureStarted();
        }
    }

    @Override
    protected void after( boolean success )
    {
        shutdown( success );
    }

    private void create()
    {
        createResources();
        try
        {
            databaseBuilder = newFactory();
            databaseBuilder.setMonitors( monitors );
            configure( databaseBuilder );
            databaseBuilder.setConfig( globalConfig );
        }
        catch ( RuntimeException e )
        {
            deleteResources();
            throw e;
        }
    }

    /**
     * @return the high level monitor in the database.
     */
    public Monitors getMonitors()
    {
        return monitors;
    }

    protected void deleteResources()
    {
    }

    protected void createResources()
    {
    }

    protected abstract DatabaseManagementServiceBuilder newFactory();

    protected void configure( DatabaseManagementServiceBuilder databaseFactory )
    {
        // Override to configure the database factory
    }

    /**
     * {@link DbmsRule} now implements {@link GraphDatabaseAPI} directly, so no need. Also for ensuring
     * a lazily started database is created, use {@link #ensureStarted()} instead.
     */
    public GraphDatabaseAPI getGraphDatabaseAPI()
    {
        ensureStarted();
        return database;
    }

    public DatabaseManagementService getManagementService()
    {
        return managementService;
    }

    public synchronized void ensureStarted()
    {
        if ( database == null )
        {
            managementService = databaseBuilder.build();
            database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
            databaseLayout = database.databaseLayout();
        }
    }

    /**
     * Adds or replaces a setting for the database managed by this database rule.
     * <p>
     * If this method is called when constructing the rule, the setting is considered a global setting applied to all tests.
     * <p>
     * If this method is called inside a specific test, i.e. after {@link #before()}, but before started (a call to {@link #startLazily()} have been made),
     * then this setting will be considered a test-specific setting, adding to or overriding the global settings for this test only.
     * Test-specific settings will be remembered throughout a test, even between restarts.
     * <p>
     * If this method is called when a database is already started an {@link IllegalStateException} will be thrown since the setting
     * will have no effect, instead letting the developer notice that and change the test code.
     */
    public <T> DbmsRule withSetting( Setting<T> key, T value )
    {
        if ( database != null )
        {
            // Database already started
            throw new IllegalStateException( "Wanted to set " + key + '=' + value + ", but database has already been started" );
        }
        if ( databaseBuilder != null )
        {
            // Test already started, but db not yet started
            databaseBuilder.setConfig( key, value );
        }
        else
        {
            // Test haven't started, we're still in phase of constructing this rule
            globalConfig.put( key, value );
        }
        return this;
    }

    /**
     * Applies all settings in the settings map.
     *
     * @see #withSetting(Setting, Object)
     */
    public DbmsRule withSettings( Map<Setting<?>,Object> configuration )
    {
        if ( database != null )
        {
            // Database already started
            throw new IllegalStateException( "Wanted to set " + configuration + ", but database has already been started" );
        }
        if ( databaseBuilder != null )
        {
            // Test already started, but db not yet started
            databaseBuilder.setConfig( configuration );
        }
        else
        {
            // Test haven't started, we're still in phase of constructing this rule
            globalConfig.putAll( configuration );
        }
        return this;
    }

    public interface RestartAction
    {
        void run( FileSystemAbstraction fs, DatabaseLayout databaseLayout ) throws IOException;

        RestartAction EMPTY = ( fs, storeDirectory ) ->
        {
            // duh
        };
    }

    public GraphDatabaseAPI restartDatabase() throws IOException
    {
        return restartDatabase( RestartAction.EMPTY, Map.of() );
    }

    public GraphDatabaseAPI restartDatabase( Map<Setting<?>,Object> configChanges ) throws IOException
    {
        return restartDatabase( RestartAction.EMPTY, configChanges );
    }

    public GraphDatabaseAPI restartDatabase( RestartAction action ) throws IOException
    {
        return restartDatabase( action, Map.of() );
    }

    public GraphDatabaseAPI restartDatabase( RestartAction action, Map<Setting<?>,Object> configChanges ) throws IOException
    {
        FileSystemAbstraction fs = resolveDependency( FileSystemAbstraction.class );
        managementService.shutdown();
        action.run( fs, databaseLayout );
        database = null;
        // This DatabaseBuilder has already been configured with the global settings as well as any test-specific settings,
        // so just apply these additional settings.
        databaseBuilder.setConfig( configChanges );
        return getGraphDatabaseAPI();
    }

    public void shutdown()
    {
        shutdown( true );
    }

    private void shutdown( boolean deleteResources )
    {
        try
        {
            if ( managementService != null )
            {
                managementService.shutdown();
            }
        }
        finally
        {
            if ( deleteResources )
            {
                deleteResources();
            }
            managementService = null;
            database = null;
        }
    }

    public void shutdownAndKeepStore()
    {
        shutdown( false );
    }

    public <T> T resolveDependency( Class<T> type )
    {
        return getGraphDatabaseAPI().getDependencyResolver().resolveDependency( type );
    }

    @Override
    public NamedDatabaseId databaseId()
    {
        return database.databaseId();
    }

    @Override
    public DatabaseInfo databaseInfo()
    {
        return database.databaseInfo();
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return database.getDependencyResolver();
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return database.databaseLayout();
    }

    @Override
    public boolean isAvailable( long timeout )
    {
        return database.isAvailable( timeout );
    }

    @Override
    public String databaseName()
    {
        return database.databaseName();
    }
}
