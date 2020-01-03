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
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.StringSearchMode;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public abstract class DatabaseRule extends ExternalResource implements GraphDatabaseAPI
{
    private GraphDatabaseBuilder databaseBuilder;
    private GraphDatabaseAPI database;
    private DatabaseLayout databaseLayout;
    private Supplier<Statement> statementSupplier;
    private boolean startEagerly = true;
    private final Map<Setting<?>, String> globalConfig = new HashMap<>();
    private final Monitors monitors = new Monitors();

    /**
     * Means the database will be started on first {@link #getGraphDatabaseAPI()}}
     * or {@link #ensureStarted()} call.
     */
    public DatabaseRule startLazily()
    {
        startEagerly = false;
        return this;
    }

    public <T> T when( Function<GraphDatabaseService, T> function )
    {
        return function.apply( getGraphDatabaseAPI() );
    }

    public void executeAndCommit( Consumer<? super GraphDatabaseService> consumer )
    {
        transaction( (Function<? super GraphDatabaseService,Void>) t ->
        {
            consumer.accept( t );
            return null;
        }, true );
    }

    public <T> T executeAndCommit( Function<? super GraphDatabaseService, T> function )
    {
        return transaction( function, true );
    }

    public <T> T executeAndRollback( Function<? super GraphDatabaseService, T> function )
    {
        return transaction( function, false );
    }

    public <FROM, TO> Function<FROM,TO> tx( Function<FROM,TO> function )
    {
        return from ->
        {
            Function<GraphDatabaseService,TO> inner = graphDb -> function.apply( from );
            return executeAndCommit( inner );
        };
    }

    private <T> T transaction( Function<? super GraphDatabaseService, T> function, boolean commit )
    {
        return tx( getGraphDatabaseAPI(), commit, RetryHandler.NO_RETRY, function );
    }

    /**
     * Perform a transaction, with the option to automatically retry on failure.
     *
     * @param db {@link GraphDatabaseService} to apply the transaction on.
     * @param retry {@link RetryHandler} deciding what type of failures to retry on.
     * @param transaction {@link Consumer} containing the transaction logic.
     */
    public static void tx( GraphDatabaseService db, RetryHandler retry,
            Consumer<? super GraphDatabaseService> transaction )
    {
        Function<? super GraphDatabaseService,Void> voidFunction = _db ->
        {
            transaction.accept( _db );
            return null;
        };
        tx( db, true, retry, voidFunction );
    }

    /**
     * Perform a transaction, with the option to automatically retry on failure.
     * Also returning a result from the supplied transaction function.
     *
     * @param db {@link GraphDatabaseService} to apply the transaction on.
     * @param commit whether or not to call {@link Transaction#success()} in the end.
     * @param retry {@link RetryHandler} deciding what type of failures to retry on.
     * @param transaction {@link Function} containing the transaction logic and returning a result.
     * @return result from transaction {@link Function}.
     */
    public static <T> T tx( GraphDatabaseService db, boolean commit,
            RetryHandler retry, Function<? super GraphDatabaseService, T> transaction )
    {
        while ( true )
        {
            try ( Transaction tx = db.beginTx() )
            {
                T result = transaction.apply( db );
                if ( commit )
                {
                    tx.success();
                }
                return result;
            }
            catch ( Throwable t )
            {
                if ( !retry.retryOn( t ) )
                {
                    throw t;
                }
                // else continue one more time
            }
        }
    }

    @Override
    public Result execute( String query ) throws QueryExecutionException
    {
        return getGraphDatabaseAPI().execute( query );
    }

    @Override
    public Result execute( String query, long timeout, TimeUnit unit ) throws QueryExecutionException
    {
        return getGraphDatabaseAPI().execute( query, timeout, unit );
    }

    @Override
    public Result execute( String query, Map<String, Object> parameters ) throws QueryExecutionException
    {
        return getGraphDatabaseAPI().execute( query, parameters );
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters, long timeout, TimeUnit unit ) throws
            QueryExecutionException
    {
        return getGraphDatabaseAPI().execute( query, parameters, timeout, unit );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext )
    {
        return getGraphDatabaseAPI().beginTransaction( type, loginContext );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, LoginContext loginContext, long timeout,
            TimeUnit unit )
    {
        return getGraphDatabaseAPI().beginTransaction( type, loginContext, timeout, unit );
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
    public Node createNode( Label... labels )
    {
        return getGraphDatabaseAPI().createNode( labels );
    }

    @Override
    public Node getNodeById( long id )
    {
        return getGraphDatabaseAPI().getNodeById( id );
    }

    @Deprecated
    @Override
    public IndexManager index()
    {
        return getGraphDatabaseAPI().index();
    }

    @Override
    public Schema schema()
    {
        return getGraphDatabaseAPI().schema();
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
            GraphDatabaseFactory factory = newFactory();
            factory.setMonitors( monitors );
            configure( factory );
            databaseBuilder = newBuilder( factory );
            globalConfig.forEach( databaseBuilder::setConfig );
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

    protected abstract GraphDatabaseFactory newFactory();

    protected abstract GraphDatabaseBuilder newBuilder( GraphDatabaseFactory factory );

    protected void configure( GraphDatabaseFactory databaseFactory )
    {
        // Override to configure the database factory
    }

    /**
     * {@link DatabaseRule} now implements {@link GraphDatabaseAPI} directly, so no need. Also for ensuring
     * a lazily started database is created, use {@link #ensureStarted()} instead.
     */
    public GraphDatabaseAPI getGraphDatabaseAPI()
    {
        ensureStarted();
        return database;
    }

    public synchronized void ensureStarted()
    {
        if ( database == null )
        {
            database = (GraphDatabaseAPI) databaseBuilder.newGraphDatabase();
            databaseLayout = database.databaseLayout();
            statementSupplier = resolveDependency( ThreadToStatementContextBridge.class );
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
    public DatabaseRule withSetting( Setting<?> key, String value )
    {
        if ( database != null )
        {
            // Database already started
            throw new IllegalStateException( "Wanted to set " + key + "=" + value + ", but database has already been started" );
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
     * @see #withSetting(Setting, String)
     */
    public DatabaseRule withSettings( Map<Setting<?>,String> configuration )
    {
        configuration.forEach( this::withSetting );
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

    public GraphDatabaseAPI restartDatabase( String... configChanges ) throws IOException
    {
        return restartDatabase( RestartAction.EMPTY, configChanges );
    }

    public GraphDatabaseAPI restartDatabase( RestartAction action, String... configChanges ) throws IOException
    {
        FileSystemAbstraction fs = resolveDependency( FileSystemAbstraction.class );
        database.shutdown();
        action.run( fs, databaseLayout );
        database = null;
        // This DatabaseBuilder has already been configured with the global settings as well as any test-specific settings,
        // so just apply these additional settings.
        databaseBuilder.setConfig( stringMap( configChanges ) );
        return getGraphDatabaseAPI();
    }

    @Override
    public void shutdown()
    {
        shutdown( true );
    }

    private void shutdown( boolean deleteResources )
    {
        statementSupplier = null;
        try
        {
            if ( database != null )
            {
                database.shutdown();
            }
        }
        finally
        {
            if ( deleteResources )
            {
                deleteResources();
            }
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

    public Statement statement()
    {
        ensureStarted();
        return statementSupplier.get();
    }

    public KernelTransaction transaction()
    {
        ensureStarted();
        return database.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                .getKernelTransactionBoundToThisThread( true );
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return database.getDependencyResolver();
    }

    @Override
    public StoreId storeId()
    {
        return database.storeId();
    }

    @Override
    public DatabaseLayout databaseLayout()
    {
        return database.databaseLayout();
    }

    public String getDatabaseDirAbsolutePath()
    {
        return databaseLayout().databaseDirectory().getAbsolutePath();
    }

    @Override
    public URL validateURLAccess( URL url ) throws URLAccessValidationError
    {
        return database.validateURLAccess( url );
    }

    @Override
    public Node createNode()
    {
        return database.createNode();
    }

    @Override
    public Long createNodeId()
    {
        return database.createNodeId();
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        return database.getRelationshipById( id );
    }

    @Override
    public ResourceIterable<Node> getAllNodes()
    {
        return database.getAllNodes();
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships()
    {
        return database.getAllRelationships();
    }

    @Override
    public ResourceIterable<Label> getAllLabelsInUse()
    {
        return database.getAllLabelsInUse();
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypesInUse()
    {
        return database.getAllRelationshipTypesInUse();
    }

    @Override
    public ResourceIterable<Label> getAllLabels()
    {
        return database.getAllLabels();
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypes()
    {
        return database.getAllRelationshipTypes();
    }

    @Override
    public ResourceIterable<String> getAllPropertyKeys()
    {
        return database.getAllPropertyKeys();
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key, Object value )
    {
        return database.findNodes( label, key, value );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2 )
    {
        return database.findNodes( label, key1, value1, key2, value2 );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2,
            String key3, Object value3 )
    {
        return database.findNodes( label, key1, value1, key2, value2, key3, value3 );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, Map<String,Object> propertyValues )
    {
        return database.findNodes( label, propertyValues );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key, String template, StringSearchMode searchMode )
    {
        return database.findNodes( label, key, template, searchMode );
    }

    @Override
    public Node findNode( Label label, String key, Object value )
    {
        return database.findNode( label, key, value );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label )
    {
        return database.findNodes( label );
    }

    @Override
    public boolean isAvailable( long timeout )
    {
        return database.isAvailable( timeout );
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler( TransactionEventHandler<T> handler )
    {
        return database.registerTransactionEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler( TransactionEventHandler<T> handler )
    {
        return database.unregisterTransactionEventHandler( handler );
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        return database.registerKernelEventHandler( handler );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return database.unregisterKernelEventHandler( handler );
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        return database.traversalDescription();
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        return database.bidirectionalTraversalDescription();
    }
}
