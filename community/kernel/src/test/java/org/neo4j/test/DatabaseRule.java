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
package org.neo4j.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.neo4j.function.Consumer;
import org.neo4j.function.Function;
import org.neo4j.function.Functions;
import org.neo4j.function.Supplier;
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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseBuilderTestTools;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.security.URLAccessValidationError;

public abstract class DatabaseRule extends ExternalResource implements GraphDatabaseAPI
{
    GraphDatabaseBuilder databaseBuilder;
    GraphDatabaseAPI database;
    private String storeDir;
    private Supplier<Statement> statementSupplier;
    private boolean startEagerly = true;

    /**
     * Means the database will be started on first {@link #getGraphDatabaseAPI()}, {@link #getGraphDatabaseService()}
     * or {@link #ensureStarted()} call.
     */
    public DatabaseRule startLazily()
    {
        startEagerly = false;
        return this;
    }

    public <T> T when( Function<GraphDatabaseService, T> function )
    {
        return function.apply( getGraphDatabaseService() );
    }

    public void executeAndCommit( Consumer<? super GraphDatabaseService> consumer )
    {
        transaction( Functions.fromConsumer( consumer ), true );
    }

    public <T> T executeAndCommit( Function<? super GraphDatabaseService, T> function )
    {
        return transaction( function, true );
    }

    public <T> T executeAndRollback( Function<? super GraphDatabaseService, T> function )
    {
        return transaction( function, false );
    }

    public <FROM, TO> AlgebraicFunction<FROM, TO> tx( final Function<FROM, TO> function )
    {
        return new AlgebraicFunction<FROM, TO>()
        {
            @Override
            public TO apply( final FROM from )
            {
                return executeAndCommit( new Function<GraphDatabaseService, TO>()
                {
                    @Override
                    public TO apply( GraphDatabaseService graphDb )
                    {
                        return function.apply( from );
                    }
                } );
            }

            @Override
            public String toString()
            {
                return "tx( " + function + " )";
            }
        };
    }

    private <T> T transaction( Function<? super GraphDatabaseService, T> function, boolean commit )
    {
        GraphDatabaseService db = getGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            T result = function.apply( db );

            if ( commit )
            {
                tx.success();
            }
            return result;
        }
    }

    @Override
    public Result execute( String query ) throws QueryExecutionException
    {
        return getGraphDatabaseAPI().execute( query );
    }

    @Override
    public Result execute( String query, Map<String, Object> parameters ) throws QueryExecutionException
    {
        return getGraphDatabaseAPI().execute( query, parameters );
    }

    @Override
    public Transaction beginTx()
    {
        return getGraphDatabaseAPI().beginTx();
    }

    @Override
    public Node createNode( Label... labels )
    {
        return getGraphDatabaseAPI().createNode( labels );
    }

    @Override
    public Node getNodeById( long id )
    {
        return getGraphDatabaseService().getNodeById( id );
    }

    @Override
    public IndexManager index()
    {
        return getGraphDatabaseService().index();
    }

    @Override
    public Schema schema()
    {
        return getGraphDatabaseAPI().schema();
    }

    @Override
    protected void before() throws Throwable
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

    private void create() throws IOException
    {
        createResources();
        try
        {
            GraphDatabaseFactory factory = newFactory();
            configure( factory );
            databaseBuilder = newBuilder( factory );
            configure( databaseBuilder );
        }
        catch ( RuntimeException e )
        {
            deleteResources();
            throw e;
        }
    }

    protected void deleteResources()
    {
    }

    protected void createResources() throws IOException
    {
    }

    protected abstract GraphDatabaseFactory newFactory();

    protected abstract GraphDatabaseBuilder newBuilder( GraphDatabaseFactory factory );

    protected void configure( GraphDatabaseFactory databaseFactory )
    {
        // Override to configure the database factory
    }

    protected void configure( GraphDatabaseBuilder builder )
    {
        // Override to configure the database

        // Adjusted defaults for testing
    }

    public GraphDatabaseBuilder setConfig( Setting<?> setting, String value )
    {
        return databaseBuilder.setConfig( setting, value );
    }

    public Config getConfigCopy()
    {
        return GraphDatabaseBuilderTestTools.createConfigCopy( databaseBuilder );
    }

    public void resetConfig()
    {
        GraphDatabaseBuilderTestTools.clearConfig( databaseBuilder );
    }

    /**
     * {@link DatabaseRule} now implements {@link GraphDatabaseAPI} directly, so no need. Also for ensuring
     * a lazily started database is created, use {@link #ensureStarted()} instead.
     */
    @Deprecated
    public GraphDatabaseService getGraphDatabaseService()
    {
        return getGraphDatabaseAPI();
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
            storeDir = database.getStoreDir();
            statementSupplier = resolveDependency( ThreadToStatementContextBridge.class );
        }
    }

    public static interface RestartAction
    {
        void run( FileSystemAbstraction fs, File storeDirectory ) throws IOException;

        public static RestartAction EMPTY = new RestartAction()
        {
            @Override
            public void run( FileSystemAbstraction fs, File storeDirectory )
            {
                // duh
            }
        };
    }

    public GraphDatabaseAPI restartDatabase() throws IOException
    {
        return restartDatabase( RestartAction.EMPTY );
    }

    public GraphDatabaseAPI restartDatabase( RestartAction action ) throws IOException
    {
        FileSystemAbstraction fs = resolveDependency( FileSystemAbstraction.class );
        database.shutdown();
        action.run( fs, new File( storeDir ) );
        database = null;
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

    public void stopAndKeepFiles()
    {
        if ( database != null )
        {
            database.shutdown();
            database = null;
            statementSupplier = null;
        }
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
    public String getStoreDir()
    {
        return database.getStoreDir();
    }

    public String getStoreDirAbsolutePath()
    {
        return new File( getStoreDir() ).getAbsolutePath();
    }

    public File getStoreDirFile()
    {
        return new File( getStoreDir() );
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
    public Relationship getRelationshipById( long id )
    {
        return database.getRelationshipById( id );
    }

    @Override
    public Iterable<Node> getAllNodes()
    {
        return database.getAllNodes();
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key, Object value )
    {
        return database.findNodes( label, key, value );
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
    public ResourceIterable<Node> findNodesByLabelAndProperty( Label label, String key, Object value )
    {
        return database.findNodesByLabelAndProperty( label, key, value );
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return database.getRelationshipTypes();
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
