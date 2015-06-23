/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.factory;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.LongFunction;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.ResourceClosingIterator;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.KernelEventHandlers;
import org.neo4j.kernel.PlaceboTransaction;
import org.neo4j.kernel.TopLevelTransaction;
import org.neo4j.kernel.TransactionEventHandlers;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.traversal.BidirectionalTraversalDescriptionImpl;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.Log;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.String.format;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.map;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;
import static org.neo4j.kernel.impl.api.operations.KeyReadOperations.NO_SUCH_LABEL;
import static org.neo4j.kernel.impl.api.operations.KeyReadOperations.NO_SUCH_PROPERTY_KEY;

/**
 * Implementation of the GraphDatabaseService/GraphDatabaseService interfaces. This delegates to the
 * services created by the {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory}.
 *
 * To make a custom GraphDatabaseFacade, the best option is to subclass an existing GraphDatabaseFacadeFactory. Another
 * alternative, used by legacy database implementations, is to subclass this class and call
 * {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory#newFacade(java.io.File, java.util.Map, org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies, GraphDatabaseFacade)} in the constructor.
 */
public class GraphDatabaseFacade
    implements GraphDatabaseAPI
{
    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();
    private static final long MAX_RELATIONSHIP_ID = IdType.RELATIONSHIP.getMaxValue();

    private boolean initialized = false;

    private ThreadToStatementContextBridge threadToTransactionBridge;
    private NodeManager nodeManager;
    private IndexManager indexManager;
    private Schema schema;
    private AvailabilityGuard availabilityGuard;
    private Log msgLog;
    private LifeSupport life;
    private Supplier<KernelAPI> kernel;
    private Supplier<QueryExecutionEngine> queryExecutor;
    private KernelEventHandlers kernelEventHandlers;
    private TransactionEventHandlers transactionEventHandlers;

    private long transactionStartTimeout;
    private DependencyResolver dependencies;
    private Supplier<StoreId> storeId;
    protected File storeDir;

    public PlatformModule platformModule;
    public EditionModule editionModule;
    public DataSourceModule dataSourceModule;

    /**
     * When {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory#newFacade(java.io.File, java.util.Map, org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies, GraphDatabaseFacade)} has created the different
     * modules of a database, it calls this method so that the facade can get access to the created services.
     *
     * @param platformModule
     * @param editionModule
     * @param dataSourceModule
     */
    public void init(PlatformModule platformModule, EditionModule editionModule, DataSourceModule dataSourceModule)
    {
        this.platformModule = platformModule;
        this.editionModule = editionModule;
        this.dataSourceModule = dataSourceModule;
        this.threadToTransactionBridge = dataSourceModule.threadToTransactionBridge;
        this.nodeManager = dataSourceModule.nodeManager;
        this.indexManager = dataSourceModule.indexManager;
        this.schema = dataSourceModule.schema;
        this.availabilityGuard = platformModule.availabilityGuard;
        this.msgLog = platformModule.logging.getInternalLog( getClass() );
        this.life = platformModule.life;
        this.kernel = dataSourceModule.kernelAPI;
        this.queryExecutor = dataSourceModule.queryExecutor;
        this.kernelEventHandlers = dataSourceModule.kernelEventHandlers;
        this.transactionEventHandlers = dataSourceModule.transactionEventHandlers;
        this.transactionStartTimeout = editionModule.transactionStartTimeout;
        this.dependencies = platformModule.dependencies;
        this.storeId = dataSourceModule.storeId;
        this.storeDir = platformModule.storeDir;

        initialized = true;
    }

    @Override
    public Node createNode()
    {
        try ( Statement statement = threadToTransactionBridge.get() )
        {
            return nodeManager.newNodeProxyById( statement.dataWriteOperations().nodeCreate() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Node createNode( Label... labels )
    {
        try ( Statement statement = threadToTransactionBridge.get() )
        {
            long nodeId = statement.dataWriteOperations().nodeCreate();
            for ( Label label : labels )
            {
                int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( label.name() );
                try
                {
                    statement.dataWriteOperations().nodeAddLabel( nodeId, labelId );
                }
                catch ( EntityNotFoundException e )
                {
                    throw new NotFoundException( "No node with id " + nodeId + " found.", e );
                }
            }
            return nodeManager.newNodeProxyById( nodeId );
        }
        catch ( ConstraintValidationKernelException e )
        {
            throw new ConstraintViolationException( "Unable to add label.", e );
        }
        catch ( SchemaKernelException e )
        {
            throw new IllegalArgumentException( e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Node getNodeById( long id )
    {
        if ( id < 0 || id > MAX_NODE_ID )
        {
            throw new NotFoundException( format( "Node %d not found", id ),
                    new EntityNotFoundException( EntityType.NODE, id ) );
        }
        try ( Statement statement = threadToTransactionBridge.get() )
        {
            if ( !statement.readOperations().nodeExists( id ) )
            {
                throw new NotFoundException( format( "Node %d not found", id ),
                        new EntityNotFoundException( EntityType.NODE, id ) );
            }

            return nodeManager.newNodeProxyById( id );
        }
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        if ( id < 0 || id > MAX_RELATIONSHIP_ID )
        {
            throw new NotFoundException( format( "Relationship %d not found", id ),
                    new EntityNotFoundException( EntityType.RELATIONSHIP, id ));
        }
        try ( Statement statement = threadToTransactionBridge.get() )
        {
            if ( !statement.readOperations().relationshipExists( id ) )
            {
                throw new NotFoundException( format( "Relationship %d not found", id ),
                        new EntityNotFoundException( EntityType.RELATIONSHIP, id ));
            }

            return nodeManager.newRelationshipProxy( id );
        }
    }

    @Override
    public IndexManager index()
    {
        // TODO: txManager.assertInUnterminatedTransaction();
        return indexManager;
    }

    @Override
    public Schema schema()
    {
        threadToTransactionBridge.assertInUnterminatedTransaction();
        return schema;
    }

    @Override
    public boolean isAvailable( long timeout )
    {
        return availabilityGuard.isAvailable( timeout );
    }

    @Override
    public void shutdown()
    {
        if (initialized)
        {
            try
            {
                msgLog.info( "Shutdown started" );
                availabilityGuard.shutdown();
                life.shutdown();
            }
            catch ( LifecycleException throwable )
            {
                msgLog.warn( "Shutdown failed", throwable );
                throw throwable;
            }
        }
    }

    @Override
     public Transaction beginTx()
     {
         checkAvailability();

         TopLevelTransaction topLevelTransaction =
                 threadToTransactionBridge.getTopLevelTransactionBoundToThisThread( false );
         if ( topLevelTransaction != null )
         {
             return new PlaceboTransaction( topLevelTransaction );
         }

         try
         {
             KernelTransaction transaction = kernel.get().newTransaction();
             topLevelTransaction = new TopLevelTransaction( transaction, threadToTransactionBridge );
             threadToTransactionBridge.bindTransactionToCurrentThread( topLevelTransaction );
             return topLevelTransaction;
         }
         catch ( org.neo4j.kernel.api.exceptions.TransactionFailureException e )
         {
             throw new TransactionFailureException( "Failure to begin transaction", e );
         }
     }

     @Override
     public Result execute( String query ) throws QueryExecutionException
     {
         return execute( query, Collections.<String,Object>emptyMap() );
     }

    @Override
    public Result execute( String query, Map<String, Object> parameters ) throws QueryExecutionException
    {
        checkAvailability();

        try
        {
            return queryExecutor.get().executeQuery( query, parameters, QueryEngineProvider.embeddedSession() );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    private void checkAvailability()
    {
        try
        {
            availabilityGuard.await( transactionStartTimeout );
        }
        catch ( AvailabilityGuard.UnavailableException e )
        {
            throw new TransactionFailureException( e.getMessage() );
        }
    }

    @Override
    public Iterable<Node> getAllNodes()
    {
        return GlobalGraphOperations.at( this ).getAllNodes();
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return GlobalGraphOperations.at( this ).getAllRelationshipTypes();
    }

    @Override
    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler )
    {
        return kernelEventHandlers.registerKernelEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return transactionEventHandlers.registerTransactionEventHandler( handler );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler )
    {
        return kernelEventHandlers.unregisterKernelEventHandler( handler );
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return transactionEventHandlers.unregisterTransactionEventHandler( handler );
    }

    @Override
    public ResourceIterator<Node> findNodes( final Label myLabel, final String key, final Object value )
    {
        return nodesByLabelAndProperty( myLabel, key, value );
    }

    @Override
    public Node findNode( final Label myLabel, final String key, final Object value )
    {
        try ( ResourceIterator<Node> iterator = findNodes( myLabel, key, value ) )
        {
            if ( !iterator.hasNext() )
            {
                return null;
            }
            Node node = iterator.next();
            if ( iterator.hasNext() )
            {
                throw new MultipleFoundException();
            }
            return node;
        }
    }

    @Override
    public ResourceIterator<Node> findNodes( final Label myLabel )
    {
        return allNodesWithLabel( myLabel );
    }

    @Override
    public ResourceIterable<Node> findNodesByLabelAndProperty( final Label myLabel, final String key,
                                                               final Object value )
    {
        return new ResourceIterable<Node>()
        {
            @Override
            public ResourceIterator<Node> iterator()
            {
                return nodesByLabelAndProperty( myLabel, key, value );
            }
        };
    }

    private ResourceIterator<Node> nodesByLabelAndProperty( Label myLabel, String key, Object value )
    {
        Statement statement = threadToTransactionBridge.get();

        ReadOperations readOps = statement.readOperations();
        int propertyId = readOps.propertyKeyGetForName( key );
        int labelId = readOps.labelGetForName( myLabel.name() );

        if ( propertyId == NO_SUCH_PROPERTY_KEY || labelId == NO_SUCH_LABEL )
        {
            statement.close();
            return IteratorUtil.emptyIterator();
        }

        IndexDescriptor descriptor = findAnyIndexByLabelAndProperty( readOps, propertyId, labelId );

        try
        {
            if ( null != descriptor )
            {
                // Ha! We found an index - let's use it to find matching nodes
                return map2nodes( readOps.nodesGetFromIndexLookup( descriptor, value ), statement );
            }
        }
        catch ( IndexNotFoundKernelException e )
        {
            // weird at this point but ignore and fallback to a label scan
        }

        return getNodesByLabelAndPropertyWithoutIndex( propertyId, value, statement, labelId );
    }

    private IndexDescriptor findAnyIndexByLabelAndProperty( ReadOperations readOps, int propertyId, int labelId )
    {
        try
        {
            IndexDescriptor descriptor = readOps.indexesGetForLabelAndPropertyKey( labelId, propertyId );

            if ( readOps.indexGetState( descriptor ) == InternalIndexState.ONLINE )
            {
                // Ha! We found an index - let's use it to find matching nodes
                return descriptor;
            }
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            // If we don't find a matching index rule, we'll scan all nodes and filter manually (below)
        }
        return null;
    }

    private ResourceIterator<Node> getNodesByLabelAndPropertyWithoutIndex( int propertyId, Object value,
                                                                           Statement statement, int labelId )
    {
        return map2nodes(
                new PropertyValueFilteringNodeIdIterator(
                        statement.readOperations().nodesGetForLabel( labelId ),
                        statement.readOperations(), propertyId, value ), statement );
    }

    private ResourceIterator<Node> allNodesWithLabel( final Label myLabel )
    {
        Statement statement = threadToTransactionBridge.get();

        int labelId = statement.readOperations().labelGetForName( myLabel.name() );
        if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
        {
            statement.close();
            return emptyIterator();
        }

        final PrimitiveLongIterator nodeIds = statement.readOperations().nodesGetForLabel( labelId );
        return ResourceClosingIterator.newResourceIterator( statement, map( new LongFunction<Node>()
        {
            @Override
            public Node apply( long nodeId )
            {
                return nodeManager.newNodeProxyById( nodeId );
            }
        }, nodeIds ) );
    }

    private ResourceIterator<Node> map2nodes( PrimitiveLongIterator input, Statement statement )
    {
        return ResourceClosingIterator.newResourceIterator( statement, map( new LongFunction<Node>()
        {
            @Override
            public Node apply( long id )
            {
                return getNodeById( id );
            }
        }, input ) );
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        return new MonoDirectionalTraversalDescription( threadToTransactionBridge );
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        return new BidirectionalTraversalDescriptionImpl( threadToTransactionBridge );
    }

    // GraphDatabaseAPI
    @Override
    public DependencyResolver getDependencyResolver()
    {
        return dependencies;
    }

    @Override
    public StoreId storeId()
    {
        return storeId.get();
    }

    @Override
    public String getStoreDir()
    {
        return storeDir.toString();
    }

    @Override
    public String toString()
    {
        return platformModule.config.get( GraphDatabaseFacadeFactory.Configuration.editionName)+" ["+storeDir+"]";
    }

    private static class PropertyValueFilteringNodeIdIterator extends PrimitiveLongCollections.PrimitiveLongBaseIterator
    {
        private final PrimitiveLongIterator nodesWithLabel;
        private final ReadOperations statement;
        private final int propertyKeyId;
        private final Object value;

        PropertyValueFilteringNodeIdIterator( PrimitiveLongIterator nodesWithLabel, ReadOperations statement,
                                              int propertyKeyId, Object value )
        {
            this.nodesWithLabel = nodesWithLabel;
            this.statement = statement;
            this.propertyKeyId = propertyKeyId;
            this.value = value;
        }

        @Override
        protected boolean fetchNext()
        {
            for ( boolean hasNext = nodesWithLabel.hasNext(); hasNext; hasNext = nodesWithLabel.hasNext() )
            {
                long nextValue = nodesWithLabel.next();
                try
                {
                    if ( statement.nodeGetProperty( nextValue, propertyKeyId ).valueEquals( value ) )
                    {
                        return next( nextValue );
                    }
                }
                catch ( EntityNotFoundException e )
                {
                    // continue to the next node
                }
            }
            return false;
        }
    }
}
