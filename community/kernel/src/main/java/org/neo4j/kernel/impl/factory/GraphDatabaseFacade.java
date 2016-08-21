/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.net.URL;
import java.util.Collections;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
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
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.helpers.collection.ResourceClosingIterator;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.legacyindex.AutoIndexing;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.api.legacyindex.InternalAutoIndexing;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.coreapi.AutoIndexerFacade;
import org.neo4j.kernel.impl.coreapi.IndexManagerImpl;
import org.neo4j.kernel.impl.coreapi.IndexProviderImpl;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PlaceboTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.coreapi.ReadOnlyIndexFacade;
import org.neo4j.kernel.impl.coreapi.ReadOnlyRelationshipIndexFacade;
import org.neo4j.kernel.impl.coreapi.RelationshipAutoIndexerFacade;
import org.neo4j.kernel.impl.coreapi.StandardNodeActions;
import org.neo4j.kernel.impl.coreapi.StandardRelationshipActions;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QuerySession;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.traversal.BidirectionalTraversalDescriptionImpl;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.EntityType;

import static java.lang.String.format;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.map;
import static org.neo4j.helpers.collection.Iterators.emptyIterator;
import static org.neo4j.kernel.impl.api.operations.KeyReadOperations.NO_SUCH_LABEL;
import static org.neo4j.kernel.impl.api.operations.KeyReadOperations.NO_SUCH_PROPERTY_KEY;

/**
 * Implementation of the GraphDatabaseService/GraphDatabaseService interfaces - the "Core API". Given an {@link SPI}
 * implementation, this provides users with
 * a clean facade to interact with the database.
 */
public class GraphDatabaseFacade implements GraphDatabaseAPI
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();

    private Schema schema;
    private IndexManager indexManager;
    private NodeProxy.NodeActions nodeActions;
    private RelationshipProxy.RelationshipActions relActions;
    private SPI spi;

    /**
     * This is what you need to implemenent to get your very own {@link GraphDatabaseFacade}. This SPI exists as a thin
     * layer to make it easy to provide
     * alternate {@link org.neo4j.graphdb.GraphDatabaseService} instances without having to re-implement this whole API
     * implementation.
     */
    public interface SPI
    {
        /**
         * Check if database is available, waiting up to {@code timeout} if it isn't. If the timeout expires before
         * database available, this returns false
         */
        boolean databaseIsAvailable( long timeout );

        DependencyResolver resolver();

        StoreId storeId();

        File storeDir();

        /** Eg. Neo4j Enterprise HA, Neo4j Community Standalone.. */
        String name();

        void shutdown();

        /**
         * Begin a new kernel transaction. If a transaction is already associated to the current context
         * (meaning, non-null is returned from {@link #currentTransaction()}), this should fail.
         *
         * @throws org.neo4j.graphdb.TransactionFailureException if unable to begin, or a transaction already exists.
         */
        KernelTransaction beginTransaction( KernelTransaction.Type type, AccessMode accessMode );

        /**
         * Retrieve the transaction associated with the current context. For the classic implementation of the Core API,
         * the context is the current thread.
         * Must not return null, and must return the underlying transaction even if it has been terminated.
         *
         * @throws org.neo4j.graphdb.NotInTransactionException if no transaction present
         * @throws org.neo4j.graphdb.DatabaseShutdownException if the database has been shut down
         */
        KernelTransaction currentTransaction();

        /** true if {@link #currentTransaction()} would return a transaction. */
        boolean isInOpenTransaction();

        /** Acquire a statement to perform work with */
        Statement currentStatement();

        /** Execute a cypher statement */
        Result executeQuery( String query, Map<String,Object> parameters, QuerySession querySession );

        AutoIndexing autoIndexing();

        void registerKernelEventHandler( KernelEventHandler handler );

        void unregisterKernelEventHandler( KernelEventHandler handler );

        <T> void registerTransactionEventHandler( TransactionEventHandler<T> handler );

        <T> void unregisterTransactionEventHandler( TransactionEventHandler<T> handler );

        URL validateURLAccess( URL url ) throws URLAccessValidationError;

        GraphDatabaseQueryService queryService();
    }

    public GraphDatabaseFacade()
    {
    }

    /**
     * Create a new Core API facade, backed by the given SPI.
     */
    public void init( SPI spi )
    {
        IndexProviderImpl idxProvider = new IndexProviderImpl( this, spi::currentStatement );

        this.spi = spi;

        this.relActions = new StandardRelationshipActions( spi::currentStatement, spi::currentTransaction,
                this::assertTransactionOpen, ( id ) -> new NodeProxy( nodeActions, id ), this );
        this.nodeActions =
                new StandardNodeActions( spi::currentStatement, spi::currentTransaction, this::assertTransactionOpen,
                        relActions, this );
        this.schema = new SchemaImpl( spi::currentStatement );
        AutoIndexerFacade<Node> nodeAutoIndexer = new AutoIndexerFacade<>(
                () -> new ReadOnlyIndexFacade<>(
                        idxProvider.getOrCreateNodeIndex( InternalAutoIndexing.NODE_AUTO_INDEX, null ) ),
                spi.autoIndexing().nodes() );
        RelationshipAutoIndexerFacade relAutoIndexer = new RelationshipAutoIndexerFacade(
                () -> new ReadOnlyRelationshipIndexFacade( idxProvider
                        .getOrCreateRelationshipIndex( InternalAutoIndexing.RELATIONSHIP_AUTO_INDEX, null ) ),
                spi.autoIndexing().relationships() );
        this.indexManager = new IndexManagerImpl( spi::currentStatement, idxProvider, nodeAutoIndexer, relAutoIndexer );
    }

    @Override
    public Node createNode()
    {
        try ( Statement statement = spi.currentStatement() )
        {
            return new NodeProxy( nodeActions, statement.dataWriteOperations().nodeCreate() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Node createNode( Label... labels )
    {
        try ( Statement statement = spi.currentStatement() )
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
            return new NodeProxy( nodeActions, nodeId );
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
        if ( id < 0 )
        {
            throw new NotFoundException( format( "Node %d not found", id ),
                    new EntityNotFoundException( EntityType.NODE, id ) );
        }
        try ( Statement statement = spi.currentStatement() )
        {
            if ( !statement.readOperations().nodeExists( id ) )
            {
                throw new NotFoundException( format( "Node %d not found", id ),
                        new EntityNotFoundException( EntityType.NODE, id ) );
            }

            return new NodeProxy( nodeActions, id );
        }
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        if ( id < 0 )
        {
            throw new NotFoundException( format( "Relationship %d not found", id ),
                    new EntityNotFoundException( EntityType.RELATIONSHIP, id ) );
        }
        try ( Statement statement = spi.currentStatement() )
        {
            try
            {
                RelationshipProxy relationship = new RelationshipProxy( relActions, id );
                statement.readOperations().relationshipVisit( id, relationship );
                return relationship;
            }
            catch ( EntityNotFoundException e )
            {
                throw new NotFoundException( format( "Relationship %d not found", id ), e );
            }
        }
    }

    @Override
    public IndexManager index()
    {
        return indexManager;
    }

    @Override
    public Schema schema()
    {
        assertTransactionOpen();
        return schema;
    }

    @Override
    public boolean isAvailable( long timeout )
    {
        return spi.databaseIsAvailable( timeout );
    }

    @Override
    public void shutdown()
    {
        spi.shutdown();
    }

    @Override
    public Transaction beginTx()
    {
        return beginTransaction( KernelTransaction.Type.explicit, AccessMode.Static.FULL );
    }

    public InternalTransaction beginTransaction( KernelTransaction.Type type, AccessMode accessMode )
    {
        if ( spi.isInOpenTransaction() )
        {
            // FIXME: perhaps we should check that the new type and access mode are compatible with the current tx
            return new PlaceboTransaction( spi::currentTransaction, spi::currentStatement );
        }

        return new TopLevelTransaction( spi.beginTransaction( type, accessMode ), spi::currentStatement );
    }

    @Override
    public Result execute( String query ) throws QueryExecutionException
    {
        return execute( query, Collections.<String,Object>emptyMap() );
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        // ensure we have a tx and create a context (the tx is gonna get closed by the Cypher result)
        InternalTransaction transaction = beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL );
        return execute( transaction, query, parameters );
    }

    // This version of execute is only needed for internal testing of LOAD CSV PERIODIC COMMIT. Can be refactored?
    public Result execute( InternalTransaction transaction, String query, Map<String,Object> parameters )
            throws QueryExecutionException
    {
        TransactionalContext transactionalContext =
                new Neo4jTransactionalContext( spi.queryService(), transaction, spi.currentStatement(), locker );
        return spi.executeQuery( query, parameters, QueryEngineProvider.embeddedSession( transactionalContext ) );
    }

    @Override
    public ResourceIterable<Node> getAllNodes()
    {
        assertTransactionOpen();
        return () -> {
            Statement statement = spi.currentStatement();
            return map2nodes( statement.readOperations().nodesGetAll(), statement );
        };
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships()
    {
        assertTransactionOpen();
        return () -> {
            final Statement statement = spi.currentStatement();
            final PrimitiveLongIterator ids = statement.readOperations().relationshipsGetAll();
            return new PrefetchingResourceIterator<Relationship>()
            {
                @Override
                public void close()
                {
                    statement.close();
                }

                @Override
                protected Relationship fetchNextOrNull()
                {
                    return ids.hasNext() ? new RelationshipProxy( relActions, ids.next() ) : null;
                }
            };
        };
    }

    @Override
    public ResourceIterable<Label> getAllLabelsInUse()
    {
        return allInUse( TokenAccess.LABELS );
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypesInUse()
    {
        return allInUse( TokenAccess.RELATIONSHIP_TYPES );
    }
    private <T> ResourceIterable<T> allInUse( final TokenAccess<T> tokens )
    {
        assertTransactionOpen();
        return () -> tokens.inUse( spi.currentStatement() );
    }

    @Override
    public ResourceIterable<Label> getAllLabels()
    {
        return all( TokenAccess.LABELS );
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypes()
    {
        return all( TokenAccess.RELATIONSHIP_TYPES );
    }

    @Override
    public ResourceIterable<String> getAllPropertyKeys()
    {
        return all( TokenAccess.PROPERTY_KEYS );
    }

    private <T> ResourceIterable<T> all( final TokenAccess<T> tokens )
    {
        assertTransactionOpen();
        return () -> tokens.all( spi.currentStatement() );
    }

    @Override
    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler )
    {
        spi.registerKernelEventHandler( handler );
        return handler;
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        spi.registerTransactionEventHandler( handler );
        return handler;
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler )
    {
        spi.unregisterKernelEventHandler( handler );
        return handler;
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        spi.unregisterTransactionEventHandler( handler );
        return handler;
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

    private ResourceIterator<Node> nodesByLabelAndProperty( Label myLabel, String key, Object value )
    {
        Statement statement = spi.currentStatement();

        ReadOperations readOps = statement.readOperations();
        int propertyId = readOps.propertyKeyGetForName( key );
        int labelId = readOps.labelGetForName( myLabel.name() );

        if ( propertyId == NO_SUCH_PROPERTY_KEY || labelId == NO_SUCH_LABEL )
        {
            statement.close();
            return emptyIterator();
        }

        IndexDescriptor descriptor = findAnyIndexByLabelAndProperty( readOps, propertyId, labelId );

        try
        {
            if ( null != descriptor )
            {
                // Ha! We found an index - let's use it to find matching nodes
                return map2nodes( readOps.nodesGetFromIndexSeek( descriptor, value ), statement );
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
            IndexDescriptor descriptor = readOps.indexGetForLabelAndPropertyKey( labelId, propertyId );

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
        Statement statement = spi.currentStatement();

        int labelId = statement.readOperations().labelGetForName( myLabel.name() );
        if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
        {
            statement.close();
            return emptyIterator();
        }

        final PrimitiveLongIterator nodeIds = statement.readOperations().nodesGetForLabel( labelId );
        return ResourceClosingIterator
                .newResourceIterator( statement, map( nodeId -> new NodeProxy( nodeActions, nodeId ), nodeIds ) );
    }

    private ResourceIterator<Node> map2nodes( PrimitiveLongIterator input, Statement statement )
    {
        return ResourceClosingIterator
                .newResourceIterator( statement, map( id -> new NodeProxy( nodeActions, id ), input ) );
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        return new MonoDirectionalTraversalDescription( spi::currentStatement );
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        return new BidirectionalTraversalDescriptionImpl( spi::currentStatement );
    }

    // GraphDatabaseAPI
    @Override
    public DependencyResolver getDependencyResolver()
    {
        return spi.resolver();
    }

    @Override
    public StoreId storeId()
    {
        return spi.storeId();
    }

    @Override
    public URL validateURLAccess( URL url ) throws URLAccessValidationError
    {
        return spi.validateURLAccess( url );
    }

    @Override
    public String getStoreDir()
    {
        return spi.storeDir().getAbsolutePath();
    }

    @Override
    public String toString()
    {
        return spi.name() + " [" + getStoreDir() + "]";
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
                    Object propertyValue = statement.nodeGetProperty( nextValue, propertyKeyId );
                    if ( propertyValue != null )
                    {
                        if ( Property.property( propertyKeyId, propertyValue ).valueEquals( value ) )
                        {
                            return next( nextValue );
                        }
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

    private void assertTransactionOpen()
    {
        Status reason = spi.currentTransaction().getReasonIfTerminated();
        if ( reason != null )
        {
            throw new TransactionTerminatedException( reason );
        }
    }
}
