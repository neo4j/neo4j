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
package org.neo4j.kernel.impl.factory;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
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
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.GraphPropertiesProxy;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.coreapi.AutoIndexerFacade;
import org.neo4j.kernel.impl.coreapi.IndexManagerImpl;
import org.neo4j.kernel.impl.coreapi.IndexProviderImpl;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PlaceboTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.coreapi.ReadOnlyIndexFacade;
import org.neo4j.kernel.impl.coreapi.ReadOnlyRelationshipIndexFacade;
import org.neo4j.kernel.impl.coreapi.RelationshipAutoIndexerFacade;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.traversal.BidirectionalTraversalDescriptionImpl;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.helpers.collection.Iterators.emptyResourceIterator;
import static org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing.NODE_AUTO_INDEX;
import static org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing.RELATIONSHIP_AUTO_INDEX;
import static org.neo4j.kernel.impl.api.operations.KeyReadOperations.NO_SUCH_LABEL;
import static org.neo4j.kernel.impl.api.operations.KeyReadOperations.NO_SUCH_PROPERTY_KEY;

/**
 * Implementation of the GraphDatabaseService/GraphDatabaseService interfaces - the "Core API". Given an {@link SPI}
 * implementation, this provides users with
 * a clean facade to interact with the database.
 */
public class GraphDatabaseFacade implements GraphDatabaseAPI, EmbeddedProxySPI
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();

    private Schema schema;
    private Supplier<IndexManager> indexManager;
    private ThreadToStatementContextBridge statementContext;
    private SPI spi;
    private TransactionalContextFactory contextFactory;
    private Config config;
    private RelationshipTypeTokenHolder relationshipTypeTokenHolder;

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
         * Begin a new kernel transaction with specified timeout in milliseconds.
         *
         * @throws org.neo4j.graphdb.TransactionFailureException if unable to begin, or a transaction already exists.
         * @see SPI#beginTransaction(KernelTransaction.Type, SecurityContext)
         */
        KernelTransaction beginTransaction( KernelTransaction.Type type, SecurityContext securityContext,
                long timeout );

        /** Execute a cypher statement */
        Result executeQuery( String query, Map<String,Object> parameters, TransactionalContext context );

        /** Execute a cypher statement */
        Result executeQuery( String query, MapValue parameters, TransactionalContext context );

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
     * Create a new Core API facade, backed by the given SPI and using pre-resolved dependencies
     */
    public void init( SPI spi, Guard guard, ThreadToStatementContextBridge txBridge, Config config,
            RelationshipTypeTokenHolder relationshipTypeTokenHolder )
    {
        this.spi = spi;
        this.config = config;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        this.schema = new SchemaImpl( txBridge );
        this.statementContext = txBridge;
        this.indexManager = Suppliers.lazySingleton( () ->
        {
            IndexProviderImpl idxProvider = new IndexProviderImpl( this, txBridge );
            AutoIndexerFacade<Node> nodeAutoIndexer = new AutoIndexerFacade<>(
                    () -> new ReadOnlyIndexFacade<>( idxProvider.getOrCreateNodeIndex( NODE_AUTO_INDEX, null ) ),
                    spi.autoIndexing().nodes() );
            RelationshipAutoIndexerFacade relAutoIndexer = new RelationshipAutoIndexerFacade(
                    () -> new ReadOnlyRelationshipIndexFacade(
                            idxProvider.getOrCreateRelationshipIndex( RELATIONSHIP_AUTO_INDEX, null ) ),
                    spi.autoIndexing().relationships() );

            return new IndexManagerImpl( txBridge, idxProvider, nodeAutoIndexer, relAutoIndexer );
        } );

        this.contextFactory = Neo4jTransactionalContextFactory.create( spi, guard, txBridge, locker );
    }

    @Override
    public Node createNode()
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true );
        try ( Statement ignore = transaction.acquireStatement() )
        {
            return newNodeProxy( transaction.dataWrite().nodeCreate() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Long createNodeId()
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true );
        try ( Statement ignore = transaction.acquireStatement() )
        {
            return transaction.dataWrite().nodeCreate();
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Node createNode( Label... labels )
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true );
        try ( Statement ignore = transaction.acquireStatement() )
        {
            Write write = transaction.dataWrite();
            long nodeId = write.nodeCreate();
            for ( Label label : labels )
            {
                int labelId = transaction.tokenWrite().labelGetOrCreateForName( label.name() );
                try
                {
                    write.nodeAddLabel( nodeId, labelId );
                }
                catch ( EntityNotFoundException e )
                {
                    throw new NotFoundException( "No node with id " + nodeId + " found.", e );
                }
            }
            return newNodeProxy( nodeId );
        }
        catch ( ConstraintValidationException e )
        {
            throw new ConstraintViolationException( "Unable to add label.", e );
        }
        catch ( SchemaKernelException e )
        {
            throw new IllegalArgumentException( e );
        }
        catch ( KernelException e )
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

        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread( true );
        assertTransactionOpen( ktx );
        try ( Statement ignore = ktx.acquireStatement() )
        {
            if ( !ktx.dataRead().nodeExists( id ) )
            {
                throw new NotFoundException( format( "Node %d not found", id ),
                        new EntityNotFoundException( EntityType.NODE, id ) );
            }
            return newNodeProxy( id );
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

        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread( true );
        assertTransactionOpen( ktx );
        try ( Statement ignore = statementContext.get() )
        {
            if ( !ktx.dataRead().relationshipExists( id ) )
            {
                throw new NotFoundException( format( "Relationship %d not found", id ),
                        new EntityNotFoundException( EntityType.RELATIONSHIP, id ) );
            }
            return newRelationshipProxy( id );
        }
    }

    @Override
    public IndexManager index()
    {
        return indexManager.get();
    }

    @Override
    public Schema schema()
    {
        assertTransactionOpen();
        return schema;
    }

    @Override
    public boolean isAvailable( long timeoutMillis )
    {
        return spi.databaseIsAvailable( timeoutMillis );
    }

    @Override
    public void shutdown()
    {
        spi.shutdown();
    }

    @Override
    public Transaction beginTx()
    {
        return beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED );
    }

    @Override
    public Transaction beginTx( long timeout, TimeUnit unit )
    {
        return beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED, timeout, unit );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, SecurityContext securityContext )
    {
        return beginTransactionInternal( type, securityContext, config.get( transaction_timeout ).toMillis() );
    }

    @Override
    public InternalTransaction beginTransaction( KernelTransaction.Type type, SecurityContext securityContext,
            long timeout, TimeUnit unit )
    {
        return beginTransactionInternal( type, securityContext, unit.toMillis( timeout ) );
    }

    @Override
    public Result execute( String query ) throws QueryExecutionException
    {
        return execute( query, Collections.emptyMap() );
    }

    @Override
    public Result execute( String query, long timeout, TimeUnit unit ) throws QueryExecutionException
    {
        return execute( query, Collections.emptyMap(), timeout, unit );
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        // ensure we have a tx and create a context (the tx is gonna get closed by the Cypher result)
        InternalTransaction transaction =
                beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );

        return execute( transaction, query, ValueUtils.asMapValue( parameters ) );
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters, long timeout, TimeUnit unit ) throws
            QueryExecutionException
    {
        InternalTransaction transaction =
                beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED, timeout, unit );
        return execute( transaction, query, ValueUtils.asMapValue( parameters ) );
    }

    public Result execute( InternalTransaction transaction, String query, MapValue parameters )
            throws QueryExecutionException
    {
        TransactionalContext context =
                contextFactory.newContext( ClientConnectionInfo.EMBEDDED_CONNECTION, transaction, query, parameters );
        return spi.executeQuery( query, parameters, context );
    }

    @Override
    public ResourceIterable<Node> getAllNodes()
    {
        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread( true );
        assertTransactionOpen( ktx );
        return () ->
        {
            Statement statement = ktx.acquireStatement();
            NodeCursor cursor = ktx.cursors().allocateNodeCursor();
            ktx.dataRead().allNodesScan( cursor );
            return new PrefetchingResourceIterator<Node>()
            {
                @Override
                protected Node fetchNextOrNull()
                {
                    if ( cursor.next() )
                    {
                        return newNodeProxy( cursor.nodeReference() );
                    }
                    else
                    {
                        close();
                        return null;
                    }
                }

                @Override
                public void close()
                {
                    cursor.close();
                    statement.close();
                }
            };
        };
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships()
    {
        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread( true );
        assertTransactionOpen( ktx );
        return () ->
        {
            Statement statement = ktx.acquireStatement();
            RelationshipScanCursor cursor = ktx.cursors().allocateRelationshipScanCursor();
            ktx.dataRead().allRelationshipsScan( cursor );
            return new PrefetchingResourceIterator<Relationship>()
            {
                @Override
                protected Relationship fetchNextOrNull()
                {
                    if ( cursor.next() )
                    {
                        return newRelationshipProxy(
                                cursor.relationshipReference(),
                                cursor.sourceNodeReference(),
                                cursor.label(),
                                cursor.targetNodeReference() );
                    }
                    else
                    {
                        close();
                        return null;
                    }
                }

                @Override
                public void close()
                {
                    cursor.close();
                    statement.close();
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
        return () -> tokens.inUse( statementContext.get() );
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
        return () -> tokens.all( statementContext.get() );
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
        return nodesByLabelAndProperty( myLabel, key, Values.of( value ) );
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
                throw new MultipleFoundException(
                        format( "Found multiple nodes with label: '%s', property name: '%s' and property " +
                                "value: '%s' while only one was expected.", myLabel, key, value ) );
            }
            return node;
        }
    }

    @Override
    public ResourceIterator<Node> findNodes( final Label myLabel )
    {
        return allNodesWithLabel( myLabel );
    }

    private InternalTransaction beginTransactionInternal( KernelTransaction.Type type, SecurityContext securityContext,
            long timeoutMillis )
    {
        if ( statementContext.hasTransaction() )
        {
            // FIXME: perhaps we should check that the new type and access mode are compatible with the current tx
            return new PlaceboTransaction( statementContext.getKernelTransactionBoundToThisThread( true ), statementContext );
        }
        return new TopLevelTransaction( spi.beginTransaction( type, securityContext, timeoutMillis ), statementContext );
    }

    private ResourceIterator<Node> nodesByLabelAndProperty( Label myLabel, String key, Value value )
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true );
        Statement statement = transaction.acquireStatement();
        Read read = transaction.dataRead();
        TokenRead tokenRead = transaction.tokenRead();
        int propertyId = tokenRead.propertyKey( key );
        int labelId = tokenRead.nodeLabel( myLabel.name() );

        if ( propertyId == NO_SUCH_PROPERTY_KEY || labelId == NO_SUCH_LABEL )
        {
            statement.close();
            return emptyResourceIterator();
        }
        CapableIndexReference index = transaction.schemaRead().index( labelId, propertyId );
        if ( index != CapableIndexReference.NO_INDEX )
        {
            // Ha! We found an index - let's use it to find matching nodes
            try
            {
                NodeValueIndexCursor cursor = transaction.cursors().allocateNodeValueIndexCursor();
                IndexQuery.ExactPredicate query = IndexQuery.exact( propertyId, value );
                read.nodeIndexSeek( index, cursor, IndexOrder.NONE, query );

                return new NodeCursorResourceIterator<>( cursor, statement, this::newNodeProxy );
            }
            catch ( KernelException e )
            {
                // weird at this point but ignore and fallback to a label scan
            }
        }

        return getNodesByLabelAndPropertyWithoutIndex( propertyId, value, statement, labelId );
    }

    private ResourceIterator<Node> getNodesByLabelAndPropertyWithoutIndex( int propertyId, Value value,
            Statement statement, int labelId )
    {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread( true );

        NodeLabelIndexCursor nodeLabelCursor = transaction.cursors().allocateNodeLabelIndexCursor();
        NodeCursor nodeCursor = transaction.cursors().allocateNodeCursor();
        PropertyCursor propertyCursor = transaction.cursors().allocatePropertyCursor();

        transaction.dataRead().nodeLabelScan( labelId, nodeLabelCursor );

        return new NodeLabelPropertyIterator( transaction.dataRead(),
                                                nodeLabelCursor,
                                                nodeCursor,
                                                propertyCursor,
                                                statement,
                                                this::newNodeProxy,
                                                propertyId,
                                                value );
    }

    private ResourceIterator<Node> allNodesWithLabel( final Label myLabel )
    {
        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread( true );
        Statement statement = ktx.acquireStatement();

        int labelId = ktx.tokenRead().nodeLabel( myLabel.name() );
        if ( labelId == NO_SUCH_LABEL )
        {
            statement.close();
            return ResourceIterator.empty();
        }

        NodeLabelIndexCursor cursor = ktx.cursors().allocateNodeLabelIndexCursor();
        ktx.dataRead().nodeLabelScan( labelId, cursor );
        return new NodeCursorResourceIterator<>( cursor, statement, this::newNodeProxy );
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        return new MonoDirectionalTraversalDescription( statementContext );
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        return new BidirectionalTraversalDescriptionImpl( statementContext );
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
    public File getStoreDir()
    {
        return spi.storeDir();
    }

    @Override
    public String toString()
    {
        return spi.name() + " [" + getStoreDir() + "]";
    }

    @Override
    public Statement statement()
    {
        return statementContext.get();
    }

    @Override
    public KernelTransaction kernelTransaction()
    {
        return statementContext.getKernelTransactionBoundToThisThread( true );
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return this;
    }

    @Override
    public void assertInUnterminatedTransaction()
    {
        statementContext.assertInUnterminatedTransaction();
    }

    @Override
    public void failTransaction()
    {
        statementContext.getKernelTransactionBoundToThisThread( true ).failure();
    }

    @Override
    public RelationshipProxy newRelationshipProxy( long id )
    {
        return new RelationshipProxy( this, id );
    }

    @Override
    public RelationshipProxy newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
    {
        return new RelationshipProxy( this, id, startNodeId, typeId, endNodeId );
    }

    @Override
    public NodeProxy newNodeProxy( long nodeId )
    {
        return new NodeProxy( this, nodeId );
    }

    @Override
    public RelationshipType getRelationshipTypeById( int type )
    {
        try
        {
            return relationshipTypeTokenHolder.getTokenById( type );
        }
        catch ( TokenNotFoundException e )
        {
            throw new IllegalStateException( "Kernel API returned non-existent relationship type: " + type );
        }
    }

    @Override
    public int getRelationshipTypeIdByName( String typeName )
    {
        return relationshipTypeTokenHolder.getIdByName( typeName );
    }

    @Override
    public GraphPropertiesProxy newGraphPropertiesProxy()
    {
        return new GraphPropertiesProxy( this );
    }

    private static class NodeLabelPropertyIterator extends PrefetchingNodeResourceIterator
    {
        private final Read read;
        private final NodeLabelIndexCursor nodeLabelCursor;
        private final NodeCursor nodeCursor;
        private final PropertyCursor propertyCursor;
        private final int propertyKeyId;
        private final Value value;

        NodeLabelPropertyIterator(
                Read read,
                NodeLabelIndexCursor nodeLabelCursor,
                NodeCursor nodeCursor,
                PropertyCursor propertyCursor,
                Statement statement,
                NodeFactory nodeFactory,
                int propertyKeyId,
                Value value )
        {
            super( statement, nodeFactory );
            this.read = read;
            this.nodeLabelCursor = nodeLabelCursor;
            this.nodeCursor = nodeCursor;
            this.propertyCursor = propertyCursor;
            this.propertyKeyId = propertyKeyId;
            this.value = value;
        }

        @Override
        protected long fetchNext()
        {
            boolean hasNext;
            do
            {
                hasNext = nodeLabelCursor.next();

            } while ( hasNext && !hasPropertyWithValue() );

            if ( hasNext )
            {
                return nodeLabelCursor.nodeReference();
            }
            else
            {
                close();
                return NO_ID;
            }
        }

        @Override
        void closeResources( Statement statement )
        {
            IOUtils.closeAllSilently( statement, nodeLabelCursor, nodeCursor, propertyCursor );
        }

        private boolean hasPropertyWithValue()
        {
            read.singleNode( nodeLabelCursor.nodeReference(), nodeCursor );
            if ( nodeCursor.next() )
            {
                nodeCursor.properties( propertyCursor );
                while ( propertyCursor.next() )
                {
                    if ( propertyCursor.propertyKey() == propertyKeyId && propertyCursor.propertyValue().equals( value ) )
                    {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private void assertTransactionOpen()
    {
        assertTransactionOpen( statementContext.getKernelTransactionBoundToThisThread( true ) );
    }

    private void assertTransactionOpen( KernelTransaction transaction )
    {
        if ( transaction.isTerminated() )
        {
            Status terminationReason = transaction.getReasonIfTerminated().orElse( Status.Transaction.Terminated );
            throw new TransactionTerminatedException( terminationReason );
        }
    }

    private static final class NodeCursorResourceIterator<CURSOR extends NodeIndexCursor> extends PrefetchingNodeResourceIterator
    {
        private final CURSOR cursor;

        NodeCursorResourceIterator( CURSOR cursor, Statement statement, NodeFactory nodeFactory )
        {
            super( statement, nodeFactory );
            this.cursor = cursor;
        }

        long fetchNext()
        {
            if ( cursor.next() )
            {
                return cursor.nodeReference();
            }
            else
            {
                close();
                return NO_ID;
            }
        }

        @Override
        void closeResources( Statement statement )
        {
            IOUtils.closeAllSilently( statement, cursor );
        }
    }

    private abstract static class PrefetchingNodeResourceIterator implements ResourceIterator<Node>
    {
        private final Statement statement;
        private final NodeFactory nodeFactory;
        private long next;
        private boolean closed;

        private static final long NOT_INITIALIZED = -2L;
        protected static final long NO_ID = -1L;

        PrefetchingNodeResourceIterator( Statement statement, NodeFactory nodeFactory )
        {
            this.statement = statement;
            this.nodeFactory = nodeFactory;
            this.next = NOT_INITIALIZED;
        }

        @Override
        public boolean hasNext()
        {
            if ( next == NOT_INITIALIZED )
            {
                next = fetchNext();
            }
            return next != NO_ID;
        }

        @Override
        public Node next()
        {
            if ( !hasNext() )
            {
                close();
                throw new NoSuchElementException(  );
            }
            Node nodeProxy = nodeFactory.make( next );
            next = fetchNext();
            return nodeProxy;
        }

        public void close()
        {
            if ( !closed )
            {
                next = NO_ID;
                closeResources( statement );
                closed = true;
            }
        }

        abstract long fetchNext();

        abstract void closeResources( Statement statement );
    }

    private interface NodeFactory
    {
        NodeProxy make( long id );
    }
}
