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
package org.neo4j.kernel.impl.coreapi;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.StringSearchMode;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.coreapi.internal.NodeCursorResourceIterator;
import org.neo4j.kernel.impl.coreapi.internal.NodeLabelPropertyIterator;
import org.neo4j.kernel.impl.coreapi.internal.RelationshipCursorResourceIterator;
import org.neo4j.kernel.impl.coreapi.internal.RelationshipTypePropertyIterator;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.traversal.BidirectionalTraversalDescriptionImpl;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static org.neo4j.internal.helpers.collection.Iterators.emptyResourceIterator;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;
import static org.neo4j.util.Preconditions.checkArgument;
import static org.neo4j.values.storable.Values.utf8Value;

/**
 * Default implementation of {@link org.neo4j.graphdb.Transaction}
 */
public class TransactionImpl extends EntityValidationTransactionImpl
{
    private static final EntityLocker locker = new EntityLocker();
    private final TokenHolders tokenHolders;
    private final TransactionalContextFactory contextFactory;
    private final DatabaseAvailabilityGuard availabilityGuard;
    private final QueryExecutionEngine executionEngine;
    private final Consumer<Status> terminationCallback;
    private final TransactionExceptionMapper exceptionMapper;
    private KernelTransaction transaction;
    private boolean closed;
    private List<TransactionClosedCallback> closeCallbacks;

    public TransactionImpl( TokenHolders tokenHolders, TransactionalContextFactory contextFactory,
            DatabaseAvailabilityGuard availabilityGuard, QueryExecutionEngine executionEngine,
            KernelTransaction transaction, Consumer<Status> terminationCallback,
            TransactionExceptionMapper exceptionMapper )
    {
        this.tokenHolders = tokenHolders;
        this.contextFactory = contextFactory;
        this.availabilityGuard = availabilityGuard;
        this.executionEngine = executionEngine;
        this.terminationCallback = terminationCallback;
        this.exceptionMapper = exceptionMapper;
        setTransaction( transaction );
    }

    @Override
    public void commit()
    {
        safeTerminalOperation( KernelTransaction::commit );
    }

    @Override
    public void rollback()
    {
        if ( isOpen() )
        {
            safeTerminalOperation( KernelTransaction::rollback );
        }
    }

    @Override
    public Node createNode()
    {
        var ktx = kernelTransaction();
        try
        {
            return newNodeEntity( ktx.dataWrite().nodeCreate() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Node createNode( Label... labels )
    {
        var ktx = kernelTransaction();
        try
        {
            TokenWrite tokenWrite = ktx.tokenWrite();
            int[] labelIds = new int[labels.length];
            String[] labelNames = new String[labels.length];
            for ( int i = 0; i < labelNames.length; i++ )
            {
                labelNames[i] = labels[i].name();
            }
            tokenWrite.labelGetOrCreateForNames( labelNames, labelIds );

            Write write = ktx.dataWrite();
            long nodeId = write.nodeCreateWithLabels( labelIds );
            return newNodeEntity( nodeId );
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

        KernelTransaction ktx = kernelTransaction();
        if ( !ktx.dataRead().nodeExists( id ) )
        {
            throw new NotFoundException( format( "Node %d not found", id ),
                    new EntityNotFoundException( EntityType.NODE, id ) );
        }
        return newNodeEntity( id );
    }

    @Override
    public Result execute( String query ) throws QueryExecutionException
    {
        return execute( query, emptyMap() );
    }

    @Override
    public Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        return execute( this, query, ValueUtils.asParameterMapValue( parameters ) );
    }

    private Result execute( InternalTransaction transaction, String query, MapValue parameters )
            throws QueryExecutionException
    {
        checkInTransaction();
        TransactionalContext context = contextFactory.newContext( transaction, query, parameters );
        try
        {
            availabilityGuard.assertDatabaseAvailable();
            return executionEngine.executeQuery( query, parameters, context, false );
        }
        catch ( UnavailableException ue )
        {
            throw new org.neo4j.graphdb.TransactionFailureException( ue.getMessage(), ue );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
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

        KernelTransaction ktx = kernelTransaction();

        if ( !ktx.dataRead().relationshipExists( id ) )
        {
            throw new NotFoundException( format( "Relationship %d not found", id ),
                    new EntityNotFoundException( EntityType.RELATIONSHIP, id ) );
        }
        return newRelationshipEntity( id );
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription()
    {
        checkInTransaction();
        return new BidirectionalTraversalDescriptionImpl();
    }

    @Override
    public TraversalDescription traversalDescription()
    {
        checkInTransaction();
        return new MonoDirectionalTraversalDescription();
    }

    @Override
    public Iterable<Label> getAllLabelsInUse()
    {
        return allInUse( TokenAccess.LABELS );
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypesInUse()
    {
        return allInUse( TokenAccess.RELATIONSHIP_TYPES );
    }

    @Override
    public Iterable<Label> getAllLabels()
    {
        return all( TokenAccess.LABELS );
    }

    @Override
    public Iterable<RelationshipType> getAllRelationshipTypes()
    {
        return all( TokenAccess.RELATIONSHIP_TYPES );
    }

    @Override
    public Iterable<String> getAllPropertyKeys()
    {
        return all( TokenAccess.PROPERTY_KEYS );
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
        checkLabel( myLabel );
        return allNodesWithLabel( myLabel );
    }

    @Override
    public ResourceIterator<Node> findNodes( final Label myLabel, final String key, final Object value )
    {
        checkLabel( myLabel );
        checkPropertyKey( key );
        KernelTransaction transaction = kernelTransaction();
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel( myLabel.name() );
        int propertyId = tokenRead.propertyKey( key );
        return nodesByLabelAndProperty( transaction, labelId, IndexQuery.exact( propertyId, Values.of( value, false ) ) );
    }

    @Override
    public ResourceIterator<Node> findNodes(
            final Label myLabel, final String key, final String value, final StringSearchMode searchMode )
    {
        checkLabel( myLabel );
        checkPropertyKey( key );
        checkArgument( value != null, "Template must not be null" );
        KernelTransaction transaction = kernelTransaction();
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel( myLabel.name() );
        int propertyId = tokenRead.propertyKey( key );
        IndexQuery query;
        switch ( searchMode )
        {
        case EXACT:
            query = IndexQuery.exact( propertyId, utf8Value( value.getBytes( UTF_8 ) ) );
            break;
        case PREFIX:
            query = IndexQuery.stringPrefix( propertyId, utf8Value( value.getBytes( UTF_8 ) ) );
            break;
        case SUFFIX:
            query = IndexQuery.stringSuffix( propertyId, utf8Value( value.getBytes( UTF_8 ) ) );
            break;
        case CONTAINS:
            query = IndexQuery.stringContains( propertyId, utf8Value( value.getBytes( UTF_8 ) ) );
            break;
        default:
            throw new IllegalStateException( "Unknown string search mode: " + searchMode );
        }
        return nodesByLabelAndProperty( transaction, labelId, query );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2,
            String key3, Object value3 )
    {
        checkLabel( label );
        checkPropertyKey( key1 );
        checkPropertyKey( key2 );
        checkPropertyKey( key3 );
        KernelTransaction transaction = kernelTransaction();
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel( label.name() );
        return nodesByLabelAndProperties( transaction, labelId,
                                          IndexQuery.exact( tokenRead.propertyKey( key1 ), Values.of( value1, false ) ),
                                          IndexQuery.exact( tokenRead.propertyKey( key2 ), Values.of( value2, false ) ),
                                          IndexQuery.exact( tokenRead.propertyKey( key3 ), Values.of( value3, false ) ) );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, String key1, Object value1, String key2, Object value2 )
    {
        checkLabel( label );
        checkPropertyKey( key1 );
        checkPropertyKey( key2 );
        KernelTransaction transaction = kernelTransaction();
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel( label.name() );
        return nodesByLabelAndProperties( transaction, labelId,
                                          IndexQuery.exact( tokenRead.propertyKey( key1 ), Values.of( value1, false ) ),
                                          IndexQuery.exact( tokenRead.propertyKey( key2 ), Values.of( value2, false ) ) );
    }

    @Override
    public ResourceIterator<Node> findNodes( Label label, Map<String,Object> propertyValues )
    {
        checkLabel( label );
        checkArgument( propertyValues != null, "Property values can not be null" );
        KernelTransaction transaction = kernelTransaction();
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel( label.name() );
        IndexQuery.ExactPredicate[] queries = new IndexQuery.ExactPredicate[propertyValues.size()];
        int i = 0;
        for ( Map.Entry<String,Object> entry : propertyValues.entrySet() )
        {
            queries[i++] = IndexQuery.exact( tokenRead.propertyKey( entry.getKey() ), Values.of( entry.getValue(), false ) );
        }
        return nodesByLabelAndProperties( transaction, labelId, queries );
    }

    @Override
    public ResourceIterable<Node> getAllNodes()
    {
        KernelTransaction ktx = kernelTransaction();
        return () ->
        {
            NodeCursor cursor = ktx.cursors().allocateNodeCursor( ktx.pageCursorTracer() );
            ktx.dataRead().allNodesScan( cursor );
            return new PrefetchingResourceIterator<>()
            {
                @Override
                protected Node fetchNextOrNull()
                {
                    if ( cursor.next() )
                    {
                        return newNodeEntity( cursor.nodeReference() );
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
                }
            };
        };
    }

    @Override
    public Relationship findRelationship( RelationshipType relationshipType, String key, Object value )
    {
        try ( var iterator = findRelationships( relationshipType, key, value ) )
        {
            if ( !iterator.hasNext() )
            {
                return null;
            }
            var rel = iterator.next();
            if ( iterator.hasNext() )
            {
                throw new MultipleFoundException(
                        format( "Found multiple relationships with type: '%s', property name: '%s' and property " +
                                "value: '%s' while only one was expected.", relationshipType, key, value ) );
            }
            return rel;
        }
    }

    @Override
    public ResourceIterator<Relationship> findRelationships( RelationshipType relationshipType, String key, Object value )
    {
        checkRelationshipType( relationshipType );
        checkPropertyKey( key );
        KernelTransaction transaction = kernelTransaction();
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.relationshipType( relationshipType.name() );
        int propertyId = tokenRead.propertyKey( key );
        return relationshipsByTypeAndProperty( transaction, labelId, IndexQuery.exact( propertyId, Values.of( value, false ) ) );
    }

    @Override
    public ResourceIterator<Relationship> findRelationships( RelationshipType relationshipType )
    {
        return allRelationshipsWithType( relationshipType );
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships()
    {
        KernelTransaction ktx = kernelTransaction();
        return () ->
        {
            RelationshipScanCursor cursor = ktx.cursors().allocateRelationshipScanCursor( ktx.pageCursorTracer() );
            ktx.dataRead().allRelationshipsScan( cursor );
            return new PrefetchingResourceIterator<>()
            {
                @Override
                protected Relationship fetchNextOrNull()
                {
                    if ( cursor.next() )
                    {
                        return newRelationshipEntity( cursor.relationshipReference(), cursor.sourceNodeReference(), cursor.type(),
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
                }
            };
        };
    }

    @Override
    public final void terminate()
    {
        terminate( Terminated );
    }

    @Override
    public void terminate( Status reason )
    {
        var ktx = transaction;
        if ( ktx == null )
        {
            return;
        }
        ktx.markForTermination( reason );
        if ( terminationCallback != null )
        {
            terminationCallback.accept( reason );
        }
    }

    @Override
    public UUID getDatabaseId()
    {
        if ( this.transaction != null )
        {
            return this.transaction.getDatabaseId();
        }
        else
        {
            return null;
        }
    }

    @Override
    public String getDatabaseName()
    {
        if ( this.transaction != null )
        {
            return this.transaction.getDatabaseName();
        }
        else
        {
            return null;
        }
    }

    @Override
    public void close()
    {
        if ( isOpen() )
        {
            safeTerminalOperation( KernelTransaction::close );
        }
    }

    @Override
    public void addCloseCallback( TransactionClosedCallback callback )
    {
        if ( closeCallbacks == null )
        {
            closeCallbacks = new ArrayList<>( 4 );
        }
        closeCallbacks.add( callback );
    }

    private void safeTerminalOperation( TransactionalOperation operation )
    {
        try
        {
            if ( closed )
            {
                throw new NotInTransactionException( "The transaction has been closed." );
            }
            operation.perform( transaction );
            closed = true;
            transaction = null;

            if ( closeCallbacks != null )
            {
                closeCallbacks.forEach( TransactionClosedCallback::transactionClosed );
            }
        }
        catch ( Exception e )
        {
            throw exceptionMapper.mapException( e );
        }
    }

    @Override
    public void setTransaction( KernelTransaction transaction )
    {
        this.transaction = transaction;
        transaction.bindToUserTransaction( this );
    }

    @Override
    public Lock acquireWriteLock( Entity entity )
    {
        return locker.exclusiveLock( kernelTransaction(), entity );
    }

    @Override
    public Lock acquireReadLock( Entity entity )
    {
        return locker.sharedLock( kernelTransaction(), entity );
    }

    @Override
    public KernelTransaction kernelTransaction()
    {
        checkInTransaction();
        return transaction;
    }

    @Override
    public KernelTransaction.Type transactionType()
    {
        return kernelTransaction().transactionType();
    }

    @Override
    public SecurityContext securityContext()
    {
        return kernelTransaction().securityContext();
    }

    @Override
    public ClientConnectionInfo clientInfo()
    {
        return kernelTransaction().clientInfo();
    }

    @Override
    public KernelTransaction.Revertable overrideWith( SecurityContext context )
    {
        return kernelTransaction().overrideWith( context );
    }

    @Override
    public Optional<Status> terminationReason()
    {
        var tx = transaction;
        return tx != null ? tx.getReasonIfTerminated() : Optional.empty();
    }

    @Override
    public void setMetaData( Map<String,Object> txMeta )
    {
        kernelTransaction().setMetaData( txMeta );
    }

    @Override
    public RelationshipEntity newRelationshipEntity( long id )
    {
        return new RelationshipEntity( this, id );
    }

    @Override
    public RelationshipEntity newRelationshipEntity( long id, long startNodeId, int typeId, long endNodeId )
    {
        return new RelationshipEntity( this, id, startNodeId, typeId, endNodeId );
    }

    @Override
    public NodeEntity newNodeEntity( long nodeId )
    {
        return new NodeEntity( this, nodeId );
    }

    @Override
    public RelationshipType getRelationshipTypeById( int type )
    {
        try
        {
            String name = tokenHolders.relationshipTypeTokens().getTokenById( type ).name();
            return RelationshipType.withName( name );
        }
        catch ( TokenNotFoundException e )
        {
            throw new IllegalStateException( "Kernel API returned non-existent relationship type: " + type );
        }
    }

    @Override
    public Schema schema()
    {
        return new SchemaImpl( kernelTransaction() );
    }

    private ResourceIterator<Node> nodesByLabelAndProperty( KernelTransaction transaction, int labelId, IndexQuery query )
    {
        Read read = transaction.dataRead();

        if ( query.propertyKeyId() == TokenRead.NO_TOKEN || labelId == TokenRead.NO_TOKEN )
        {
            return emptyResourceIterator();
        }

        var index = findMatchingIndex( transaction, SchemaDescriptor.forLabel( labelId, query.propertyKeyId() ) );
        if ( index != IndexDescriptor.NO_INDEX )
        {
            // Ha! We found an index - let's use it to find matching nodes
            try
            {
                NodeValueIndexCursor cursor = transaction.cursors().allocateNodeValueIndexCursor( transaction.pageCursorTracer(), transaction.memoryTracker() );
                IndexReadSession indexSession = read.indexReadSession( index );
                read.nodeIndexSeek( indexSession, cursor, unconstrained(), query );

                return new NodeCursorResourceIterator<>( cursor, this::newNodeEntity );
            }
            catch ( KernelException e )
            {
                // weird at this point but ignore and fallback to a label scan
            }
        }

        return getNodesByLabelAndPropertyWithoutIndex( labelId, query );
    }

    private ResourceIterator<Relationship> relationshipsByTypeAndProperty( KernelTransaction transaction, int typeId, IndexQuery query )
    {
        Read read = transaction.dataRead();

        if ( query.propertyKeyId() == TokenRead.NO_TOKEN || typeId == TokenRead.NO_TOKEN )
        {
            return emptyResourceIterator();
        }

        var index = findMatchingIndex( transaction, SchemaDescriptor.forRelType( typeId, query.propertyKeyId() ) );
        if ( index != IndexDescriptor.NO_INDEX )
        {
            // Ha! We found an index - let's use it to find matching relationships
            try
            {
                var cursor = transaction.cursors().allocateRelationshipValueIndexCursor( transaction.pageCursorTracer(), transaction.memoryTracker() );
                IndexReadSession indexSession = read.indexReadSession( index );
                read.relationshipIndexSeek( indexSession, cursor, unconstrained(), query );

                return new RelationshipCursorResourceIterator<>( cursor, this::newRelationshipEntity );
            }
            catch ( KernelException e )
            {
                // weird at this point but ignore and fallback to a type scan
            }
        }

        return getRelationshipsByTypeAndPropertyWithoutIndex( typeId, query );
    }

    @Override
    public void checkInTransaction()
    {
        if ( closed )
        {
            throw new NotInTransactionException( "The transaction has been closed." );
        }
        if ( transaction.isTerminated() )
        {
            Status terminationReason = transaction.getReasonIfTerminated().orElse( Status.Transaction.Terminated );
            throw new TransactionTerminatedException( terminationReason );
        }
    }

    @Override
    public boolean isOpen()
    {
        return !closed;
    }

    private ResourceIterator<Node> getNodesByLabelAndPropertyWithoutIndex( int labelId, IndexQuery... queries )
    {
        KernelTransaction transaction = kernelTransaction();

        NodeLabelIndexCursor nodeLabelCursor = transaction.cursors().allocateNodeLabelIndexCursor( transaction.pageCursorTracer() );
        NodeCursor nodeCursor = transaction.cursors().allocateNodeCursor( transaction.pageCursorTracer() );
        PropertyCursor propertyCursor = transaction.cursors().allocatePropertyCursor( transaction.pageCursorTracer(), transaction.memoryTracker() );

        transaction.dataRead().nodeLabelScan( labelId, nodeLabelCursor, IndexOrder.NONE );

        return new NodeLabelPropertyIterator( transaction.dataRead(),
                nodeLabelCursor,
                nodeCursor,
                propertyCursor,
                this::newNodeEntity,
                queries );
    }

    private ResourceIterator<Relationship> getRelationshipsByTypeAndPropertyWithoutIndex( int labelId, IndexQuery... queries )
    {
        KernelTransaction transaction = kernelTransaction();

        RelationshipTypeIndexCursor relationshipTypeIndexCursor = transaction.cursors().allocateRelationshipTypeIndexCursor( );
        RelationshipScanCursor relationshipScanCursor = transaction.cursors().allocateRelationshipScanCursor( transaction.pageCursorTracer() );
        PropertyCursor propertyCursor = transaction.cursors().allocatePropertyCursor( transaction.pageCursorTracer(), transaction.memoryTracker() );

        transaction.dataRead().relationshipTypeScan( labelId, relationshipTypeIndexCursor, IndexOrder.NONE );

        return new RelationshipTypePropertyIterator( transaction.dataRead(),
                                                     relationshipTypeIndexCursor,
                                                     relationshipScanCursor,
                                                     propertyCursor,
                                                     this::newRelationshipEntity,
                                                     queries );
    }

    private ResourceIterator<Node> nodesByLabelAndProperties(
            KernelTransaction transaction, int labelId, IndexQuery.ExactPredicate... queries )
    {
        Read read = transaction.dataRead();

        if ( isInvalidQuery( labelId, queries ) )
        {
            return emptyResourceIterator();
        }

        int[] propertyIds = getPropertyIds( queries );
        IndexDescriptor index = findMatchingIndex( transaction, labelId, propertyIds );

        if ( index != IndexDescriptor.NO_INDEX )
        {
            try
            {
                NodeValueIndexCursor cursor = transaction.cursors().allocateNodeValueIndexCursor( transaction.pageCursorTracer(), transaction.memoryTracker() );
                IndexReadSession indexSession = read.indexReadSession( index );
                read.nodeIndexSeek( indexSession, cursor, unconstrained(), getReorderedIndexQueries( index.schema().getPropertyIds(), queries ) );
                return new NodeCursorResourceIterator<>( cursor, this::newNodeEntity );
            }
            catch ( KernelException e )
            {
                // weird at this point but ignore and fallback to a label scan
            }
        }
        return getNodesByLabelAndPropertyWithoutIndex( labelId, queries );
    }

    private static IndexQuery[] getReorderedIndexQueries( int[] indexPropertyIds, IndexQuery[] queries )
    {
        IndexQuery[] orderedQueries = new IndexQuery[queries.length];
        for ( int i = 0; i < indexPropertyIds.length; i++ )
        {
            int propertyKeyId = indexPropertyIds[i];
            for ( IndexQuery query : queries )
            {
                if ( query.propertyKeyId() == propertyKeyId )
                {
                    orderedQueries[i] = query;
                    break;
                }
            }
        }
        return orderedQueries;
    }

    private ResourceIterator<Node> allNodesWithLabel( final Label myLabel )
    {
        KernelTransaction ktx = kernelTransaction();

        int labelId = ktx.tokenRead().nodeLabel( myLabel.name() );
        if ( labelId == TokenRead.NO_TOKEN )
        {
            return Iterators.emptyResourceIterator();
        }

        NodeLabelIndexCursor cursor = ktx.cursors().allocateNodeLabelIndexCursor( transaction.pageCursorTracer() );
        ktx.dataRead().nodeLabelScan( labelId, cursor, IndexOrder.NONE );
        return new NodeCursorResourceIterator<>( cursor, this::newNodeEntity );
    }

    private ResourceIterator<Relationship> allRelationshipsWithType( final RelationshipType type )
    {
        KernelTransaction ktx = kernelTransaction();

        int typeId = ktx.tokenRead().relationshipType( type.name() );
        if ( typeId == TokenRead.NO_TOKEN )
        {
            return Iterators.emptyResourceIterator();
        }

        RelationshipTypeIndexCursor cursor = ktx.cursors().allocateRelationshipTypeIndexCursor();
        ktx.dataRead().relationshipTypeScan( typeId, cursor, IndexOrder.NONE );
        return new RelationshipCursorResourceIterator<>( cursor, this::newRelationshipEntity );
    }

    private static IndexDescriptor findMatchingIndex( KernelTransaction transaction, int labelId, int[] propertyIds )
    {
        // Try a direct schema match first.
        Iterator<IndexDescriptor> iterator = transaction.schemaRead().index( SchemaDescriptor.forLabel( labelId, propertyIds ) );
        while ( iterator.hasNext() )
        {
            IndexDescriptor index = iterator.next();
            if ( index.getIndexType() == IndexType.BTREE )
            {
                return index;
            }
        }

        // Attempt to find matching index with different property order
        Arrays.sort( propertyIds );
        assertNoDuplicates( propertyIds, transaction.tokenRead() );

        int[] workingCopy = new int[propertyIds.length];

        Iterator<IndexDescriptor> indexes = transaction.schemaRead().indexesGetForLabel( labelId );
        while ( indexes.hasNext() )
        {
            IndexDescriptor index = indexes.next();
            int[] original = index.schema().getPropertyIds();
            if ( index.getIndexType() == IndexType.BTREE && hasSamePropertyIds( original, workingCopy, propertyIds ) )
            {
                // Ha! We found an index with the same properties in another order
                return index;
            }
        }

        // No dice.
        return IndexDescriptor.NO_INDEX;
    }

    private static IndexDescriptor findMatchingIndex( KernelTransaction transaction, SchemaDescriptor schemaDescriptor )
    {
        Iterator<IndexDescriptor> iterator = transaction.schemaRead().index( schemaDescriptor );
        while ( iterator.hasNext() )
        {
            IndexDescriptor index = iterator.next();
            if ( index.getIndexType() == IndexType.BTREE )
            {
                return index;
            }
        }
        return IndexDescriptor.NO_INDEX;
    }

    private static void assertNoDuplicates( int[] propertyIds, TokenRead tokenRead )
    {
        int prev = propertyIds[0];
        for ( int i = 1; i < propertyIds.length; i++ )
        {
            int curr = propertyIds[i];
            if ( curr == prev )
            {
                throw new IllegalArgumentException(
                        format( "Provided two queries for property %s. Only one query per property key can be performed",
                                tokenRead.propertyKeyGetName( curr ) ) );
            }
            prev = curr;
        }
    }

    private static boolean hasSamePropertyIds( int[] original, int[] workingCopy, int[] propertyIds )
    {
        if ( original.length == propertyIds.length )
        {
            System.arraycopy( original, 0, workingCopy, 0, original.length );
            Arrays.sort( workingCopy );
            return Arrays.equals( propertyIds, workingCopy );
        }
        return false;
    }

    private static int[] getPropertyIds( IndexQuery[] queries )
    {
        int[] propertyIds = new int[queries.length];
        for ( int i = 0; i < queries.length; i++ )
        {
            propertyIds[i] = queries[i].propertyKeyId();
        }
        return propertyIds;
    }

    private static boolean isInvalidQuery( int labelId, IndexQuery[] queries )
    {
        boolean invalidQuery = labelId == TokenRead.NO_TOKEN;
        for ( IndexQuery query : queries )
        {
            int propertyKeyId = query.propertyKeyId();
            invalidQuery = invalidQuery || propertyKeyId == TokenRead.NO_TOKEN;
        }
        return invalidQuery;
    }

    private <T> Iterable<T> allInUse( final TokenAccess<T> tokens )
    {
        var transaction = kernelTransaction();
        return () -> tokens.inUse( transaction );
    }

    private <T> Iterable<T> all( final TokenAccess<T> tokens )
    {
        var transaction = kernelTransaction();
        return () -> tokens.all( transaction );
    }

    @FunctionalInterface
    private interface TransactionalOperation
    {
        void perform( KernelTransaction transaction ) throws Exception;
    }

    private static void checkPropertyKey( String key )
    {
        checkArgument( key != null, "Property key can not be null" );
    }

    private static void checkLabel( Label label )
    {
        checkArgument( label != null, "Label can not be null" );
    }

    private static void checkRelationshipType( RelationshipType type )
    {
        checkArgument( type != null, "Relationship type can not be null" );
    }
}
