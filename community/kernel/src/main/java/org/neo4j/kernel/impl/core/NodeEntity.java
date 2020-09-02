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
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TokenCapacityExceededKernelException;
import org.neo4j.internal.kernel.api.helpers.Nodes;
import org.neo4j.internal.kernel.api.helpers.RelationshipFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingIterator;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

public class NodeEntity implements Node, RelationshipFactory<Relationship>
{
    public static final long SHALLOW_SIZE = shallowSizeOfInstance( NodeEntity.class );

    private final InternalTransaction internalTransaction;
    private final long nodeId;

    public NodeEntity( InternalTransaction internalTransaction, long nodeId )
    {
        this.internalTransaction = internalTransaction;
        this.nodeId = nodeId;
    }

    public static boolean isDeletedInCurrentTransaction( Node node )
    {
        if ( node instanceof NodeEntity )
        {
            NodeEntity proxy = (NodeEntity) node;
            KernelTransaction ktx = proxy.internalTransaction.kernelTransaction();
            return ktx.dataRead().nodeDeletedInTransaction( proxy.nodeId );
        }
        return false;
    }

    @Override
    public long getId()
    {
        return nodeId;
    }

    @Override
    public void delete()
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        try
        {
            boolean deleted = transaction.dataWrite().nodeDelete( getId() );
            if ( !deleted )
            {
                throw new NotFoundException( "Unable to delete Node[" + nodeId +
                                             "] since it has already been deleted." );
            }
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public ResourceIterable<Relationship> getRelationships()
    {
        return getRelationships( Direction.BOTH );
    }

    @Override
    public ResourceIterable<Relationship> getRelationships( final Direction direction )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        return innerGetRelationships( transaction, direction, null );
    }

    @Override
    public ResourceIterable<Relationship> getRelationships( RelationshipType... types )
    {
        return getRelationships( Direction.BOTH, types );
    }

    @Override
    public ResourceIterable<Relationship> getRelationships( final Direction direction, RelationshipType... types )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int[] typeIds = relTypeIds( types, transaction.tokenRead() );
        return innerGetRelationships( transaction, direction, typeIds );
    }

    private ResourceIterable<Relationship> innerGetRelationships(
            KernelTransaction transaction, final Direction direction, int[] typeIds )
    {
        return () -> getRelationshipSelectionIterator( transaction, direction, typeIds );
    }

    @Override
    public boolean hasRelationship()
    {
        return hasRelationship( Direction.BOTH );
    }

    @Override
    public boolean hasRelationship( Direction direction )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        return innerHasRelationships( transaction, direction, null );
    }

    @Override
    public boolean hasRelationship( RelationshipType... types )
    {
        return hasRelationship( Direction.BOTH, types );
    }

    @Override
    public boolean hasRelationship( Direction direction, RelationshipType... types )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int[] typeIds = relTypeIds( types, transaction.tokenRead() );
        return innerHasRelationships( transaction, direction, typeIds );
    }

    private boolean innerHasRelationships( final KernelTransaction transaction, final Direction direction, int[] typeIds )
    {
        try ( ResourceIterator<Relationship> iterator =
                      getRelationshipSelectionIterator( transaction, direction, typeIds ) )
        {
            return iterator.hasNext();
        }
    }

    @Override
    public Relationship getSingleRelationship( RelationshipType type, Direction dir )
    {
        try ( ResourceIterator<Relationship> rels = getRelationships( dir, type ).iterator() )
        {
            if ( !rels.hasNext() )
            {
                return null;
            }

            Relationship rel = rels.next();
            while ( rels.hasNext() )
            {
                Relationship other = rels.next();
                if ( !other.equals( rel ) )
                {
                    throw new NotFoundException( "More than one relationship[" +
                                                 type + ", " + dir + "] found for " + this );
                }
            }
            return rel;
        }
    }

    @Override
    public void setProperty( String key, Object value )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int propertyKeyId;
        try
        {
            propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( key );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( format( "Invalid property key '%s'.", key ), e );
        }
        catch ( TokenCapacityExceededKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( KernelException e )
        {
            throw new TransactionFailureException( "Unknown error trying to create property key token", e );
        }

        try
        {
            transaction.dataWrite().nodeSetProperty( nodeId, propertyKeyId, Values.of( value, false ) );
        }
        catch ( ConstraintValidationException e )
        {
            throw new ConstraintViolationException( e.getUserMessage( transaction.tokenRead() ), e );
        }
        catch ( IllegalArgumentException e )
        {
            try
            {
                transaction.rollback();
            }
            catch ( org.neo4j.internal.kernel.api.exceptions.TransactionFailureException ex )
            {
                ex.addSuppressed( e );
                throw new TransactionFailureException( "Fail to rollback transaction.", ex );
            }
            throw e;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( KernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Object removeProperty( String key ) throws NotFoundException
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int propertyKeyId;
        try
        {
            propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( key );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( format( "Invalid property key '%s'.", key ), e );
        }
        catch ( KernelException e )
        {
            throw new TransactionFailureException( "Unknown error trying to get property key token", e );
        }

        try
        {
            return transaction.dataWrite().nodeRemoveProperty( nodeId, propertyKeyId ).asObjectCopy();
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            return defaultValue;
        }
        singleNode( transaction, nodes );
        nodes.properties( properties );

        return properties.seekProperty( propertyKey ) ? properties.propertyValue().asObjectCopy() : defaultValue;
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        List<String> keys = new ArrayList<>();
        try
        {
            NodeCursor nodes = transaction.ambientNodeCursor();
            PropertyCursor properties = transaction.ambientPropertyCursor();
            singleNode( transaction, nodes );
            TokenRead token = transaction.tokenRead();
            nodes.properties( properties );
            while ( properties.next() )
            {
                keys.add( token.propertyKeyName( properties.propertyKey() ) );
            }
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist.", e );
        }
        return keys;
    }

    @Override
    public Map<String,Object> getProperties( String... keys )
    {
        Objects.requireNonNull( keys, "Properties keys should be not null array." );

        if ( keys.length == 0 )
        {
            return Collections.emptyMap();
        }

        KernelTransaction transaction = internalTransaction.kernelTransaction();

        int itemsToReturn = keys.length;
        Map<String,Object> properties = new HashMap<>( itemsToReturn );
        TokenRead token = transaction.tokenRead();

        //Find ids, note we are betting on that the number of keys
        //is small enough not to use a set here.
        int[] propertyIds = new int[itemsToReturn];
        for ( int i = 0; i < itemsToReturn; i++ )
        {
            String key = keys[i];
            if ( key == null )
            {
                throw new NullPointerException( String.format( "Key %d was null", i ) );
            }
            propertyIds[i] = token.propertyKey( key );
        }

        NodeCursor nodes = transaction.ambientNodeCursor();
        PropertyCursor propertyCursor = transaction.ambientPropertyCursor();
        singleNode( transaction, nodes );
        nodes.properties( propertyCursor );
        int propertiesToFind = itemsToReturn;
        while ( propertiesToFind > 0 && propertyCursor.next() )
        {
            //Do a linear check if this is a property we are interested in.
            int currentKey = propertyCursor.propertyKey();
            for ( int i = 0; i < itemsToReturn; i++ )
            {
                if ( propertyIds[i] == currentKey )
                {
                    properties.put( keys[i],
                                    propertyCursor.propertyValue().asObjectCopy() );
                    propertiesToFind--;
                    break;
                }
            }
        }
        return properties;
    }

    @Override
    public Map<String,Object> getAllProperties()
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        return getAllProperties( transaction.ambientNodeCursor(), transaction.ambientPropertyCursor() );
    }

    public Map<String,Object> getAllProperties( NodeCursor nodes, PropertyCursor propertyCursor )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        Map<String,Object> properties = new HashMap<>();

        try
        {
            TokenRead token = transaction.tokenRead();
            if ( nodes.isClosed() || nodes.nodeReference() != getId() )
            {
                singleNode( transaction, nodes );
            }
            nodes.properties( propertyCursor );
            while ( propertyCursor.next() )
            {
                properties.put( token.propertyKeyName( propertyCursor.propertyKey() ),
                                propertyCursor.propertyValue().asObjectCopy() );
            }
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist.", e );
        }
        return properties;
    }

    @Override
    public Object getProperty( String key ) throws NotFoundException
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            throw new NotFoundException( format( "No such property, '%s'.", key ) );
        }

        NodeCursor nodes = transaction.ambientNodeCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        singleNode( transaction, nodes );
        nodes.properties( properties );
        if ( !properties.seekProperty( propertyKey ) )
        {
            throw new NotFoundException( format( "No such property, '%s'.", key ) );
        }
        return properties.propertyValue().asObjectCopy();
    }

    @Override
    public boolean hasProperty( String key )
    {
        if ( null == key )
        {
            return false;
        }

        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            return false;
        }

        NodeCursor nodes = transaction.ambientNodeCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        singleNode( transaction, nodes );
        nodes.properties( properties );
        return properties.seekProperty( propertyKey );
    }

    public int compareTo( Object node )
    {
        Node n = (Node) node;
        return Long.compare( this.getId(), n.getId() );
    }

    @Override
    public boolean equals( Object o )
    {
        return o instanceof Node && this.getId() == ((Node) o).getId();
    }

    @Override
    public int hashCode()
    {
        return (int) ((nodeId >>> 32) ^ nodeId);
    }

    @Override
    public String toString()
    {
        return "Node[" + this.getId() + "]";
    }

    @Override
    public Relationship createRelationshipTo( Node otherNode, RelationshipType type )
    {
        if ( otherNode == null )
        {
            throw new IllegalArgumentException( "Other node is null." );
        }

        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int relationshipTypeId;
        try
        {
            relationshipTypeId = transaction.tokenWrite().relationshipTypeGetOrCreateForName( type.name() );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( e );
        }
        catch ( TokenCapacityExceededKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( KernelException e )
        {
            throw new TransactionFailureException( "Unknown error trying to create relationship type token", e );
        }

        try
        {
            long relationshipId = transaction.dataWrite().relationshipCreate( nodeId, relationshipTypeId, otherNode.getId() );
            return internalTransaction.newRelationshipEntity( relationshipId, nodeId, relationshipTypeId, otherNode.getId() );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node[" + e.entityId() + "] is deleted and cannot be used to create a relationship" );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public void addLabel( Label label )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int labelId;
        try
        {
            labelId = transaction.tokenWrite().labelGetOrCreateForName( label.name() );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new ConstraintViolationException( format( "Invalid label name '%s'.", label.name() ), e );
        }
        catch ( TokenCapacityExceededKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( KernelException e )
        {
            throw new TransactionFailureException( "Unknown error trying to create label token", e );
        }

        try
        {
            transaction.dataWrite().nodeAddLabel( getId(), labelId );
        }
        catch ( ConstraintValidationException e )
        {
            throw new ConstraintViolationException( e.getUserMessage( transaction.tokenRead() ), e );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "No node with id " + getId() + " found.", e );
        }
        catch ( KernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public void removeLabel( Label label )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        try
        {
            int labelId = transaction.tokenRead().nodeLabel( label.name() );
            if ( labelId != TokenRead.NO_TOKEN )
            {
                transaction.dataWrite().nodeRemoveLabel( getId(), labelId );
            }
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "No node with id " + getId() + " found.", e );
        }
        catch ( KernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public boolean hasLabel( Label label )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        int labelId = transaction.tokenRead().nodeLabel( label.name() );
        if ( labelId == NO_SUCH_LABEL )
        {
            return false;
        }
        transaction.dataRead().singleNode( nodeId, nodes );
        return nodes.next() && nodes.hasLabel( labelId );
    }

    @Override
    public Iterable<Label> getLabels()
    {
        NodeCursor nodes = internalTransaction.kernelTransaction().ambientNodeCursor();
        return getLabels( nodes );
    }

    public Iterable<Label> getLabels( NodeCursor nodes )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        try
        {
            singleNode( transaction, nodes );
            TokenSet tokenSet = nodes.labels();
            TokenRead tokenRead = transaction.tokenRead();
            List<Label> list = new ArrayList<>( tokenSet.numberOfTokens() );
            for ( int i = 0; i < tokenSet.numberOfTokens(); i++ )
            {
                list.add( label( tokenRead.nodeLabelName( tokenSet.token( i ) ) ) );
            }
            return list;
        }
        catch ( LabelNotFoundKernelException e )
        {
            throw new IllegalStateException( "Label retrieved through kernel API should exist.", e );
        }
    }

    public InternalTransaction getTransaction()
    {
        return internalTransaction;
    }

    @Override
    public int getDegree()
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        singleNode( transaction, nodes );

        return Nodes.countAll( nodes );
    }

    @Override
    public int getDegree( RelationshipType type )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int typeId = transaction.tokenRead().relationshipType( type.name() );
        if ( typeId == NO_TOKEN )
        {   // This type doesn't even exist. Return 0
            return 0;
        }

        NodeCursor nodes = transaction.ambientNodeCursor();
        singleNode( transaction, nodes );
        return Nodes.countAll( nodes, typeId );
    }

    @Override
    public int getDegree( Direction direction )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();

        NodeCursor nodes = transaction.ambientNodeCursor();
        singleNode( transaction, nodes );
        switch ( direction )
        {
        case OUTGOING:
            return Nodes.countOutgoing( nodes );
        case INCOMING:
            return Nodes.countIncoming( nodes );
        case BOTH:
            return Nodes.countAll( nodes );
        default:
            throw new IllegalStateException( "Unknown direction " + direction );
        }
    }

    @Override
    public int getDegree( RelationshipType type, Direction direction )
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        int typeId = transaction.tokenRead().relationshipType( type.name() );
        if ( typeId == NO_TOKEN )
        {   // This type doesn't even exist. Return 0
            return 0;
        }

        NodeCursor nodes = transaction.ambientNodeCursor();
        singleNode( transaction, nodes );
        switch ( direction )
        {
        case OUTGOING:
            return Nodes.countOutgoing( nodes, typeId );
        case INCOMING:
            return Nodes.countIncoming( nodes, typeId );
        case BOTH:
            return Nodes.countAll( nodes, typeId );
        default:
            throw new IllegalStateException( "Unknown direction " + direction );
        }
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        try
        {
            NodeCursor nodes = transaction.ambientNodeCursor();
            TokenRead tokenRead = transaction.tokenRead();
            singleNode( transaction, nodes );
            Degrees degrees = nodes.degrees( ALL_RELATIONSHIPS );
            List<RelationshipType> types = new ArrayList<>();
            for ( int type : degrees.types() )
            {
                // only include this type if there are any relationships with this type
                if ( degrees.totalDegree( type ) > 0 )
                {
                    types.add( RelationshipType.withName( tokenRead.relationshipTypeName( type ) ) );
                }
            }

            return types;
        }
        catch ( KernelException e )
        {
            throw new NotFoundException( "Relationship name not found.", e );
        }
    }

    private ResourceIterator<Relationship> getRelationshipSelectionIterator(
            KernelTransaction transaction, Direction direction, int[] typeIds )
    {
        NodeCursor node = transaction.ambientNodeCursor();
        transaction.dataRead().singleNode( getId(), node );
        if ( !node.next() )
        {
            throw new NotFoundException( format( "Node %d not found", nodeId ) );
        }

        var cursorTracer = transaction.pageCursorTracer();
        var cursors = transaction.cursors();
        switch ( direction )
        {
        case OUTGOING:
            return outgoingIterator( cursors, node, typeIds, this, cursorTracer );
        case INCOMING:
            return incomingIterator( cursors, node, typeIds, this, cursorTracer );
        case BOTH:
            return allIterator( cursors, node, typeIds, this, cursorTracer );
        default:
            throw new IllegalStateException( "Unknown direction " + direction );
        }
    }

    private int[] relTypeIds( RelationshipType[] types, TokenRead token )
    {
        int[] ids = new int[types.length];
        int outIndex = 0;
        for ( RelationshipType type : types )
        {
            int id = token.relationshipType( type.name() );
            if ( id != NO_SUCH_RELATIONSHIP_TYPE )
            {
                ids[outIndex++] = id;
            }
        }

        if ( outIndex != ids.length )
        {
            // One or more relationship types do not exist, so we can exclude them right away.
            ids = Arrays.copyOf( ids, outIndex );
        }
        return ids;
    }

    private void singleNode( KernelTransaction transaction, NodeCursor nodes )
    {
        transaction.dataRead().singleNode( nodeId, nodes );
        if ( !nodes.next() )
        {
            throw new NotFoundException( new EntityNotFoundException( EntityType.NODE, nodeId ) );
        }
    }

    @Override
    public Relationship relationship( long id, long startNodeId, int typeId, long endNodeId )
    {
        return internalTransaction.newRelationshipEntity( id, startNodeId, typeId, endNodeId );
    }
}
