/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.internal.kernel.api.helpers.Nodes;
import org.neo4j.internal.kernel.api.helpers.RelationshipFactory;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingIterator;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_LABEL;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.core.TokenHolder.NO_ID;

public class NodeProxy implements Node, RelationshipFactory<Relationship>
{
    private final EmbeddedProxySPI spi;
    private final long nodeId;

    public NodeProxy( EmbeddedProxySPI spi, long nodeId )
    {
        this.nodeId = nodeId;
        this.spi = spi;
    }

    @Override
    public long getId()
    {
        return nodeId;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return spi.getGraphDatabase();
    }

    @Override
    public void delete()
    {
        KernelTransaction transaction = safeAcquireTransaction();
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
        catch ( AutoIndexingKernelException e )
        {
            throw new IllegalStateException( "Auto indexing encountered a failure while deleting the node: "
                                             + e.getMessage(), e );
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
        KernelTransaction transaction = safeAcquireTransaction();
        return innerGetRelationships( transaction, direction, null );
    }

    @Override
    public ResourceIterable<Relationship> getRelationships( RelationshipType... types )
    {
        return getRelationships( Direction.BOTH, types );
    }

    @Override
    public ResourceIterable<Relationship> getRelationships( RelationshipType type, Direction dir )
    {
        return getRelationships( dir, type );
    }

    @Override
    public ResourceIterable<Relationship> getRelationships( final Direction direction, RelationshipType... types )
    {
        KernelTransaction transaction = safeAcquireTransaction();
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
        KernelTransaction transaction = safeAcquireTransaction();
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
        KernelTransaction transaction = safeAcquireTransaction();
        int[] typeIds = relTypeIds( types, transaction.tokenRead() );
        return innerHasRelationships( transaction, direction, typeIds );
    }

    @Override
    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return hasRelationship( dir, type );
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
        KernelTransaction transaction = spi.kernelTransaction();
        int propertyKeyId;
        try
        {
            propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( key );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( format( "Invalid property key '%s'.", key ), e );
        }

        try ( Statement ignore = transaction.acquireStatement() )
        {
            transaction.dataWrite().nodeSetProperty( nodeId, propertyKeyId, Values.of( value, false ) );
        }
        catch ( ConstraintValidationException e )
        {
            throw new ConstraintViolationException(
                    e.getUserMessage( new SilentTokenNameLookup( transaction.tokenRead() ) ), e );
        }
        catch ( IllegalArgumentException e )
        {
            // Trying to set an illegal value is a critical error - fail this transaction
            spi.failTransaction();
            throw e;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( AutoIndexingKernelException e )
        {
            throw new IllegalStateException( "Auto indexing encountered a failure while setting property: "
                                             + e.getMessage(), e );
        }
        catch ( KernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Object removeProperty( String key ) throws NotFoundException
    {
        KernelTransaction transaction = spi.kernelTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( key );
            return transaction.dataWrite().nodeRemoveProperty( nodeId, propertyKeyId ).asObjectCopy();
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( format( "Invalid property key '%s'.", key ), e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( AutoIndexingKernelException e )
        {
            throw new IllegalStateException( "Auto indexing encountered a failure while removing property: "
                                             + e.getMessage(), e );
        }
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }
        KernelTransaction transaction = safeAcquireTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            return defaultValue;
        }
        singleNode( transaction, nodes );
        nodes.properties( properties );
        while ( properties.next() )
        {
            if ( propertyKey == properties.propertyKey() )
            {
                Value value = properties.propertyValue();
                return value == Values.NO_VALUE ? defaultValue : value.asObjectCopy();
            }
        }
        return defaultValue;
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        KernelTransaction transaction = safeAcquireTransaction();
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

        KernelTransaction transaction = safeAcquireTransaction();

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
        KernelTransaction transaction = safeAcquireTransaction();
        Map<String,Object> properties = new HashMap<>();

        try
        {
            NodeCursor nodes = transaction.ambientNodeCursor();
            PropertyCursor propertyCursor = transaction.ambientPropertyCursor();
            TokenRead token = transaction.tokenRead();
            singleNode( transaction, nodes );
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
        KernelTransaction transaction = safeAcquireTransaction();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            throw new NotFoundException( format( "No such property, '%s'.", key ) );
        }

        NodeCursor nodes = transaction.ambientNodeCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        singleNode( transaction, nodes );
        nodes.properties( properties );
        while ( properties.next() )
        {
            if ( propertyKey == properties.propertyKey() )
            {
                Value value = properties.propertyValue();
                if ( value == Values.NO_VALUE )
                {
                    throw new NotFoundException( format( "No such property, '%s'.", key ) );
                }
                return value.asObjectCopy();
            }
        }
        throw new NotFoundException( format( "No such property, '%s'.", key ) );
    }

    @Override
    public boolean hasProperty( String key )
    {
        if ( null == key )
        {
            return false;
        }

        KernelTransaction transaction = safeAcquireTransaction();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            return false;
        }

        NodeCursor nodes = transaction.ambientNodeCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        singleNode( transaction, nodes );
        nodes.properties( properties );
        while ( properties.next() )
        {
            if ( propertyKey == properties.propertyKey() )
            {
                return true;
            }
        }
        return false;
    }

    private KernelTransaction safeAcquireTransaction()
    {
        KernelTransaction transaction = spi.kernelTransaction();
        if ( transaction.isTerminated() )
        {
            Status terminationReason = transaction.getReasonIfTerminated().orElse( Status.Transaction.Terminated );
            throw new TransactionTerminatedException( terminationReason );
        }
        return transaction;
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
        // TODO: This is the checks we would like to do, but we have tests that expect to mix nodes...
        //if ( !(otherNode instanceof NodeProxy) || (((NodeProxy) otherNode).actions != actions) )
        //{
        //    throw new IllegalArgumentException( "Nodes do not belong to same graph database." );
        //}

        KernelTransaction transaction = safeAcquireTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            int relationshipTypeId = transaction.tokenWrite().relationshipTypeGetOrCreateForName( type.name() );
            long relationshipId = transaction.dataWrite()
                    .relationshipCreate( nodeId, relationshipTypeId, otherNode.getId() );
            return spi.newRelationshipProxy( relationshipId, nodeId, relationshipTypeId, otherNode.getId() );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new IllegalArgumentException( e );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node[" + e.entityId() +
                                         "] is deleted and cannot be used to create a relationship" );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public void addLabel( Label label )
    {
        KernelTransaction transaction = spi.kernelTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            transaction.dataWrite().nodeAddLabel( getId(),
                    transaction.tokenWrite().labelGetOrCreateForName( label.name() ) );
        }
        catch ( ConstraintValidationException e )
        {
            throw new ConstraintViolationException(
                    e.getUserMessage( new SilentTokenNameLookup( transaction.tokenRead() ) ), e );
        }
        catch ( IllegalTokenNameException e )
        {
            throw new ConstraintViolationException( format( "Invalid label name '%s'.", label.name() ), e );
        }
        catch ( TooManyLabelsException e )
        {
            throw new ConstraintViolationException( "Unable to add label.", e );
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
        KernelTransaction transaction = spi.kernelTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
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
        KernelTransaction transaction = safeAcquireTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            int labelId = transaction.tokenRead().nodeLabel( label.name() );
            if ( labelId == NO_SUCH_LABEL )
            {
                return false;
            }
            transaction.dataRead().singleNode( nodeId, nodes );
            return nodes.next() && nodes.labels().contains( labelId );
        }
    }

    @Override
    public Iterable<Label> getLabels()
    {
        KernelTransaction transaction = safeAcquireTransaction();
        NodeCursor nodes = transaction.ambientNodeCursor();
        try ( Statement ignore = spi.statement() )
        {
            singleNode( transaction, nodes );
            LabelSet labelSet = nodes.labels();
            TokenRead tokenRead = transaction.tokenRead();
            ArrayList<Label> list = new ArrayList<>( labelSet.numberOfLabels() );
            for ( int i = 0; i < labelSet.numberOfLabels(); i++ )
            {
                list.add( label( tokenRead.nodeLabelName( labelSet.label( i ) ) ) );
            }
            return list;
        }
        catch ( LabelNotFoundKernelException e )
        {
            throw new IllegalStateException( "Label retrieved through kernel API should exist.", e );
        }
    }

    @Override
    public int getDegree()
    {
        KernelTransaction transaction = safeAcquireTransaction();

        try ( Statement ignore = transaction.acquireStatement() )
        {
            NodeCursor nodes = transaction.ambientNodeCursor();
            singleNode( transaction, nodes );

            return Nodes.countAll( nodes, transaction.cursors() );
        }
    }

    @Override
    public int getDegree( RelationshipType type )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        int typeId = transaction.tokenRead().relationshipType( type.name() );
        if ( typeId == NO_ID )
        {   // This type doesn't even exist. Return 0
            return 0;
        }

        try ( Statement ignore = transaction.acquireStatement() )
        {
            NodeCursor nodes = transaction.ambientNodeCursor();
            singleNode( transaction, nodes );

            return Nodes.countAll( nodes, transaction.cursors(), typeId );
        }
    }

    @Override
    public int getDegree( Direction direction )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            NodeCursor nodes = transaction.ambientNodeCursor();
            singleNode( transaction, nodes );

            switch ( direction )
            {
            case OUTGOING:
                return Nodes.countOutgoing( nodes, transaction.cursors() );
            case INCOMING:
                return Nodes.countIncoming( nodes, transaction.cursors() );
            case BOTH:
                return Nodes.countAll( nodes, transaction.cursors() );
            default:
                throw new IllegalStateException( "Unknown direction " + direction );
            }
        }
    }

    @Override
    public int getDegree( RelationshipType type, Direction direction )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        int typeId = transaction.tokenRead().relationshipType( type.name() );
        if ( typeId == NO_ID )
        {   // This type doesn't even exist. Return 0
            return 0;
        }

        try ( Statement ignore = transaction.acquireStatement() )
        {
            NodeCursor nodes = transaction.ambientNodeCursor();
            singleNode( transaction, nodes );
            switch ( direction )
            {
            case OUTGOING:
                return Nodes.countOutgoing( nodes, transaction.cursors(), typeId );
            case INCOMING:
                return Nodes.countIncoming( nodes, transaction.cursors(), typeId );
            case BOTH:
                return Nodes.countAll( nodes, transaction.cursors(), typeId );
            default:
                throw new IllegalStateException( "Unknown direction " + direction );
            }
        }
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        KernelTransaction transaction = safeAcquireTransaction();
        try ( RelationshipTraversalCursor relationships = transaction.cursors().allocateRelationshipTraversalCursor();
              Statement ignore = transaction.acquireStatement() )
        {
            NodeCursor nodes = transaction.ambientNodeCursor();
            TokenRead tokenRead = transaction.tokenRead();
            singleNode( transaction, nodes );
            nodes.allRelationships( relationships );
            PrimitiveIntSet seen = Primitive.intSet();
            List<RelationshipType> types = new ArrayList<>();
            while ( relationships.next() )
            {
                int type = relationships.type();
                if ( !seen.contains( type ) )
                {
                    types.add( RelationshipType.withName( tokenRead.relationshipTypeName( relationships.type() ) ) );
                    seen.add( type );
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

        switch ( direction )
        {
        case OUTGOING:
            return outgoingIterator( transaction.cursors(), node, typeIds, this );
        case INCOMING:
            return incomingIterator( transaction.cursors(), node, typeIds, this );
        case BOTH:
            return allIterator( transaction.cursors(), node, typeIds, this );
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
        return spi.newRelationshipProxy( id, startNodeId, typeId, endNodeId );
    }
}
