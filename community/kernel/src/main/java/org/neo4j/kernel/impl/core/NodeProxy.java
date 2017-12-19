/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
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
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.map;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;
import static org.neo4j.kernel.impl.core.TokenHolder.NO_ID;

public class NodeProxy implements Node
{
    public interface NodeActions
    {
        Statement statement();

        KernelTransaction kernelTransaction();

        GraphDatabaseService getGraphDatabase();

        void assertInUnterminatedTransaction();

        void failTransaction();

        Relationship newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId );
    }

    private final NodeActions actions;
    private final long nodeId;

    public NodeProxy( NodeActions actions, long nodeId )
    {
        this.nodeId = nodeId;
        this.actions = actions;
    }

    @Override
    public long getId()
    {
        return nodeId;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return actions.getGraphDatabase();
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
    public ResourceIterable<Relationship> getRelationships( final Direction dir )
    {
        assertInUnterminatedTransaction();
        return () ->
        {
            Statement statement = actions.statement();
            try
            {
                RelationshipConversion result = new RelationshipConversion( actions );
                result.iterator = statement.readOperations().nodeGetRelationships( nodeId, dir );
                result.statement = statement;
                return result;
            }
            catch ( EntityNotFoundException e )
            {
                statement.close();
                throw new NotFoundException( format( "Node %d not found", nodeId ), e );
            }
            catch ( Throwable e )
            {
                statement.close();
                throw e;
            }
        };
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
        final int[] typeIds;
        try ( Statement statement = actions.statement() )
        {
            typeIds = relTypeIds( types, statement );
        }
        return () ->
        {
            Statement statement = actions.statement();
            try
            {
                RelationshipConversion result = new RelationshipConversion( actions );
                result.iterator = statement.readOperations().nodeGetRelationships(
                        nodeId, direction, typeIds );
                result.statement = statement;
                return result;
            }
            catch ( EntityNotFoundException e )
            {
                statement.close();
                throw new NotFoundException( format( "Node %d not found", nodeId ), e );
            }
            catch ( Throwable e )
            {
                statement.close();
                throw e;
            }
        };
    }

    @Override
    public boolean hasRelationship()
    {
        return hasRelationship( Direction.BOTH );
    }

    @Override
    public boolean hasRelationship( Direction dir )
    {
        try ( ResourceIterator<Relationship> rels = getRelationships( dir ).iterator() )
        {
            return rels.hasNext();
        }
    }

    @Override
    public boolean hasRelationship( RelationshipType... types )
    {
        return hasRelationship( Direction.BOTH, types );
    }

    @Override
    public boolean hasRelationship( Direction direction, RelationshipType... types )
    {
        try ( ResourceIterator<Relationship> rels = getRelationships( direction, types ).iterator() )
        {
            return rels.hasNext();
        }
    }

    @Override
    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return hasRelationship( dir, type );
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

    private void assertInUnterminatedTransaction()
    {
        actions.assertInUnterminatedTransaction();
    }

    @Override
    public void setProperty( String key, Object value )
    {
        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            try
            {
                statement.dataWriteOperations().nodeSetProperty( nodeId, propertyKeyId, Values.of( value, false ) );
            }
            catch ( ConstraintValidationException e )
            {
                throw new ConstraintViolationException(
                        e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
            catch ( IllegalArgumentException e )
            {
                // Trying to set an illegal value is a critical error - fail this transaction
                actions.failTransaction();
                throw e;
            }
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
            throw new IllegalStateException( "Auto indexing encountered a failure while setting property: "
                                             + e.getMessage(), e );
        }
    }

    @Override
    public Object removeProperty( String key ) throws NotFoundException
    {
        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            return statement.dataWriteOperations().nodeRemoveProperty( nodeId, propertyKeyId ).asObjectCopy();
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
        NodeCursor nodes = transaction.nodeCursor();
        PropertyCursor properties = transaction.propertyCursor();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
        {
            return defaultValue;
        }
        transaction.dataRead().singleNode( nodeId, nodes );
        if ( !nodes.next() )
        {
            throw new NotFoundException( new EntityNotFoundException( EntityType.NODE, nodeId ) );
        }
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
            NodeCursor nodes = transaction.nodeCursor();
            PropertyCursor properties = transaction.propertyCursor();
            transaction.dataRead().singleNode( nodeId, nodes );
            TokenRead token = transaction.tokenRead();
            if ( !nodes.next() )
            {
                throw new NotFoundException( new EntityNotFoundException( EntityType.NODE, nodeId ) );
            }
            nodes.properties( properties );
            while ( properties.next() )
            {
               keys.add( token.propertyKeyGetName( properties.propertyKey() ));
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

        NodeCursor nodes = transaction.nodeCursor();
        PropertyCursor propertyCursor = transaction.propertyCursor();
        transaction.dataRead().singleNode( nodeId, nodes );
        if ( !nodes.next() )
        {
            throw new NotFoundException( new EntityNotFoundException( EntityType.NODE, nodeId ) );
        }
        nodes.properties( propertyCursor );
        int propertiesToFind = itemsToReturn;
        while ( propertiesToFind > 0 && propertyCursor.next() )
        {
            //Do a linear check if this is a property we are interested in.
            for ( int i = 0; i < itemsToReturn; i++ )
            {
                int propertyId = propertyIds[i];
                int currentKey = propertyCursor.propertyKey();
                if ( propertyId == currentKey )
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
            NodeCursor nodes = transaction.nodeCursor();
            PropertyCursor propertyCursor = transaction.propertyCursor();
            TokenRead token = transaction.tokenRead();
            transaction.dataRead().singleNode( nodeId, nodes );
            if ( !nodes.next() )
            {
                throw new NotFoundException( new EntityNotFoundException( EntityType.NODE, nodeId ) );
            }
            nodes.properties( propertyCursor );
            while ( propertyCursor.next() )
            {
                properties.put( token.propertyKeyGetName( propertyCursor.propertyKey() ),
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
        if ( propertyKey == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
        {
            throw new NotFoundException( format( "No such property, '%s'.", key ) );
        }

        NodeCursor nodes = transaction.nodeCursor();
        PropertyCursor properties = transaction.propertyCursor();
        transaction.dataRead().singleNode( nodeId, nodes );
        if ( !nodes.next() )
        {
            throw new NotFoundException( new EntityNotFoundException( EntityType.NODE, nodeId ) );
        }
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
        if ( propertyKey == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
        {
            return false;
        }

        NodeCursor nodes = transaction.nodeCursor();
        PropertyCursor properties = transaction.propertyCursor();
        transaction.dataRead().singleNode( nodeId, nodes );
        if ( !nodes.next() )
        {
            throw new NotFoundException( new EntityNotFoundException( EntityType.NODE, nodeId ) );
        }
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
        KernelTransaction transaction = actions.kernelTransaction();
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
        long ourId = this.getId();
        long theirId = n.getId();

        if ( ourId < theirId )
        {
            return -1;
        }
        else if ( ourId > theirId )
        {
            return 1;
        }
        else
        {
            return 0;
        }
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
        try ( Statement statement = actions.statement() )
        {
            int relationshipTypeId = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( type.name() );
            long relationshipId = statement.dataWriteOperations()
                    .relationshipCreate( relationshipTypeId, nodeId, otherNode.getId() );
            return actions.newRelationshipProxy( relationshipId, nodeId, relationshipTypeId, otherNode.getId() );
        }
        catch ( IllegalTokenNameException | RelationshipTypeIdNotFoundKernelException e )
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
        try ( Statement statement = actions.statement() )
        {
            try
            {
                statement.dataWriteOperations().nodeAddLabel( getId(),
                        statement.tokenWriteOperations().labelGetOrCreateForName( label.name() ) );
            }
            catch ( ConstraintValidationException e )
            {
                throw new ConstraintViolationException(
                        e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
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
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public void removeLabel( Label label )
    {
        try ( Statement statement = actions.statement() )
        {
            int labelId = statement.readOperations().labelGetForName( label.name() );
            if ( labelId != KeyReadOperations.NO_SUCH_LABEL )
            {
                statement.dataWriteOperations().nodeRemoveLabel( getId(), labelId );
            }
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "No node with id " + getId() + " found.", e );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public boolean hasLabel( Label label )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        try ( Statement ignore = transaction.acquireStatement();
              NodeCursor nodes = transaction.cursors().allocateNodeCursor() )
        {
            int labelId = transaction.tokenRead().labelGetForName( label.name() );
            transaction.dataRead().singleNode( nodeId, nodes );
            return nodes.next() && nodes.labels().contains( labelId );

        }
        catch ( LabelNotFoundKernelException e )
        {
            return false;
        }
    }

    @Override
    public Iterable<Label> getLabels()
    {
        KernelTransaction transaction = safeAcquireTransaction();
        try ( Statement ignore = actions.statement();
              NodeCursor nodes = transaction.cursors().allocateNodeCursor() )
        {
            transaction.dataRead().singleNode( nodeId, nodes );
            if ( !nodes.next() )
            {
                throw new NotFoundException( "Node not found" );
            }
            LabelSet labelSet = nodes.labels();
            TokenRead tokenRead = transaction.tokenRead();
            ArrayList<Label> list = new ArrayList<>( labelSet.numberOfLabels() );
            for ( int i = 0; i < labelSet.numberOfLabels(); i++ )
            {
                list.add( label( tokenRead.labelGetName( labelSet.label( i ) ) ) );
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
        try ( Statement statement = actions.statement() )
        {
            return statement.readOperations().nodeGetDegree( nodeId, Direction.BOTH );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found.", e );
        }
    }

    @Override
    public int getDegree( RelationshipType type )
    {
        try ( Statement statement = actions.statement() )
        {
            ReadOperations ops = statement.readOperations();
            int typeId = ops.relationshipTypeGetForName( type.name() );
            if ( typeId == NO_ID )
            {   // This type doesn't even exist. Return 0
                return 0;
            }
            return ops.nodeGetDegree( nodeId, Direction.BOTH, typeId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found.", e );
        }
    }

    @Override
    public int getDegree( Direction direction )
    {
        try ( Statement statement = actions.statement() )
        {
            ReadOperations ops = statement.readOperations();
            return ops.nodeGetDegree( nodeId, direction );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found.", e );
        }
    }

    @Override
    public int getDegree( RelationshipType type, Direction direction )
    {
        try ( Statement statement = actions.statement() )
        {
            ReadOperations ops = statement.readOperations();
            int typeId = ops.relationshipTypeGetForName( type.name() );
            if ( typeId == NO_ID )
            {   // This type doesn't even exist. Return 0
                return 0;
            }
            return ops.nodeGetDegree( nodeId, direction, typeId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found.", e );
        }
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        try ( Statement statement = actions.statement() )
        {
            PrimitiveIntIterator relTypes = statement.readOperations().nodeGetRelationshipTypes( nodeId );
            return asList( map( relTypeId -> convertToRelationshipType( statement, relTypeId ), relTypes ) );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found.", e );
        }
    }

    private int[] relTypeIds( RelationshipType[] types, Statement statement )
    {
        int[] ids = new int[types.length];
        int outIndex = 0;
        for ( RelationshipType type : types )
        {
            int id = statement.readOperations().relationshipTypeGetForName( type.name() );
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

    private RelationshipType convertToRelationshipType( final Statement statement, int relTypeId )
    {
        try
        {
            return RelationshipType.withName( statement.readOperations().relationshipTypeGetName( relTypeId ) );
        }
        catch ( RelationshipTypeIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Kernel API returned non-existent relationship type: " + relTypeId );
        }
    }
}
