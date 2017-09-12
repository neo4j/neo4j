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
import org.neo4j.cursor.Cursor;
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
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
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
        try ( Statement statement = actions.statement() )
        {
            statement.dataWriteOperations().nodeDelete( getId() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Unable to delete Node[" + nodeId +
                                             "] since it has already been deleted." );
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

        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( key );
            Value value =  statement.readOperations().nodeGetProperty( nodeId, propertyKeyId );
            return value == Values.NO_VALUE ? defaultValue : value.asObjectCopy();
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        try ( Statement statement = actions.statement() )
        {
            List<String> keys = new ArrayList<>();
            PrimitiveIntIterator properties = statement.readOperations().nodeGetPropertyKeys( getId() );
            while ( properties.hasNext() )
            {
                keys.add( statement.readOperations().propertyKeyGetName( properties.next() ) );
            }
            return keys;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found", e );
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist.", e );
        }
    }

    @Override
    public Map<String, Object> getProperties( String... keys )
    {
        Objects.requireNonNull( keys, "Properties keys should be not null array." );

        if ( keys.length == 0 )
        {
            return Collections.emptyMap();
        }

        try ( Statement statement = actions.statement() )
        {
            try ( Cursor<NodeItem> node = statement.readOperations().nodeCursorById( nodeId ) )
            {
                try ( Cursor<PropertyItem> propertyCursor = statement.readOperations().nodeGetProperties( node.get() ) )
                {
                    return PropertyContainerProxyHelper.getProperties( statement, propertyCursor, keys );
                }
            }
            catch ( EntityNotFoundException e )
            {
                throw new NotFoundException( "Node not found", e );
            }
        }
    }

    @Override
    public Map<String, Object> getAllProperties()
    {
        try ( Statement statement = actions.statement() )
        {
            try ( Cursor<NodeItem> node = statement.readOperations().nodeCursorById( nodeId ) )
            {
                try ( Cursor<PropertyItem> propertyCursor = statement.readOperations().nodeGetProperties( node.get() ) )
                {
                    Map<String, Object> properties = new HashMap<>();

                    // Get all properties
                    while ( propertyCursor.next() )
                    {
                        String name = statement.readOperations().propertyKeyGetName(
                                propertyCursor.get().propertyKeyId() );
                        properties.put( name, propertyCursor.get().value().asObjectCopy() );
                    }

                    return properties;
                }
            }
            catch ( EntityNotFoundException e )
            {
                throw new NotFoundException( "Node not found", e );
            }
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist.", e );
        }
    }

    @Override
    public Object getProperty( String key ) throws NotFoundException
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }

        try ( Statement statement = actions.statement() )
        {
            try
            {
                int propertyKeyId = statement.readOperations().propertyKeyGetForName( key );
                if ( propertyKeyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
                {
                    throw new NotFoundException( format( "No such property, '%s'.", key ) );
                }

                Value value = statement.readOperations().nodeGetProperty( nodeId, propertyKeyId );

                if ( value == Values.NO_VALUE )
                {
                    throw new PropertyNotFoundException( propertyKeyId, EntityType.NODE, nodeId );
                }

                return value.asObjectCopy();

            }
            catch ( EntityNotFoundException | PropertyNotFoundException e )
            {
                throw new NotFoundException(
                        e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
            }
        }
    }

    @Override
    public boolean hasProperty( String key )
    {
        if ( null == key )
        {
            return false;
        }

        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.readOperations().propertyKeyGetForName( key );
            return statement.readOperations().nodeHasProperty( nodeId, propertyKeyId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
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
            return actions.newRelationshipProxy( relationshipId, nodeId, relationshipTypeId, otherNode.getId()  );
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
        try ( Statement statement = actions.statement() )
        {
            int labelId = statement.readOperations().labelGetForName( label.name() );
            return statement.readOperations().nodeHasLabel( getId(), labelId );
        }
        catch ( EntityNotFoundException e )
        {
            return false;
        }
    }

    @Override
    public Iterable<Label> getLabels()
    {
        try ( Statement statement = actions.statement() )
        {
            PrimitiveIntIterator labels = statement.readOperations().nodeGetLabels( getId() );
            return asList( map( labelId -> convertToLabel( statement, labelId ), labels ) );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Node not found", e );
        }
    }

    private Label convertToLabel( Statement statement, int labelId )
    {
        try
        {
            return label( statement.readOperations().labelGetName( labelId ) );
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
        for ( int i = 0; i < types.length; i++ )
        {
            int id = statement.readOperations().relationshipTypeGetForName( types[i].name() );
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
