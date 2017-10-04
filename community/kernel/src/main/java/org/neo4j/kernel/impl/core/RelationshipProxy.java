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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

public class RelationshipProxy implements Relationship, RelationshipVisitor<RuntimeException>
{
    public interface RelationshipActions
    {
        Statement statement();

        Node newNodeProxy( long nodeId );

        RelationshipType getRelationshipTypeById( int type );

        GraphDatabaseService getGraphDatabaseService();

        void failTransaction();

        void assertInUnterminatedTransaction();
    }

    private final RelationshipActions actions;
    private long id = AbstractBaseRecord.NO_ID;
    private long startNode = AbstractBaseRecord.NO_ID;
    private long endNode = AbstractBaseRecord.NO_ID;
    private int type;

    public RelationshipProxy( RelationshipActions actions, long id, long startNode, int type, long endNode )
    {
        this.actions = actions;
        visit( id, type, startNode, endNode );
    }

    public RelationshipProxy( RelationshipActions actions, long id )
    {
        this.actions = actions;
        this.id = id;
    }

    @Override
    public void visit( long id, int type, long startNode, long endNode ) throws RuntimeException
    {
        this.id = id;
        this.type = type;
        this.startNode = startNode;
        this.endNode = endNode;
    }

    private void initializeData()
    {
        // it enough to check only start node, since it's absence will indicate that data was not yet loaded
        if ( startNode == AbstractBaseRecord.NO_ID )
        {
            try ( Statement statement = actions.statement() )
            {
                statement.readOperations().relationshipVisit( getId(), this );
            }
            catch ( EntityNotFoundException e )
            {
                throw new NotFoundException( e );
            }
        }
    }

    @Override
    public long getId()
    {
        return id;
    }

    private int typeId()
    {
        initializeData();
        return type;
    }

    private long sourceId()
    {
        initializeData();
        return startNode;
    }

    private long targetId()
    {
        initializeData();
        return endNode;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return actions.getGraphDatabaseService();
    }

    @Override
    public void delete()
    {
        try ( Statement statement = actions.statement() )
        {
            statement.dataWriteOperations().relationshipDelete( getId() );
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Unable to delete relationship[" +
                                             getId() + "] since it is already deleted." );
        }
        catch ( AutoIndexingKernelException e )
        {
            throw new IllegalStateException( "Auto indexing encountered a failure while deleting the relationship: "
                                             + e.getMessage(), e );
        }
    }

    @Override
    public Node[] getNodes()
    {
        assertInUnterminatedTransaction();
        return new Node[]{
                actions.newNodeProxy( sourceId() ),
                actions.newNodeProxy( targetId() )};
    }

    @Override
    public Node getOtherNode( Node node )
    {
        assertInUnterminatedTransaction();
        return actions.newNodeProxy( getOtherNodeId( node.getId() ) );
    }

    @Override
    public Node getStartNode()
    {
        assertInUnterminatedTransaction();
        return actions.newNodeProxy( sourceId() );
    }

    @Override
    public Node getEndNode()
    {
        assertInUnterminatedTransaction();
        return actions.newNodeProxy( targetId() );
    }

    @Override
    public long getStartNodeId()
    {
        return sourceId();
    }

    @Override
    public long getEndNodeId()
    {
        return targetId();
    }

    @Override
    public long getOtherNodeId( long id )
    {
        long start = sourceId();
        long end = targetId();
        if ( start == id )
        {
            return end;
        }
        if ( end == id )
        {
            return start;
        }
        throw new NotFoundException( "Node[" + id + "] not connected to this relationship[" + getId() + "]" );
    }

    @Override
    public RelationshipType getType()
    {
        assertInUnterminatedTransaction();
        return actions.getRelationshipTypeById( typeId() );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        try ( Statement statement = actions.statement() )
        {
            List<String> keys = new ArrayList<>();
            PrimitiveIntIterator properties = statement.readOperations().relationshipGetPropertyKeys( getId() );
            while ( properties.hasNext() )
            {
                keys.add( statement.readOperations().propertyKeyGetName( properties.next() ) );
            }
            return keys;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Relationship not found", e );
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist." );
        }
    }

    @Override
    public Map<String, Object> getProperties( String... keys )
    {
        if ( keys == null )
        {
            throw new NullPointerException( "keys" );
        }

        if ( keys.length == 0 )
        {
            return Collections.emptyMap();
        }

        try ( Statement statement = actions.statement() )
        {
            try ( Cursor<RelationshipItem> relationship = statement.readOperations().relationshipCursorById( getId() ) )
            {
                try ( Cursor<PropertyItem> propertyCursor = statement.readOperations()
                        .relationshipGetProperties( relationship.get() ) )
                {
                    return PropertyContainerProxyHelper.getProperties( statement, propertyCursor, keys );
                }
            }
            catch ( EntityNotFoundException e )
            {
                throw new NotFoundException( "Relationship not found", e );
            }
        }
    }

    @Override
    public Map<String, Object> getAllProperties()
    {
        try ( Statement statement = actions.statement() )
        {
            try ( Cursor<RelationshipItem> relationship = statement.readOperations().relationshipCursorById( getId() ) )
            {
                try ( Cursor<PropertyItem> propertyCursor = statement.readOperations()
                        .relationshipGetProperties( relationship.get() ) )
                {
                    Map<String,Object> properties = new HashMap<>();

                    // Get all properties
                    while ( propertyCursor.next() )
                    {
                        String name =
                                statement.readOperations().propertyKeyGetName( propertyCursor.get().propertyKeyId() );
                        properties.put( name, propertyCursor.get().value().asObjectCopy() );
                    }

                    return properties;
                }
            }
            catch ( EntityNotFoundException e )
            {
                throw new NotFoundException( "Relationship not found", e );
            }
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist.", e );
        }
    }

    @Override
    public Object getProperty( String key )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }

        try ( Statement statement = actions.statement() )
        {
            try
            {
                int propertyId = statement.readOperations().propertyKeyGetForName( key );
                if ( propertyId == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
                {
                    throw new NotFoundException( String.format( "No such property, '%s'.", key ) );
                }

                Value value = statement.readOperations().relationshipGetProperty( getId(), propertyId );

                if ( value == Values.NO_VALUE )
                {
                    throw new PropertyNotFoundException( propertyId, EntityType.RELATIONSHIP, getId() );
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
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }

        try ( Statement statement = actions.statement() )
        {
            int propertyId = statement.readOperations().propertyKeyGetForName( key );
            Value value = statement.readOperations().relationshipGetProperty( getId(), propertyId );
            return value == Values.NO_VALUE ? defaultValue : value.asObjectCopy();
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
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
            int propertyId = statement.readOperations().propertyKeyGetForName( key );
            return propertyId != KeyReadOperations.NO_SUCH_PROPERTY_KEY &&
                   statement.readOperations().relationshipHasProperty( getId(), propertyId );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
    }

    @Override
    public void setProperty( String key, Object value )
    {
        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            statement.dataWriteOperations()
                    .relationshipSetProperty( getId(), propertyKeyId, Values.of( value, false ) );
        }
        catch ( IllegalArgumentException e )
        {
            // Trying to set an illegal value is a critical error - fail this transaction
            actions.failTransaction();
            throw e;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( IllegalTokenNameException e )
        {
            // TODO: Maybe throw more context-specific error than just IllegalArgument
            throw new IllegalArgumentException( e );
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
    public Object removeProperty( String key )
    {
        try ( Statement statement = actions.statement() )
        {
            int propertyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            return statement.dataWriteOperations().relationshipRemoveProperty( getId(), propertyId ).asObjectCopy();
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
        catch ( IllegalTokenNameException e )
        {
            // TODO: Maybe throw more context-specific error than just IllegalArgument
            throw new IllegalArgumentException( e );
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
    public boolean isType( RelationshipType type )
    {
        assertInUnterminatedTransaction();
        return actions.getRelationshipTypeById( typeId() ).name().equals( type.name() );
    }

    public int compareTo( Object rel )
    {
        Relationship r = (Relationship) rel;
        long ourId = this.getId();
        long theirId = r.getId();

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
        return o instanceof Relationship && this.getId() == ((Relationship) o).getId();
    }

    @Override
    public int hashCode()
    {
        return (int) ((getId() >>> 32) ^ getId());
    }

    @Override
    public String toString()
    {
        String relType;
        try
        {
            relType = actions.getRelationshipTypeById( typeId() ).name();
            return format( "(%d)-[%s,%d]->(%d)", sourceId(), relType, getId(), targetId() );
        }
        catch ( NotInTransactionException | DatabaseShutdownException e )
        {
            // We don't keep the rel-name lookup if the database is shut down. Source ID and target ID also requires
            // database access in a transaction. However, failing on toString would be uncomfortably evil, so we fall
            // back to noting the relationship type id.
        }
        relType = "RELTYPE(" + type + ")";
        return format( "(?)-[%s,%d]->(?)", relType, getId() );
    }

    private void assertInUnterminatedTransaction()
    {
        actions.assertInUnterminatedTransaction();
    }
}
