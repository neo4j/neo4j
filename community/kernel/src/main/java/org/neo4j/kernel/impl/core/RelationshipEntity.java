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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TokenCapacityExceededKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.Read.NO_ID;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

public class RelationshipEntity implements Relationship, RelationshipVisitor<RuntimeException>
{
    public static final long SHALLOW_SIZE = shallowSizeOfInstance( RelationshipEntity.class );

    private final InternalTransaction internalTransaction;
    private long id = NO_ID;
    private long startNode = NO_ID;
    private long endNode = NO_ID;
    private int type;

    public RelationshipEntity( InternalTransaction internalTransaction, long id, long startNode, int type, long endNode )
    {
        this.internalTransaction = internalTransaction;
        visit( id, type, startNode, endNode );
    }

    public RelationshipEntity( InternalTransaction internalTransaction, long id )
    {
        this.internalTransaction = internalTransaction;
        this.id = id;
    }

    public static boolean isDeletedInCurrentTransaction( Relationship relationship )
    {
        if ( relationship instanceof RelationshipEntity )
        {
            RelationshipEntity proxy = (RelationshipEntity) relationship;
            KernelTransaction ktx = proxy.internalTransaction.kernelTransaction();
            return ktx.dataRead().relationshipDeletedInTransaction( proxy.id );
        }
        return false;
    }

    @Override
    public final void visit( long id, int type, long startNode, long endNode ) throws RuntimeException
    {
        this.id = id;
        this.type = type;
        this.startNode = startNode;
        this.endNode = endNode;
    }

    public boolean initializeData()
    {
        // It enough to check only start node, since it's absence will indicate that data was not yet loaded.
        if ( startNode == NO_ID )
        {
            KernelTransaction transaction = internalTransaction.kernelTransaction();
            RelationshipScanCursor relationships = transaction.ambientRelationshipCursor();
            transaction.dataRead().singleRelationship( id, relationships );
            // At this point we don't care if it is there or not just load what we got.
            boolean wasPresent = relationships.next();
            this.type = relationships.type();
            this.startNode = relationships.sourceNodeReference();
            this.endNode = relationships.targetNodeReference();
            // But others might care, e.g. the Bolt server needs to know for serialisation purposes.
            return wasPresent;
        }
        return true;
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
    public void delete()
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        try
        {
            boolean deleted = transaction.dataWrite().relationshipDelete( id );
            if ( !deleted )
            {
                throw new NotFoundException( "Unable to delete relationship[" +
                                             getId() + "] since it is already deleted." );
            }
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Node[] getNodes()
    {
        internalTransaction.checkInTransaction();
        return new Node[]{
                internalTransaction.newNodeEntity( sourceId() ),
                internalTransaction.newNodeEntity( targetId() )};
    }

    @Override
    public Node getOtherNode( Node node )
    {
        internalTransaction.checkInTransaction();
        return internalTransaction.newNodeEntity( getOtherNodeId( node.getId() ) );
    }

    @Override
    public Node getStartNode()
    {
        internalTransaction.checkInTransaction();
        return internalTransaction.newNodeEntity( sourceId() );
    }

    @Override
    public Node getEndNode()
    {
        internalTransaction.checkInTransaction();
        return internalTransaction.newNodeEntity( targetId() );
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
        throw new NotFoundException( "Node[" + id + "] not connected to this relationship[" + getId() + ']' );
    }

    @Override
    public RelationshipType getType()
    {
        internalTransaction.checkInTransaction();
        return internalTransaction.getRelationshipTypeById( typeId() );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        List<String> keys = new ArrayList<>();
        try
        {
            RelationshipScanCursor relationships = transaction.ambientRelationshipCursor();
            PropertyCursor properties = transaction.ambientPropertyCursor();
            singleRelationship( transaction, relationships );
            TokenRead token = transaction.tokenRead();
            relationships.properties( properties );
            while ( properties.next() )
            {
                keys.add( token.propertyKeyName( properties.propertyKey() ));
            }
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new IllegalStateException( "Property key retrieved through kernel API should exist.", e );
        }
        return keys;
    }

    @Override
    public Map<String, Object> getProperties( String... keys )
    {
        Objects.requireNonNull( keys, "Properties keys should be not null array." );

        if ( keys.length == 0 )
        {
            return Collections.emptyMap();
        }

        KernelTransaction transaction = internalTransaction.kernelTransaction();

        int itemsToReturn = keys.length;
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

        Map<String,Object> properties = new HashMap<>( itemsToReturn );
        RelationshipScanCursor relationships = transaction.ambientRelationshipCursor();
        PropertyCursor propertyCursor = transaction.ambientPropertyCursor();
        singleRelationship( transaction, relationships );
        relationships.properties( propertyCursor );
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
    public Map<String, Object> getAllProperties()
    {
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        Map<String,Object> properties = new HashMap<>();

        try
        {
            RelationshipScanCursor relationships = transaction.ambientRelationshipCursor();
            PropertyCursor propertyCursor = transaction.ambientPropertyCursor();
            TokenRead token = transaction.tokenRead();
            singleRelationship( transaction, relationships );
            relationships.properties( propertyCursor );
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
    public Object getProperty( String key )
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

        RelationshipScanCursor relationships = transaction.ambientRelationshipCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        singleRelationship( transaction, relationships );
        relationships.properties( properties );
        if ( !properties.seekProperty( propertyKey ) )
        {
            throw new NotFoundException( format( "No such property, '%s'.", key ) );
        }
        return properties.propertyValue().asObjectCopy();
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }
        KernelTransaction transaction = internalTransaction.kernelTransaction();
        RelationshipScanCursor relationships = transaction.ambientRelationshipCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            return defaultValue;
        }
        singleRelationship( transaction, relationships );
        relationships.properties( properties );
        return properties.seekProperty( propertyKey ) ? properties.propertyValue().asObjectCopy() : defaultValue;
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

        RelationshipScanCursor relationships = transaction.ambientRelationshipCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        singleRelationship( transaction, relationships );
        relationships.properties( properties );
        return properties.seekProperty( propertyKey );
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
            transaction.dataWrite().relationshipSetProperty( id, propertyKeyId, Values.of( value, false ) );
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
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
    }

    @Override
    public Object removeProperty( String key )
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
            return transaction.dataWrite().relationshipRemoveProperty( id, propertyKeyId ).asObjectCopy();
        }
        catch ( InvalidTransactionTypeKernelException e )
        {
            throw new ConstraintViolationException( e.getMessage(), e );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e );
        }
    }

    @Override
    public boolean isType( RelationshipType type )
    {
        internalTransaction.checkInTransaction();
        return internalTransaction.getRelationshipTypeById( typeId() ).name().equals( type.name() );
    }

    public int compareTo( Object rel )
    {
        Relationship r = (Relationship) rel;
        return Long.compare( this.getId(), r.getId() );
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
            relType = internalTransaction.getRelationshipTypeById( typeId() ).name();
            return format( "(%d)-[%s,%d]->(%d)", sourceId(), relType, getId(), targetId() );
        }
        catch ( NotInTransactionException | DatabaseShutdownException e )
        {
            // We don't keep the rel-name lookup if the database is shut down. Source ID and target ID also requires
            // database access in a transaction. However, failing on toString would be uncomfortably evil, so we fall
            // back to noting the relationship type id.
        }
        relType = "RELTYPE(" + type + ')';
        return format( "(?)-[%s,%d]->(?)", relType, getId() );
    }

    private void singleRelationship( KernelTransaction transaction, RelationshipScanCursor relationships )
    {
        transaction.dataRead().singleRelationship( id, relationships );
        if ( !relationships.next() )
        {
            throw new NotFoundException( new EntityNotFoundException( EntityType.RELATIONSHIP, id ) );
        }
    }
}
