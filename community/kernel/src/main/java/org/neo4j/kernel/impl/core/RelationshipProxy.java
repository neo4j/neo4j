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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;

public class RelationshipProxy implements Relationship, RelationshipVisitor<RuntimeException>
{
    private final EmbeddedProxySPI spi;
    private long id = AbstractBaseRecord.NO_ID;
    private long startNode = AbstractBaseRecord.NO_ID;
    private long endNode = AbstractBaseRecord.NO_ID;
    private int type;

    public RelationshipProxy( EmbeddedProxySPI spi, long id, long startNode, int type, long endNode )
    {
        this.spi = spi;
        visit( id, type, startNode, endNode );
    }

    public RelationshipProxy( EmbeddedProxySPI spi, long id )
    {
        this.spi = spi;
        this.id = id;
    }

    public static boolean isDeletedInCurrentTransaction( Relationship relationship )
    {
        if ( relationship instanceof RelationshipProxy )
        {
            RelationshipProxy proxy = (RelationshipProxy) relationship;
            KernelTransaction ktx = proxy.spi.kernelTransaction();
            try ( Statement ignore = ktx.acquireStatement() )
            {
                return ktx.dataRead().relationshipDeletedInTransaction( proxy.id );
            }
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
        if ( startNode == AbstractBaseRecord.NO_ID )
        {
            KernelTransaction transaction = spi.kernelTransaction();
            try ( Statement ignore = transaction.acquireStatement() )
            {
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
    public GraphDatabaseService getGraphDatabase()
    {
        return spi.getGraphDatabase();
    }

    @Override
    public void delete()
    {
        KernelTransaction transaction = spi.kernelTransaction();
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
        catch ( AutoIndexingKernelException e )
        {
            throw new IllegalStateException( "Auto indexing encountered a failure while deleting the relationship: "
                                             + e.getMessage(), e );
        }
    }

    @Override
    public Node[] getNodes()
    {
        spi.assertInUnterminatedTransaction();
        return new Node[]{
                spi.newNodeProxy( sourceId() ),
                spi.newNodeProxy( targetId() )};
    }

    @Override
    public Node getOtherNode( Node node )
    {
        spi.assertInUnterminatedTransaction();
        return spi.newNodeProxy( getOtherNodeId( node.getId() ) );
    }

    @Override
    public Node getStartNode()
    {
        spi.assertInUnterminatedTransaction();
        return spi.newNodeProxy( sourceId() );
    }

    @Override
    public Node getEndNode()
    {
        spi.assertInUnterminatedTransaction();
        return spi.newNodeProxy( targetId() );
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
        spi.assertInUnterminatedTransaction();
        return spi.getRelationshipTypeById( typeId() );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        KernelTransaction transaction = spi.kernelTransaction();
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

        KernelTransaction transaction = spi.kernelTransaction();

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
        KernelTransaction transaction = spi.kernelTransaction();
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
        KernelTransaction transaction = spi.kernelTransaction();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            throw new NotFoundException( format( "No such property, '%s'.", key ) );
        }

        RelationshipScanCursor relationships = transaction.ambientRelationshipCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        singleRelationship( transaction, relationships );
        relationships.properties( properties );
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
    public Object getProperty( String key, Object defaultValue )
    {
        if ( null == key )
        {
            throw new IllegalArgumentException( "(null) property key is not allowed" );
        }
        KernelTransaction transaction = spi.kernelTransaction();
        RelationshipScanCursor relationships = transaction.ambientRelationshipCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            return defaultValue;
        }
        singleRelationship( transaction, relationships );
        relationships.properties( properties );
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
    public boolean hasProperty( String key )
    {
        if ( null == key )
        {
            return false;
        }

        KernelTransaction transaction = spi.kernelTransaction();
        int propertyKey = transaction.tokenRead().propertyKey( key );
        if ( propertyKey == TokenRead.NO_TOKEN )
        {
            return false;
        }

        RelationshipScanCursor relationships = transaction.ambientRelationshipCursor();
        PropertyCursor properties = transaction.ambientPropertyCursor();
        singleRelationship( transaction, relationships );
        relationships.properties( properties );
        while ( properties.next() )
        {
            if ( propertyKey == properties.propertyKey() )
            {
                return true;
            }
        }
        return false;
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
            transaction.dataWrite().relationshipSetProperty( id, propertyKeyId, Values.of( value, false ) );
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

    }

    @Override
    public Object removeProperty( String key )
    {
        KernelTransaction transaction = spi.kernelTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            int propertyKeyId = transaction.tokenWrite().propertyKeyGetOrCreateForName( key );
            return transaction.dataWrite().relationshipRemoveProperty( id, propertyKeyId ).asObjectCopy();
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
    public boolean isType( RelationshipType type )
    {
        spi.assertInUnterminatedTransaction();
        return spi.getRelationshipTypeById( typeId() ).name().equals( type.name() );
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
            relType = spi.getRelationshipTypeById( typeId() ).name();
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

    private void singleRelationship( KernelTransaction transaction, RelationshipScanCursor relationships )
    {
        transaction.dataRead().singleRelationship( id, relationships );
        if ( !relationships.next() )
        {
            throw new NotFoundException( new EntityNotFoundException( EntityType.RELATIONSHIP, id ) );
        }
    }
}
