/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;

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
    /** [unused,target][source,id] **/
    private short hiBits;
    private short type;
    private int loId, loSource, loTarget;

    public RelationshipProxy( RelationshipActions actions, long id, long startNode, int type, long endNode )
    {
        this.actions = actions;
        visit( id, type, startNode, endNode );
    }

    public RelationshipProxy( RelationshipActions actions, long id )
    {
        this.actions = actions;
        assert (0xFFFF_FFF0_0000_0000L & id) == 0;
        this.hiBits = (short) (0xF000 | (id >> 32));
        this.loId = (int) id;
    }

    @Override
    public void visit( long id, int type, long startNode, long endNode ) throws RuntimeException
    {
        assert (0xFFFF_FFF0_0000_0000L & id) == 0 &&        // 36 bits
               (0xFFFF_FFF0_0000_0000L & startNode) == 0 && // 36 bits
               (0xFFFF_FFF0_0000_0000L & endNode) == 0 &&   // 36 bits
               (0xFFFF_0000 & type) == 0                    // 16 bits
               : "For id:" + id + ", type:" + type + ", source:" + startNode + ", target:" + endNode;
        this.hiBits = (short) ((id >> 32) | ((startNode >> 28) & 0x00F0) | ((endNode >> 24) & 0x0F00));
        this.type = (short) type;
        this.loId = (int) id;
        this.loSource = (int) startNode;
        this.loTarget = (int) endNode;
    }

    private void initializeData()
    {
        if ( (hiBits & 0xF000) != 0 )
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
        long loBits = allBitsOf( loId );
        return hiBits == 0 ? loBits : ((hiBits & 0x000FL) << 32 | loBits);
    }

    private int typeId()
    {
        initializeData();
        return type & 0xFFFF;
    }

    private long sourceId()
    {
        initializeData();
        long loBits = allBitsOf( loSource );
        return hiBits == 0 ? loBits : ((hiBits & 0x00F0L) << 28 | loBits);
    }

    private long targetId()
    {
        initializeData();
        long loBits = allBitsOf( loTarget );
        return hiBits == 0 ? loBits : ((hiBits & 0x0F00L) << 24 | loBits);
    }

    private long allBitsOf( int bits )
    {
        return bits&0xFFFFFFFFL;
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
            throw new IllegalStateException( "Unable to delete relationship[" +
                                             getId() + "] since it is already deleted." );
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
        if ( sourceId() == node.getId() )
        {
            return actions.newNodeProxy( targetId() );
        }
        if ( targetId() == node.getId() )
        {
            return actions.newNodeProxy( sourceId() );
        }
        throw new NotFoundException( "Node[" + node.getId()
                                     + "] not connected to this relationship[" + getId() + "]" );
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
            Iterator<DefinedProperty> properties = statement.readOperations().relationshipGetAllProperties( getId() );
            while ( properties.hasNext() )
            {
                keys.add( statement.readOperations().propertyKeyGetName( properties.next().propertyKeyId() ) );
            }
            return keys;
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( "Relationship not found", e );
        }
        catch ( PropertyKeyIdNotFoundKernelException e )
        {
            throw new ThisShouldNotHappenError( "Jake",
                    "Property key retrieved through kernel API should exist." );
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
                return statement.readOperations().relationshipGetProperty( getId(), propertyId ).value();
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
            return statement.readOperations().relationshipGetProperty( getId(), propertyId ).value( defaultValue );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
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
                   statement.readOperations().relationshipGetProperty( getId(), propertyId ).isDefined();
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
        }
    }

    @Override
    public void setProperty( String key, Object value )
    {
        try ( Statement statement = actions.statement() )
        {
            int propertyKeyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            statement.dataWriteOperations().relationshipSetProperty( getId(), Property.property( propertyKeyId, value ) );
        }
        catch ( IllegalArgumentException e )
        {
            // Trying to set an illegal value is a critical error - fail this transaction
            actions.failTransaction();
            throw e;
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
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
    }

    @Override
    public Object removeProperty( String key )
    {
        try ( Statement statement = actions.statement() )
        {
            int propertyId = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( key );
            return statement.dataWriteOperations().relationshipRemoveProperty( getId(), propertyId ).value( null );
        }
        catch ( EntityNotFoundException e )
        {
            throw new IllegalStateException( e );
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
        long ourId = this.getId(), theirId = r.getId();

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
        return "Relationship[" + this.getId() + "]";
    }

    private void assertInUnterminatedTransaction()
    {
        actions.assertInUnterminatedTransaction();
    }
}
