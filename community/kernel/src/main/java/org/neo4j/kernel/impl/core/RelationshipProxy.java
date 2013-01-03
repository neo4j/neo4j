/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.transaction.LockType;

public class RelationshipProxy implements Relationship
{
    public interface RelationshipLookups
    {
        Node lookupNode(long nodeId);
        Node newNodeProxy( long nodeId );
        RelationshipImpl lookupRelationship(long relationshipId);
        GraphDatabaseService getGraphDatabaseService();
        NodeManager getNodeManager();
        RelationshipImpl lookupRelationship( long relId, LockType lock );
    }
    
    private final long relId;
    private final RelationshipLookups relationshipLookups;

    RelationshipProxy( long relId, RelationshipLookups relationshipLookups )
    {
        this.relId = relId;
        this.relationshipLookups = relationshipLookups;
    }

    public long getId()
    {
        return relId;
    }

    public GraphDatabaseService getGraphDatabase()
    {
        return relationshipLookups.getGraphDatabaseService();
    }

    public void delete()
    {
        relationshipLookups.lookupRelationship( relId, LockType.WRITE ).delete( relationshipLookups.getNodeManager(), this );
    }

    public Node[] getNodes()
    {
        RelationshipImpl relationship = relationshipLookups.lookupRelationship( relId );
        return new Node[]{ relationshipLookups.newNodeProxy( relationship.getStartNodeId() ), relationshipLookups.newNodeProxy( relationship.getEndNodeId() )};
    }

    public Node getOtherNode( Node node )
    {
        RelationshipImpl relationship = relationshipLookups.lookupRelationship( relId );
        if ( relationship.getStartNodeId() == node.getId() )
        {
            return relationshipLookups.newNodeProxy( relationship.getEndNodeId() );
        }
        if ( relationship.getEndNodeId() == node.getId() )
        {
            return relationshipLookups.newNodeProxy( relationship.getStartNodeId() );
        }
        throw new NotFoundException( "Node[" + node.getId()
            + "] not connected to this relationship[" + getId() + "]" );
    }

    public Node getStartNode()
    {
        return relationshipLookups.newNodeProxy( relationshipLookups.lookupRelationship( relId ).getStartNodeId() );
    }

    public Node getEndNode()
    {
        return relationshipLookups.newNodeProxy( relationshipLookups.lookupRelationship( relId ).getEndNodeId() );
    }

    public RelationshipType getType()
    {
        return relationshipLookups.getNodeManager().getRelationshipTypeById( relationshipLookups.lookupRelationship( relId )
                                                                                 .getTypeId() );
    }

    public Iterable<String> getPropertyKeys()
    {
        return relationshipLookups.lookupRelationship( relId ).getPropertyKeys( relationshipLookups.getNodeManager() );
    }

    public Iterable<Object> getPropertyValues()
    {
        return relationshipLookups.lookupRelationship( relId ).getPropertyValues( relationshipLookups.getNodeManager() );
    }

    public Object getProperty( String key )
    {
        return relationshipLookups.lookupRelationship( relId ).getProperty( relationshipLookups.getNodeManager(), key );
    }

    public Object getProperty( String key, Object defaultValue )
    {
        return relationshipLookups.lookupRelationship( relId ).getProperty( relationshipLookups.getNodeManager(), key, defaultValue );
    }

    public boolean hasProperty( String key )
    {
        return relationshipLookups.lookupRelationship( relId ).hasProperty( relationshipLookups.getNodeManager(), key );
    }

    public void setProperty( String key, Object property )
    {
        relationshipLookups.lookupRelationship( relId, LockType.WRITE ).setProperty( relationshipLookups.getNodeManager(), this, key, property );
    }

    public Object removeProperty( String key )
    {
        return relationshipLookups.lookupRelationship( relId, LockType.WRITE ).removeProperty( relationshipLookups.getNodeManager(), this, key );
    }

    public boolean isType( RelationshipType type )
    {
        return relationshipLookups.getNodeManager().getRelationshipTypeById( relationshipLookups.lookupRelationship( relId ).getTypeId() ).name().equals( type.name() );
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
        if ( !(o instanceof Relationship) )
        {
            return false;
        }
        return this.getId() == ((Relationship) o).getId();
    }

    @Override
    public int hashCode()
    {
        return (int) (( relId >>> 32 ) ^ relId );
    }

    @Override
    public String toString()
    {
        return "Relationship[" + this.getId() + "]";
    }
}