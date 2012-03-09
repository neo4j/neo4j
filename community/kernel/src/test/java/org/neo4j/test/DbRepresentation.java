/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;
import org.neo4j.tooling.GlobalGraphOperations;

public class DbRepresentation implements Serializable
{
    private final Map<Long, NodeRep> nodes = new TreeMap<Long, NodeRep>();
    private long highestNodeId;
    private long highestRelationshipId;

    public static DbRepresentation of( GraphDatabaseService db )
    {
        DbRepresentation result = new DbRepresentation();
        for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
        {
            NodeRep nodeRep = new NodeRep( db, node );
            result.nodes.put( node.getId(), nodeRep );
            result.highestNodeId = Math.max( node.getId(), result.highestNodeId );
            result.highestRelationshipId = Math.max( nodeRep.highestRelationshipId, result.highestRelationshipId );
        }
        return result;
    }

    public static DbRepresentation of( String storeDir )
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).newGraphDatabase();
        try
        {
            return of( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    public long getHighestNodeId()
    {
        return highestNodeId;
    }

    public long getHighestRelationshipId()
    {
        return highestRelationshipId;
    }

    @Override
    public boolean equals( Object obj )
    {
        return ((DbRepresentation)obj).nodes.equals( nodes );
    }

    @Override
    public int hashCode()
    {
        return nodes.hashCode();
    }

    @Override
    public String toString()
    {
        return nodes.toString();
    }

    private static class NodeRep implements Serializable
    {
        private final PropertiesRep properties;
        private final Map<Long, PropertiesRep> outRelationships = new HashMap<Long, PropertiesRep>();
        private final long highestRelationshipId;
        private final long myId;
        private final Map<String, Map<String, Serializable>> index;

        NodeRep( GraphDatabaseService db, Node node )
        {
            myId = node.getId();
            properties = new PropertiesRep( node );
            long highestRel = 0;
            for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
            {
                outRelationships.put( rel.getId(), new PropertiesRep( rel ) );
                highestRel = Math.max( highestRel, rel.getId() );
            }
            this.highestRelationshipId = highestRel;
            index = new HashMap<String, Map<String, Serializable>>();
            fillIndex(db);
        }

        private void fillIndex(GraphDatabaseService db)
        {
            for (String indexName : db.index().nodeIndexNames())
            {
                Map<String, Serializable> thisIndex = new HashMap<String, Serializable>();
                Index<Node> tempIndex = db.index().forNodes( indexName );
                for (Map.Entry<String, Serializable> property : properties.props.entrySet())
                {
                    IndexHits<Node> content = tempIndex.get( property.getKey(), property.getValue() );
                    if (content.hasNext())
                    {
                        for (Node hit : content)
                        {
                            if (hit.getId() == myId)
                            {
                                thisIndex.put( property.getKey(), property.getValue() );
                                break;
                            }
                        }
                    }
                }
                index.put( indexName, thisIndex );
            }
        }

        /*
         * Yes, this is not the best way to do it - hash map does a deep equals. However,
         * if things go wrong, this way give the ability to check where the inequality
         * happened. If you feel strongly about this, feel free to change.
         * Admittedly, the implementation could use some cleanup.
         */
        private boolean checkEqualsForIndex( NodeRep other )
        {
            if ( other.index == index )
            {
                return true;
            }
            for ( Map.Entry<String, Map<String, Serializable>> entry : index.entrySet() )
            {
                if ( other.index.get( entry.getKey() ) == null )
                {
                    return false;
                }
                Map<String, Serializable> thisIndex = entry.getValue();
                Map<String, Serializable> otherIndex = other.index.get( entry.getKey() );
                if ( thisIndex.size() != otherIndex.size() )
                {
                    return false;
                }
                for (Map.Entry<String, Serializable> indexEntry : thisIndex.entrySet())
                {
                    if ( !indexEntry.getValue().equals(
                            otherIndex.get( indexEntry.getKey() ) ) )
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public boolean equals( Object obj )
        {
            NodeRep o = (NodeRep) obj;
            return o.myId == myId && o.properties.equals( properties )
                   && o.outRelationships.equals( outRelationships )
                   && checkEqualsForIndex( o );
        }

        @Override
        public int hashCode()
        {
            int result = 7;
            result += properties.hashCode()*7;
            result += outRelationships.hashCode()*13;
            result += myId * 17;
            result += index.hashCode() * 19;
            return result;
        }

        @Override
        public String toString()
        {
            return "<props: " + properties + ", rels: " + outRelationships + ">";
        }
    }

    private static class PropertiesRep implements Serializable
    {
        private final Map<String, Serializable> props = new HashMap<String, Serializable>();

        PropertiesRep( PropertyContainer entity )
        {
            for ( String key : entity.getPropertyKeys() )
            {
                Serializable value = (Serializable) entity.getProperty( key, null );
                // We do this because the node may have changed since we did getPropertyKeys()
                if ( value != null )
                {
                    if ( value.getClass().isArray() )
                    {
                        props.put( key, new ArrayList<Object>( Arrays.asList( IoPrimitiveUtils.asArray( value ) ) ) );
                    }
                    else
                    {
                        props.put( key, value );
                    }
                }
            }
        }

        @Override
        public boolean equals( Object obj )
        {
            return ((PropertiesRep)obj).props.equals( props );
        }

        @Override
        public int hashCode()
        {
            return props.hashCode();
        }

        @Override
        public String toString()
        {
            return props.toString();
        }
    }
}
