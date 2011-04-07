/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

public class DbRepresentation implements Serializable
{
    private final Map<Long, NodeRep> nodes = new TreeMap<Long, NodeRep>();
    private long highestNodeId;
    private long highestRelationshipId;
    
    public static DbRepresentation of( GraphDatabaseService db )
    {
        DbRepresentation result = new DbRepresentation();
        for ( Node node : db.getAllNodes() )
        {
            NodeRep nodeRep = new NodeRep( node );
            result.nodes.put( node.getId(), nodeRep );
            result.highestNodeId = Math.max( node.getId(), result.highestNodeId );
            result.highestRelationshipId = Math.max( nodeRep.highestRelationshipId, result.highestRelationshipId );
        }
        return result;
    }
    
    public static DbRepresentation of( String storeDir )
    {
        GraphDatabaseService db = new EmbeddedGraphDatabase( storeDir );
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
        
        NodeRep( Node node )
        {
            properties = new PropertiesRep( node );
            long highestRel = 0;
            for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
            {
                outRelationships.put( rel.getId(), new PropertiesRep( rel ) );
                highestRel = Math.max( highestRel, rel.getId() );
            }
            this.highestRelationshipId = highestRel;
        }
        
        @Override
        public boolean equals( Object obj )
        {
            NodeRep o = (NodeRep) obj;
            return o.properties.equals( properties ) && o.outRelationships.equals( outRelationships );
        }
        
        @Override
        public int hashCode()
        {
            int result = 7;
            result += properties.hashCode()*7;
            result += outRelationships.hashCode()*13;
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
