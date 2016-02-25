/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package cypher.feature.parser;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class ParsedNode implements Node
{
    @Override
    public long getId()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> getRelationships()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRelationship()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRelationship( RelationshipType... types )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRelationship( Direction direction, RelationshipType... types )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction dir )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRelationship( Direction dir )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType type, Direction dir )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Relationship getSingleRelationship( RelationshipType type, Direction dir )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Relationship createRelationshipTo( Node otherNode, RelationshipType type )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDegree()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDegree( RelationshipType type )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDegree( Direction direction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getDegree( RelationshipType type, Direction direction )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addLabel( Label label )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeLabel( Label label )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasLabel( Label label )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Label> getLabels()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasProperty( String key )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getProperty( String key )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setProperty( String key, Object value )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object removeProperty( String key )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String,Object> getProperties( String... keys )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String,Object> getAllProperties()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj == null )
        {
            return false;
        }
        else if ( obj instanceof ParsedNode )
        {
            ParsedNode other = (ParsedNode) obj;

            Set<Label> thisLabels = new HashSet<>();
            getLabels().forEach( thisLabels::add );
            Set<Label> otherLabels = new HashSet<>();
            other.getLabels().forEach( otherLabels::add );
            boolean labelEquality = thisLabels.equals( otherLabels );
            boolean propEquality = getAllProperties().equals( other.getAllProperties() );
            return labelEquality && propEquality;
        }
        return false;
    }

    public static Node parsedNode(final Iterable<Label> labelNames, final Map<String, Object> properties) {
        return new ParsedNode() {
            @Override
            public Map<String,Object> getAllProperties()
            {
                return properties;
            }

            @Override
            public Iterable<Label> getLabels()
            {
                return labelNames;
            }
        };
    }
}
