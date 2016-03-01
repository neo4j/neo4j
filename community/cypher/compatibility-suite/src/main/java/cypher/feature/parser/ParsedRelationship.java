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

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class ParsedRelationship implements Relationship
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
    public Node getStartNode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node getEndNode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node getOtherNode( Node node )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Node[] getNodes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelationshipType getType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isType( RelationshipType type )
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
        if ( obj == this )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        else if ( obj instanceof ParsedRelationship )
        {
            ParsedRelationship other = (ParsedRelationship) obj;

            boolean typeEquality = this.getType().name().equals( other.getType().name() );
            boolean propEquality = getAllProperties().equals( other.getAllProperties() );
            return typeEquality && propEquality;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = hash * 23 + getType().hashCode();
        hash = hash * 23 + getAllProperties().hashCode();
        return hash;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder( "[:" );
        sb.append( getType().name() );
        if ( !getAllProperties().isEmpty() )
        {
            sb.append( " {" );
            getAllProperties().forEach( ( key, value ) -> sb.append( key ).append( ":" ).append( value.toString() ) );
            sb.append( "}" );
        }
        return sb.append( "]" ).toString();
    }

    public static Relationship parsedRelationship( final RelationshipType type, final Map<String,Object> properties )
    {
        return new ParsedRelationship()
        {
            @Override
            public Map<String,Object> getAllProperties()
            {
                return properties;
            }

            @Override
            public RelationshipType getType()
            {
                return type;
            }
        };
    }

    public static Relationship fromRealRelationship( Relationship real )
    {
        final RelationshipType type = real.getType();
        // need to fetch them before the tx closes
        final Map<String,Object> realProps = real.getAllProperties();
        return new ParsedRelationship()
        {
            @Override
            public Map<String,Object> getAllProperties()
            {
                return realProps;
            }

            @Override
            public RelationshipType getType()
            {
                return type;
            }
        };
    }
}
