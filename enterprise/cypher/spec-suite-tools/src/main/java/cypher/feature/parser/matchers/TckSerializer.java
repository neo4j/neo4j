/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.feature.parser.matchers;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Paths;

class TckSerializer
{
    private final StringBuilder sb;

    private TckSerializer( )
    {
        this.sb = new StringBuilder();
    }

    static String serialize( Object obj )
    {
        return new TckSerializer().innerSerialize( obj );
    }

    private String innerSerialize( Object obj )
    {
        if ( obj == null )
        {
            sb.append( "null" );
        }
        else if ( obj instanceof Node )
        {
            Node n = (Node) obj;
            sb.append( "(" );
            n.getLabels().forEach( ( l ) -> sb.append( ":" ).append( l.name() ) );
            sb.append( " {" );
            String[] comma = new String[]{ "" };
            n.getAllProperties().forEach( ( k, v ) -> {
                sb.append( comma[0] ).append( k ).append( ": " ).append( serialize( v ) );
                comma[0] = ", ";
            } );
            sb.append( "})" );
        }
        else if ( obj instanceof Relationship )
        {
            Relationship r = (Relationship) obj;
            sb.append( "[:" );
            sb.append( r.getType().name() );
            sb.append( " {" );
            String[] comma = new String[]{ "" };
            r.getAllProperties().forEach( ( k, v ) -> {
                sb.append( comma[0] ).append( k ).append( ": " ).append( serialize( v ) );
                comma[0] = ", ";
            } );
            sb.append( "}]" );
        }
        else if ( obj instanceof Path )
        {
            Path p = (Path) obj;
            sb.append( "<" );
            sb.append( Paths.pathToString( p, new Paths.PathDescriptor<Path>()
            {
                @Override
                public String nodeRepresentation( Path path, Node node )
                {
                    return serialize( node );
                }

                @Override
                public String relationshipRepresentation( Path path,
                        Node from, Relationship relationship )
                {
                    String prefix = "-", suffix = "-";
                    if ( from.equals( relationship.getEndNode() ) )
                    {
                        prefix = "<-";
                    }
                    else
                    {
                        suffix = "->";
                    }
                    return prefix + serialize( relationship ) + suffix;
                }
            } ) );
            sb.append( ">" );
        }
        else if ( obj instanceof String )
        {
            sb.append( "'" ).append( obj.toString() ).append( "'" );
        }
        else if ( obj instanceof List )
        {
            List<?> list = (List) obj;
            List<String> output = new ArrayList<>( list.size() );
            list.forEach( item -> output.add( serialize( item ) ) );
            sb.append( output );
        }
        else if ( obj.getClass().isArray() )
        {
            List<Object> list = new ArrayList<>();
            for ( int i = 0; i < Array.getLength( obj ); ++i )
            {
                list.add( Array.get( obj, i ) );
            }
            sb.append( serialize( list ) );
        }
        else if ( obj instanceof Map )
        {
            Map<?,?> map = (Map) obj;
            sb.append( "{" );
            String[] comma = new String[]{ "" };
            map.forEach( ( k, v ) -> {
                sb.append( comma[0] ).append( k ).append( ": " ).append( serialize( v ) );
                comma[0] = ", ";
            } );
            sb.append( "}" );
        }
        else
        {
            sb.append( obj.toString() );
        }

        return sb.toString();
    }

}
