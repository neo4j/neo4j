/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
            n.getAllProperties().forEach( ( k, v ) ->
            {
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
            r.getAllProperties().forEach( ( k, v ) ->
            {
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
            map.forEach( ( k, v ) ->
            {
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
