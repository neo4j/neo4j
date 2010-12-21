/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.plugins;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Description( "An extension for accessing the reference node of the graph database, this can be used as the root for your graph." )
public class Plugin extends ServerPlugin
{
    public static final String GET_REFERENCE_NODE = "reference_node_uri";
    public static final String GET_CONNECTED_NODES = "connected_nodes";
    static String _string;
    static Byte _byte;
    static Character _character;
    static Integer _integer;
    static Short _short;
    static Long _long;
    static Float _float;
    static Double _double;
    static Boolean _boolean;
    static Long optional;
    static Set<String> stringSet;
    static List<String> stringList;
    static String[] stringArray;

    @Description( "Get the reference node from the graph database" )
    @PluginTarget( GraphDatabaseService.class )
    @Name( GET_REFERENCE_NODE )
    public Node getReferenceNode( @Source GraphDatabaseService graphDb )
    {
        return graphDb.getReferenceNode();
    }

    @Name( GET_CONNECTED_NODES )
    @PluginTarget( Node.class )
    public Iterable<Node> getAllConnectedNodes( @Source Node start )
    {
        ArrayList<Node> nodes = new ArrayList<Node>();

        for ( Relationship rel : start.getRelationships() )
        {
            nodes.add( rel.getOtherNode( start ) );
        }

        return nodes;
    }

    @PluginTarget( Node.class )
    public Node getThisNodeOrById( @Source Node start, @Parameter( name = "id", optional = true ) Long id )
    {
        optional = id;

        if ( id == null )
        {
            return start;
        }

        return start.getGraphDatabase().getNodeById( id );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithIntParam(
            @Source GraphDatabaseService db,
            @Parameter( name = "id", optional = false ) int id )
    {
        return db.getNodeById( id );
    }

    @PluginTarget( Relationship.class )
    public Iterable<Node> methodOnRelationship( @Source Relationship rel )
    {
        return Arrays.asList( rel.getNodes() );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithAllParams(
            @Source GraphDatabaseService db,
            @Parameter( name = "id", optional = false ) String a,
            @Parameter( name = "id2", optional = false ) Byte b,
            @Parameter( name = "id3", optional = false ) Character c,
            @Parameter( name = "id4", optional = false ) Short d,
            @Parameter( name = "id5", optional = false ) Integer e,
            @Parameter( name = "id6", optional = false ) Long f,
            @Parameter( name = "id7", optional = false ) Float g,
            @Parameter( name = "id8", optional = false ) Double h,
            @Parameter( name = "id9", optional = false ) Boolean i )
    {
        _string = a;
        _byte = b;
        _character = c;
        _short = d;
        _integer = e;
        _long = f;
        _float = g;
        _double = h;
        _boolean = i;

        return db.getReferenceNode();
    }
//
//    @PluginTarget( GraphDatabaseService.class )
//    public Node methodWithSet( @Source GraphDatabaseService db,
//                               @Parameter( name = "strings", optional = false ) Set<String> params )
//    {
//        stringSet = params;
//        return db.getReferenceNode();
//    }

//    @PluginTarget( GraphDatabaseService.class )
//    public Node methodWithList( @Source GraphDatabaseService db,
//                                @Parameter( name = "strings", optional = false ) List<String> params )
//    {
//        stringList = params;
//        return db.getReferenceNode();
//    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithArray( @Source GraphDatabaseService db,
                                 @Parameter( name = "strings", optional = false ) String[] params )
    {
        stringArray = params;
        return db.getReferenceNode();
    }


}
