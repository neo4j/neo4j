/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.plugins;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

@Description( "Here you can describe your plugin. It will show up in the description of the methods." )
public class FunctionalTestPlugin extends ServerPlugin
{
    public static final String CREATE_NODE = "createNode";
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
    public static int[] intArray;

    @Name( GET_CONNECTED_NODES )
    @PluginTarget( Node.class )
    public Iterable<Node> getAllConnectedNodes( @Source Node start )
    {
        ArrayList<Node> nodes = new ArrayList<>();
        try ( Transaction tx = start.getGraphDatabase().beginTx() )
        {
            for ( Relationship rel : start.getRelationships() )
            {
                nodes.add( rel.getOtherNode( start ) );
            }

            tx.success();
        }
        return nodes;
    }

    @PluginTarget( Node.class )
    public Iterable<Relationship> getRelationshipsBetween( final @Source Node start,
            final @Parameter( name = "other" ) Node end )
    {
        List<Relationship> result = new ArrayList<>();
        try ( Transaction tx = start.getGraphDatabase().beginTx() )
        {
            for ( Relationship relationship : start.getRelationships() )
            {
                if ( relationship.getOtherNode( start ).equals( end ) )
                {
                    result.add( relationship );
                }
            }
            tx.success();
        }
        return result;
    }

    @PluginTarget( Node.class )
    public Iterable<Relationship> createRelationships( @Source Node start,
            @Parameter( name = "type" ) RelationshipType type, @Parameter( name = "nodes" ) Iterable<Node> nodes )
    {
        List<Relationship> result = new ArrayList<>();
        try ( Transaction tx = start.getGraphDatabase().beginTx() )
        {
            for ( Node end : nodes )
            {
                result.add( start.createRelationshipTo( end, type ) );
            }
            tx.success();
        }
        return result;
    }

    @PluginTarget( Node.class )
    public Node getThisNodeOrById( @Source Node start, @Parameter( name = "id", optional = true ) Long id )
    {
        optional = id;

        if ( id == null )
        {
            return start;
        }

        try ( Transaction tx = start.getGraphDatabase().beginTx() )
        {
            Node node = start.getGraphDatabase().getNodeById( id );

            tx.success();
            return node;
        }
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node createNode( @Source GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();

            tx.success();
            return node;
        }
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithIntParam( @Source GraphDatabaseService db, @Parameter( name = "id", optional = false ) int id )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( id );

            tx.success();
            return node;
        }
    }

    @PluginTarget( Relationship.class )
    public Iterable<Node> methodOnRelationship( @Source Relationship rel )
    {
        try ( Transaction tx = rel.getGraphDatabase().beginTx() )
        {
            List<Node> nodes = Arrays.asList( rel.getNodes() );

            tx.success();
            return nodes;
        }
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithAllParams( @Source GraphDatabaseService db,
            @Parameter( name = "id", optional = false ) String a, @Parameter( name = "id2", optional = false ) Byte b,
            @Parameter( name = "id3", optional = false ) Character c,
            @Parameter( name = "id4", optional = false ) Short d,
            @Parameter( name = "id5", optional = false ) Integer e,
            @Parameter( name = "id6", optional = false ) Long f, @Parameter( name = "id7", optional = false ) Float g,
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

        return getOrCreateANode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithSet( @Source GraphDatabaseService db,
            @Parameter( name = "strings", optional = false ) Set<String> params )
    {
        stringSet = params;
        return getOrCreateANode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithList( @Source GraphDatabaseService db,
            @Parameter( name = "strings", optional = false ) List<String> params )
    {
        stringList = params;
        return getOrCreateANode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithListAndInt( @Source GraphDatabaseService db,
            @Parameter( name = "strings", optional = false ) List<String> params,
            @Parameter( name = "count", optional = false ) int i )
    {
        stringList = params;
        _integer = i;
        return getOrCreateANode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithArray( @Source GraphDatabaseService db,
            @Parameter( name = "strings", optional = false ) String[] params )
    {
        stringArray = params;
        return getOrCreateANode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithIntArray( @Source GraphDatabaseService db,
            @Parameter( name = "ints", optional = false ) int[] params )
    {
        intArray = params;
        return getOrCreateANode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithOptionalArray( @Source GraphDatabaseService db,
            @Parameter( name = "ints", optional = true ) int[] params )
    {
        intArray = params;
        return getOrCreateANode( db );
    }

    @PluginTarget( Node.class )
    public Path pathToReference( @Source Node me )
    {
        PathFinder<Path> finder = GraphAlgoFactory.shortestPath( PathExpanders.allTypesAndDirections(), 6 );
        try ( Transaction tx = me.getGraphDatabase().beginTx() )
        {
            Node other;
            if ( me.hasRelationship( DynamicRelationshipType.withName( "friend" ) ) )
            {
                other = me.getRelationships( DynamicRelationshipType.withName( "friend" ) )
                        .iterator()
                        .next()
                        .getOtherNode( me );
            }
            else
            {
                other = me.getGraphDatabase().createNode();
            }
            Path path = finder.findSinglePath( other, me );

            tx.success();
            return path;
        }
    }

    private Node getOrCreateANode( GraphDatabaseService db )
    {
        try(Transaction tx = db.beginTx())
        {
            Node node;
            try
            {
                node = db.getNodeById( 0l );
            } catch(NotFoundException e)
            {
                node = db.createNode();
            }
            tx.success();
            return node;
        }
    }

}
