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
package org.neo4j.server.plugins;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.FilteringIterable;
import org.neo4j.kernel.Traversal;

@Description( "Here you can describe your plugin. It will show up in the description of the methods." )
public class FunctionalTestPlugin extends ServerPlugin
{
    // TODO Remove this when the reference node is removed and fix the rest of the plugin tests
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
    public static int[] intArray;

    // TODO remove when the reference node is no more
    @Description( "Get the reference node from the graph database - deprecated functionality" )
    @PluginTarget( GraphDatabaseService.class )
    @Name( GET_REFERENCE_NODE )
    public Node getReferenceNode( @Source GraphDatabaseService graphDb )
    {
        return referenceNode( graphDb );
    }

    @Name( GET_CONNECTED_NODES )
    @PluginTarget( Node.class )
    public Iterable<Node> getAllConnectedNodes( @Source Node start )
    {
        ArrayList<Node> nodes = new ArrayList<>();
        Transaction tx = start.getGraphDatabase().beginTx();
        try
        {
            for ( Relationship rel : start.getRelationships() )
            {
                nodes.add( rel.getOtherNode( start ) );
            }

            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return nodes;
    }

    @PluginTarget( Node.class )
    public Iterable<Relationship> getRelationshipsBetween( final @Source Node start,
            final @Parameter( name = "other" ) Node end )
    {
        Transaction tx = start.getGraphDatabase().beginTx();
        try
        {
            return new FilteringIterable<>( start.getRelationships(), new Predicate<Relationship>()
            {
                @Override
                public boolean accept( Relationship item )
                {
                    return item.getOtherNode( start ).equals( end );
                }
            } );
        }
        finally
        {
            tx.finish();
        }
    }

    @PluginTarget( Node.class )
    public Iterable<Relationship> createRelationships( @Source Node start,
            @Parameter( name = "type" ) RelationshipType type, @Parameter( name = "nodes" ) Iterable<Node> nodes )
    {
        List<Relationship> result = new ArrayList<>();
        Transaction tx = start.getGraphDatabase().beginTx();
        try
        {
            for ( Node end : nodes )
            {
                result.add( start.createRelationshipTo( end, type ) );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
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

        Transaction tx = start.getGraphDatabase().beginTx();
        try
        {
            Node node = start.getGraphDatabase().getNodeById( id );

            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithIntParam( @Source GraphDatabaseService db, @Parameter( name = "id", optional = false ) int id )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.getNodeById( id );

            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }

    @PluginTarget( Relationship.class )
    public Iterable<Node> methodOnRelationship( @Source Relationship rel )
    {
        Transaction tx = rel.getGraphDatabase().beginTx();
        try
        {
            List<Node> nodes = Arrays.asList( rel.getNodes() );

            tx.success();
            return nodes;
        }
        finally
        {
            tx.finish();
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

        return referenceNode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithSet( @Source GraphDatabaseService db,
            @Parameter( name = "strings", optional = false ) Set<String> params )
    {
        stringSet = params;
        return referenceNode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithList( @Source GraphDatabaseService db,
            @Parameter( name = "strings", optional = false ) List<String> params )
    {
        stringList = params;
        return referenceNode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithListAndInt( @Source GraphDatabaseService db,
            @Parameter( name = "strings", optional = false ) List<String> params,
            @Parameter( name = "count", optional = false ) int i )
    {
        stringList = params;
        _integer = i;
        return referenceNode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithArray( @Source GraphDatabaseService db,
            @Parameter( name = "strings", optional = false ) String[] params )
    {
        stringArray = params;
        return referenceNode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithIntArray( @Source GraphDatabaseService db,
            @Parameter( name = "ints", optional = false ) int[] params )
    {
        intArray = params;
        return referenceNode( db );
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithOptionalArray( @Source GraphDatabaseService db,
            @Parameter( name = "ints", optional = true ) int[] params )
    {
        intArray = params;
        return referenceNode( db );
    }

    @PluginTarget( Node.class )
    public Path pathToReference( @Source Node me )
    {
        PathFinder<Path> finder = GraphAlgoFactory.shortestPath( Traversal.expanderForAllTypes(), 6 );
        Transaction tx = me.getGraphDatabase().beginTx();
        try
        {
            @SuppressWarnings("deprecation")
            Path path = finder.findSinglePath( me.getGraphDatabase().getReferenceNode(), me );

            tx.success();
            return path;
        }
        finally
        {
            tx.finish();
        }
    }

    private Node referenceNode( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        try
        {
            @SuppressWarnings("deprecation")
            Node node = db.getReferenceNode();

            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }
}
