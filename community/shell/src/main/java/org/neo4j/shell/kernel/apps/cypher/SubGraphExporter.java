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
package org.neo4j.shell.kernel.apps.cypher;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.shell.Output;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.TreeMap;

public class SubGraphExporter
{
    private final SubGraph graph;

    SubGraphExporter( SubGraph graph )
    {
        this.graph = graph;
    }

    public void export( Output out ) throws RemoteException, ShellException
    {
        begin( out );
        init( out );
        appendNodes( out );
        appendRelationships( out );
        commit( out );
    }

    private void begin( Output out ) throws RemoteException
    {
        out.println( "begin" );
    }

    private void commit( Output out ) throws RemoteException
    {
        out.println( "commit" );
    }

    private void init( Output out ) throws RemoteException, ShellException
    {
        final Node refNode = getReferenceNode();
        if ( refNode != null && refNode.hasRelationship() )
        {
            out.println( "start _0 = node(0) with _0 " );
            appendPropertySetters( out, refNode );
        }
    }

    private void appendPropertySetters( Output out, Node node ) throws RemoteException, ShellException
    {
        for ( String prop : node.getPropertyKeys() )
        {
            out.println( "set _0.`" + prop + "`=" + formatProperty( node.getProperty( prop ) ) );
        }
    }

    private Node getReferenceNode()
    {
        try
        {
            return graph.getReferenceNode();
        } catch ( NotFoundException nfe )
        {
            return null;
        }
    }

    private void appendRelationships( Output out ) throws RemoteException
    {
        for ( Node node : graph.getNodes() )
        {
            for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
            {
                appendRelationship( out, rel );
            }
        }
    }

    private void appendRelationship( Output out, Relationship rel ) throws RemoteException
    {
        out.print( "create " );
        formatNode( out, rel.getStartNode() );
        out.print( "-[:`" );
        out.print( rel.getType().name() );
        out.print( "`" );
        formatProperties( out, rel );
        out.print( "]->" );
        formatNode( out, rel.getEndNode() );
        out.println();
    }

    private void appendNodes( Output out ) throws RemoteException
    {
        for ( Node node : graph.getNodes() )
        {
            if ( isReferenceNode( node ) )
            {
                continue;
            }
            appendNode( out, node );
        }
    }

    private void appendNode( Output out, Node node ) throws RemoteException
    {
        out.print( "create (" );
        formatNode( out, node );
        formatProperties( out, node );
        out.println( ")" );
    }

    private boolean isReferenceNode( Node node )
    {
        return node.getId() == 0;
    }

    private void formatNode( Output out, Node n ) throws RemoteException
    {
        out.print( "_" );
        out.print( n.getId() );
    }

    private void formatProperties( Output out, PropertyContainer pc ) throws RemoteException
    {
        final Map<String, Object> properties = toMap( pc );
        if ( properties.isEmpty() )
        {
            return;
        }
        out.print( " " );
        final String propertyString = formatProperties( properties );
        out.print( propertyString );
    }

    private String formatProperties( Map<String, Object> properties )
    {
        final String jsonString = new JSONObject( properties ).toString();
        return removeNameQuotes( jsonString );
    }

    private String formatProperty( Object property ) throws ShellException
    {
        try
        {
            return JSONObject.valueToString( property );
        } catch ( JSONException e )
        {
            throw new ShellException( "Could not format " + property + " for dump" );
        }
    }

    private String removeNameQuotes( String json )
    {
        return json.replaceAll( "\"`([^\\´]+)`\":", "`$1`:" );
    }

    Map<String, Object> toMap( PropertyContainer pc )
    {
        Map<String, Object> result = new TreeMap<String, Object>();
        for ( String prop : pc.getPropertyKeys() )
        {
            final String quotedName = "`" + prop + "`";
            result.put( quotedName, pc.getProperty( prop ) );
        }
        return result;
    }
}