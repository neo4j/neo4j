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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.shell.Output;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.util.json.JSONObject;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ShellSubGraphExporter
{
    private final SubGraph graph;
    private int batchSize;
    private final Set<Node> exportedNodes = new HashSet<Node>();
    private final Set<Relationship> exportedRelationships = new HashSet<Relationship>();

    ShellSubGraphExporter( SubGraph graph, final int batchSize )
    {
        this.graph = graph;
        this.batchSize = batchSize;
    }

    public void export( Output out ) throws RemoteException, ShellException
    {
        begin( out );
        for ( Node node : graph.getNodes() )
        {
            exportNode( node, out );
            batchIfNeeded( out );
            exportRelationships( node, out );
            removeVariableForNode( node, out );
        }
        commit( out );
        setVariables( out, "_n", "null", "_m", "null", "_props", "null" );
    }

    private void setVariables( Output out, String... nameValues ) throws RemoteException
    {
        final int size = nameValues.length;
        for ( int i = 0; i < size; i += 2 )
        {
            out.print( "export " + nameValues[i] + "=" + nameValues[i + 1] );
            if ( i < size - 2 )
            {
                out.print( " && " );
            }
        }
        out.println();
    }

    private void removeVariableForNode( Node node, Output out ) throws RemoteException
    {
        setVariables( out, "_" + node.getId(), "null" );
    }

    private void batchIfNeeded( Output out ) throws RemoteException
    {
        if ( needBatch() )
        {
            begin( out );
            commit( out );
        }
    }

    private boolean needBatch()
    {
        final int count = totalCount( exportedNodes, exportedRelationships );
        return count > 0 && count % this.batchSize == 0;
    }

    private int totalCount( Set<Node> exportedNodes, Set<Relationship> exportedRelationships )
    {
        return ( exportedNodes.size() + exportedRelationships.size() );
    }

    private void begin( Output out ) throws RemoteException
    {
        out.println( "begin" );
    }

    private void commit( Output out ) throws RemoteException
    {
        out.println( "commit" );
    }

    private void exportRelationships( Node node, Output out ) throws RemoteException
    {
        for ( Relationship relationship : node.getRelationships() )
        {
            if ( !graph.contains( relationship ) || exportedRelationships.contains( relationship ) )
            {
                continue;
            }
            exportNode( relationship.getOtherNode( node ), out );
            exportRelationship( relationship, out );
            exportedRelationships.add( relationship );
        }
    }

    private void exportRelationship( Relationship relationship, Output out ) throws RemoteException
    {
        setVariables( out, "_n", "_" + relationship.getStartNode().getId(),
                "_m", "_" + relationship.getEndNode().getId() );
        final String type = relationship.getType().name();

        final Map<String, Object> props = toMap( relationship );
        final String query = "start n=node({_n}), m=node({_m}) create n-[:`%s` %s]->m;";
        if ( props.isEmpty() )
        {
            out.println( String.format( query, type, "" ) );
        }
        else
        {
            setVariables( out, "_props", toJson( props ) );
            out.println( String.format( query, type, "{_props}" ) );
        }
    }

    private void exportNode( Node node, Output out ) throws RemoteException
    {
        if ( exportedNodes.contains( node ) )
        {
            return;
        }
        final Map<String, Object> props = toMap( node );
        if ( props.isEmpty() )
        {
            out.println( "create _n return _n;" );
        }
        else
        {
            setVariables( out, "_props", toJson( props ) );
            out.println( "create _n={_props} return _n;" );
        }
        setVariables( out, "_" + node.getId(), "_n" );
        exportedNodes.add( node );
    }

    private String toJson( Map<String, Object> properties )
    {
        return new JSONObject( properties ).toString();
    }

    Map<String, Object> toMap( PropertyContainer pc )
    {
        Map<String, Object> result = new TreeMap<String, Object>();
        for ( String prop : pc.getPropertyKeys() )
        {
            result.put( prop, pc.getProperty( prop ) );
        }
        return result;
    }
}