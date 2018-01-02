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
package org.neo4j.shell.apps.extra;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.kernel.apps.NonTransactionProvidingApp;

public class PathShellApp extends NonTransactionProvidingApp
{
    {
        addOptionDefinition( "a", new OptionDefinition( OptionValueType.MUST, "Which algorithm to use" ) );
        addOptionDefinition( "m", new OptionDefinition( OptionValueType.MUST, "Maximum depth to traverse" ) );
        addOptionDefinition( "f", new OptionDefinition( OptionValueType.MUST, "Relationship types and directions, f.ex: {KNOWS:out,LOVES:both}" ) );
        addOptionDefinition( "from", new OptionDefinition( OptionValueType.MUST, "Use some other star point than the current node" ) );
        addOptionDefinition( "q", new OptionDefinition( OptionValueType.NONE, "More quiet, print less verbose paths" ) );
        addOptionDefinition( "s", new OptionDefinition( OptionValueType.NONE, "Find max one path" ) );
    }
    
    @Override
    public String getDescription()
    {
        return "Displays paths from current (or any node) to another node using supplied algorithm. Usage:\n" +
                "\n# Find shortest paths from current to node 241 at max depth 10" +
        		"\npaths -m 10 -a shortestPath -f KNOWS:out,LOVES:in 241"; 
    }
    
    @Override
    public String getName()
    {
        return "paths";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out ) throws Exception
    {
        String fromString = parser.options().get( "from" );
        String toString = parser.argument( 0, "Must supply a 'to' node as first argument" );
        String algo = parser.options().get( "a" );
        String maxDepthString = parser.options().get( "m" );
        boolean quietPrint = parser.options().containsKey( "q" );
        boolean caseInsensitiveFilters = parser.options().containsKey( "i" );
        boolean looseFilters = parser.options().containsKey( "l" );
        
        int maxDepth = maxDepthString != null ? Integer.parseInt( maxDepthString ) : Integer.MAX_VALUE;
        fromString = fromString != null ? fromString : String.valueOf( this.getCurrent( session ).getId() );
        
        Map<String, Object> filter = parseFilter( parser.options().get( "f" ), out );
        PathExpander expander = toExpander( getServer().getDb(), Direction.BOTH, filter,
                caseInsensitiveFilters, looseFilters );
        PathFinder<Path> finder = expander != null ? getPathFinder( algo, expander, maxDepth, out ) : null;
        if ( finder != null )
        {
            Node fromNode = getNodeById( Long.parseLong( fromString ) );
            Node toNode = getNodeById( Long.parseLong( toString ) );
            boolean single = parser.options().containsKey( "s" );
            Iterable<Path> paths = single ? Arrays.asList( finder.findSinglePath( fromNode, toNode ) ) :
                    finder.findAllPaths( fromNode, toNode );
            for ( Path path : paths )
            {
                printPath( path, quietPrint, session, out );
            }
        }
        
        return Continuation.INPUT_COMPLETE;
    }

    private PathFinder<Path> getPathFinder( String algo, PathExpander expander, int maxDepth, Output out ) throws Exception
    {
        Method method;
        try
        {
            method = GraphAlgoFactory.class.getDeclaredMethod( algo, PathExpander.class, Integer.TYPE );
        }
        catch ( Exception e )
        {
            out.println( "Couldn't find algorithm '" + algo + "'" );
            return null;
        }
        return (PathFinder) method.invoke( null, expander, maxDepth );
    }
}
