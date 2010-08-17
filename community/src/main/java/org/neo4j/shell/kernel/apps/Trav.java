/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.shell.kernel.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

/**
 * Traverses the graph using {@link Traverser}.
 */
@Service.Implementation( App.class )
public class Trav extends GraphDatabaseApp
{
    /**
     * Constructs a new command which can traverse the graph.
     */
    public Trav()
    {
        super();
        this.addOptionDefinition( "o", new OptionDefinition( OptionValueType.MUST,
            "The traversal order [BREADTH_FIRST/DEPTH_FIRST/breadth/depth]" ) );
        this.addOptionDefinition( "r", new OptionDefinition( OptionValueType.MUST,
            "The relationship type(s) expressed as a JSON string " +
            "(supports regex matching of the types) f.ex. " +
            "\"MY_REL_TYPE:out,.*_HAS_.*:both\". Matching is case-insensitive." ) );
        this.addOptionDefinition( "f", new OptionDefinition( OptionValueType.MUST,
            "Filters node property keys/values. Supplied either as a single " +
            "value or as a JSON string where both keys and values can " +
            "contain regex. Starting/ending {} brackets are optional. Examples:\n" +
            "\"username\"\n" +
            "   nodes which has property 'username' gets listed\n" +
            "\".*name: ma.*, age: ''\"\n" +
            "   nodes which has any key matching '.*name' where the " +
            "property value\n" +
            "   for that key matches 'ma.*' AND has the 'age' property gets listed" ) );
        this.addOptionDefinition( "i", new OptionDefinition( OptionValueType.NONE,
            "Filters are case-insensitive (case-sensitive by default)" ) );
        this.addOptionDefinition( "l", new OptionDefinition( OptionValueType.NONE,
            "Filters matches more loosely, i.e. it's considered a match if " +
            "just a part of a value matches the pattern, not necessarily " +
            "the whole value" ) );
        this.addOptionDefinition( "c", OPTION_DEF_FOR_C );
    }

    @Override
    public String getDescription()
    {
    	return "Traverses the node space from your current position (pwd). " +
    		"It's a reflection of the neo4j traverser API with some options for filtering " +
    		"which nodes will be returned.";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException, RemoteException
    {
        assertCurrentIsNode( session );

        Node node = this.getCurrent( session ).asNode();
        boolean caseInsensitiveFilters = parser.options().containsKey( "i" );
        boolean looseFilters = parser.options().containsKey( "l" );
        Object[] relationshipTypes = parseRelationshipTypes( parser, out,
            caseInsensitiveFilters, looseFilters );
        if ( relationshipTypes.length == 0 )
        {
            out.println( "No matching relationship types" );
            return null;
        }

        StopEvaluator stopEvaluator = parseStopEvaluator( parser );
        ReturnableEvaluator returnableEvaluator =
            parseReturnableEvaluator( parser );
        Order order = parseOrder( parser );

        String filterString = parser.options().get( "f" );
        Map<String, Object> filterMap = filterString != null ?
            parseFilter( filterString, out ) : null;
        String commandToRun = parser.options().get( "c" );
        Collection<String> commandsToRun = new ArrayList<String>();
        if ( commandToRun != null )
        {
            commandsToRun.addAll( Arrays.asList( commandToRun.split( Pattern.quote( "&&" ) ) ) );
        }
        for ( Node traversedNode : node.traverse( order, stopEvaluator,
            returnableEvaluator, relationshipTypes ) )
        {
            boolean hit = false;
            if ( filterMap == null )
            {
                hit = true;
            }
            else
            {
                Map<String, Boolean> matchPerFilterKey =
                    new HashMap<String, Boolean>();
                for ( String key : traversedNode.getPropertyKeys() )
                {
                    for ( Map.Entry<String, Object> filterEntry :
                        filterMap.entrySet() )
                    {
                        String filterKey = filterEntry.getKey();
                        if ( matchPerFilterKey.containsKey( filterKey ) )
                        {
                            continue;
                        }

                        if ( matches( newPattern( filterKey,
                            caseInsensitiveFilters ), key,
                            caseInsensitiveFilters, looseFilters ) )
                        {
                            Object value = traversedNode.getProperty( key );
                            String filterPattern =
                                filterEntry.getValue() != null ?
                                filterEntry.getValue().toString() : null;
                            if ( matches( newPattern( filterPattern,
                                caseInsensitiveFilters ), value.toString(),
                                caseInsensitiveFilters, looseFilters ) )
                            {
                                matchPerFilterKey.put( filterKey, true );
                            }
                        }
                    }
                }

                if ( matchPerFilterKey.size() == filterMap.size() )
                {
                    hit = true;
                }
            }
            if ( hit )
            {
                printAndInterpretTemplateLines( commandsToRun, false, true, traversedNode,
                        getServer(), session, out );
            }
        }
        return null;
    }

    private Order parseOrder( AppCommandParser parser )
    {
        return ( Order ) parseEnum( Order.class, parser.options().get( "o" ),
            Order.DEPTH_FIRST );
    }

    private ReturnableEvaluator parseReturnableEvaluator(
        AppCommandParser parser )
    {
        // TODO
        return ReturnableEvaluator.ALL_BUT_START_NODE;
    }

    private StopEvaluator parseStopEvaluator( AppCommandParser parser )
    {
        // TODO
        return StopEvaluator.END_OF_GRAPH;
    }

    private Object[] parseRelationshipTypes( AppCommandParser parser,
        Output out, boolean caseInsensitiveFilters, boolean looseFilters )
        throws ShellException, RemoteException
    {
        String option = parser.options().get( "r" );
        List<Object> result = new ArrayList<Object>();
        if ( option == null )
        {
            for ( RelationshipType type :
                getServer().getDb().getRelationshipTypes() )
            {
                result.add( type );
                result.add( Direction.BOTH );
            }
        }
        else
        {
            Map<String, Object> map = parseFilter( option, out );
            List<RelationshipType> allRelationshipTypes =
            	new ArrayList<RelationshipType>();
            for ( RelationshipType type :
            	getServer().getDb().getRelationshipTypes() )
            {
            	allRelationshipTypes.add( type );
            }

            for ( Map.Entry<String, Object> entry : map.entrySet() )
            {
                String type = entry.getKey();
                Direction direction = getDirection( ( String ) entry.getValue(),
                    Direction.BOTH );

                Pattern typePattern =
                    newPattern( type, caseInsensitiveFilters );
                for ( RelationshipType relationshipType : allRelationshipTypes )
                {
                    if ( relationshipType.name().equals( type ) ||
                    	matches( typePattern, relationshipType.name(),
                    	    caseInsensitiveFilters, looseFilters ) )
                    {
                        result.add( relationshipType );
                        result.add( direction );
                    }
                }
            }
        }
        return result.toArray();
    }
}
