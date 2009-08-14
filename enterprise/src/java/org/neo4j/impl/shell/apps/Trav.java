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
package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.OptionValueType;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class Trav extends NeoApp
{
    public Trav()
    {
        super();
        this.addValueType( "o", new OptionContext( OptionValueType.MUST,
            "The traversal order [BREADTH_FIRST/DEPTH_FIRST/breadth/depth]" ) );
        this.addValueType( "r", new OptionContext( OptionValueType.MUST,
            "The relationship type(s) expressed as a JSON string " +
            "(supports regex\n" +
            "matching of the types) f.ex. " +
            "\"MY_REL_TYPE:out,.*_HAS_.*:both\".\n" +
            "Matching is case-insensitive." ) );
        this.addValueType( "f", new OptionContext( OptionValueType.MUST,
            "Filters node property keys/values. Supplied either as a single " +
            "value\n" +
            "or as a JSON string where both keys and values can " +
            "contain regex.\n" +
            "Starting/ending {} brackets are optional. Examples:\n" +
            "\"username\"\n" +
            "   nodes which has property 'username' gets listed\n" +
            "\".*name: ma.*, age: ''\"\n" +
            "   nodes which has any key matching '.*name' where the " +
            "property value\n" +
            "   for that key matches 'ma.*' AND has the 'age' property " +
            "gets listed" ) );
        this.addValueType( "i", new OptionContext( OptionValueType.NONE,
            "Filters are case-insensitive (case-sensitive by default)" ) );
        this.addValueType( "l", new OptionContext( OptionValueType.NONE,
            "Filters matches more loosely, i.e. it's considered a match if " +
            "just\n" +
            "a part of a value matches the pattern, not necessarily " +
            "the whole value" ) );
        this.addValueType( "c", new OptionContext( OptionValueType.MUST,
        	"Command to run for each returned node. Use $n as a node-id " +
        	"replacement.\n" +
        	"Example: -c \"ls -f name $n\". Multiple commands " +
        	"can be supplied with\n" +
        	"&& in between" ) );
    }
    
    @Override
    public String getDescription()
    {
    	return "Traverses the node space from your current position (pwd). " +
    		"It's a reflection\n" +
    		"of the neo4j traverser API with some options for filtering " +
    		"which nodes\n" +
    		"will be returned.";
    }
    
    @Override
    protected String exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException, RemoteException
    {
        assertCurrentIsNode( session );
        
        Node node = this.getCurrent( session ).asNode();
        Object[] relationshipTypes = parseRelationshipTypes( parser, out );
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
        boolean caseInsensitiveFilters = parser.options().containsKey( "i" );
        boolean looseFilters = parser.options().containsKey( "l" );
        String commandToRun = parser.options().get( "c" );
        String[] commandsToRun = commandToRun != null ?
            commandToRun.split( Pattern.quote( "&&" ) ) : new String[ 0 ];
        
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
        		out.println( getDisplayName( getNeoServer(), session,
        		    traversedNode ) );
                Map<String, Object> data = new HashMap<String, Object>();
                data.put( "n", traversedNode.getId() );
        	    for ( String command : commandsToRun )
        	    {
            		String line = templateString( command, "\\$", data );
            		getServer().interpretLine( line, session, out );
            	}
                out.println();
            }
        }
        return null;
    }
    
	private String templateString( String templateString, String variablePrefix,
		Map<String, Object> data )
	{
		// Sort data strings on length.
		Map<Integer, List<String>> lengthMap =
			new HashMap<Integer, List<String>>();
		int longest = 0;
		for ( String key : data.keySet() )
		{
			int length = key.length();
			if ( length > longest )
			{
				longest = length;
			}
			
			List<String> innerList = null;
			Integer innerKey = Integer.valueOf( length );
			if ( lengthMap.containsKey( innerKey ) )
			{
				innerList = lengthMap.get( innerKey );
			}
			else
			{
				innerList = new ArrayList<String>();
				lengthMap.put( innerKey, innerList );
			}
			innerList.add( key );
		}
		
		// Replace it.
		String result = templateString;
		for ( int i = longest; i >= 0; i-- )
		{
			Integer lengthKey = Integer.valueOf( i );
			if ( !lengthMap.containsKey( lengthKey ) )
			{
				continue;
			}
			
			List<String> list = lengthMap.get( lengthKey );
			for ( String key : list )
			{
				String replacement = data.get( key ).toString();
				String regExpMatchString = variablePrefix + key;
				result = result.replaceAll( regExpMatchString, replacement );
			}
		}
		
		return result;
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
        Output out ) throws ShellException, RemoteException
    {
        String option = parser.options().get( "r" );
        List<Object> result = new ArrayList<Object>();
        if ( option == null )
        {
            for ( RelationshipType type :
                getNeoServer().getNeo().getRelationshipTypes() )
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
            	getNeoServer().getNeo().getRelationshipTypes() )
            {
            	allRelationshipTypes.add( type );
            }
            
            for ( Map.Entry<String, Object> entry : map.entrySet() )
            {
                String type = entry.getKey();
                Direction direction = getDirection( ( String ) entry.getValue(),
                    Direction.BOTH );
                
                Pattern typePattern = Pattern.compile( type );
                for ( RelationshipType relationshipType : allRelationshipTypes )
                {
                    if ( relationshipType.name().equals( type ) ||
                    	matches( typePattern, relationshipType.name(),
                        true, false ) )
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
