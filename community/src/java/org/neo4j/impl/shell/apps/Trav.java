package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
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
            "The relationship type(s) w/ optional direction\n" +
            "(also supports regex matching of relationship types),\n" +
            "f.ex. MY_REL_TYPE:OUTGOING,.*_HAS_.*'" ) );
        this.addValueType( "f", new OptionContext( OptionValueType.MUST,
            "Filters node property keys (regex string)" ) );
        this.addValueType( "g", new OptionContext( OptionValueType.MUST,
            "Filters node property values (regex string)" ) );
        this.addValueType( "s", new OptionContext( OptionValueType.NONE,
            "Case sensitive filters" ) );
        this.addValueType( "x", new OptionContext( OptionValueType.NONE,
            "Filters will only match if the entire value matches " +
            "(exact match)" ) );
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
        Node node = this.getCurrentNode( session );
        Object[] relationshipTypes = parseRelationshipTypes( parser );
        if ( relationshipTypes.length == 0 )
        {
            out.println( "No matching relationship types" );
            return null;
        }
        
        StopEvaluator stopEvaluator = parseStopEvaluator( parser );
        ReturnableEvaluator returnableEvaluator =
            parseReturnableEvaluator( parser );
        Order order = parseOrder( parser );
        
        String nodeKeyFilter = parser.options().get( "f" );
        String nodeValueFilter = parser.options().get( "g" );
        boolean caseSensitiveFilters = parser.options().containsKey( "s" );
        boolean exactFilterMatch = parser.options().containsKey( "x" );
        Pattern nodeKeyPattern =
            newPattern( nodeKeyFilter, caseSensitiveFilters );
        Pattern nodeValuePattern =
            newPattern( nodeValueFilter, caseSensitiveFilters );
        String commandToRun = parser.options().get( "c" );
        String[] commandsToRun = commandToRun != null ?
            commandToRun.split( Pattern.quote( "&&" ) ) : new String[ 0 ];
        
        for ( Node traversedNode : node.traverse( order, stopEvaluator,
            returnableEvaluator, relationshipTypes ) )
        {
            boolean hit = false;
            if ( nodeKeyFilter == null && nodeValueFilter == null )
            {
                hit = true;
            }
            else
            {
                for ( String key : traversedNode.getPropertyKeys() )
                {
                    hit = matches( nodeKeyPattern, key, caseSensitiveFilters,
                        exactFilterMatch );
                    Object value = traversedNode.getProperty( key );
                    hit = hit && matches( nodeValuePattern, value.toString(),
                        caseSensitiveFilters, exactFilterMatch );
                    if ( hit )
                    {
                        break;
                    }
                }
            }
            if ( hit )
            {
        		out.println( getDisplayNameForNode( traversedNode ) );
                Map<String, Object> data = new HashMap<String, Object>();
                data.put( "n", traversedNode.getId() );
        	    for ( String command : commandsToRun )
        	    {
            		String line = templateString( command, "\\$", data );
            		getServer().interpretLine( line, session, out );
            		out.println();
            	}
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

    private Object[] parseRelationshipTypes( AppCommandParser parser )
        throws ShellException
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
            StringTokenizer typeTokenizer = new StringTokenizer( option, "," );
            List<RelationshipType> allRelationshipTypes =
            	new ArrayList<RelationshipType>();
            for ( RelationshipType type :
            	getNeoServer().getNeo().getRelationshipTypes() )
            {
            	allRelationshipTypes.add( type );
            }
            
            while ( typeTokenizer.hasMoreTokens() )
            {
                String typeToken = typeTokenizer.nextToken();
                StringTokenizer directionTokenizer = new StringTokenizer(
                    typeToken, ":" );
                String type = directionTokenizer.nextToken();
                Direction direction = getDirection(
                    directionTokenizer.hasMoreTokens() ?
                        directionTokenizer.nextToken() : null,
                        Direction.BOTH );
                
                Pattern typePattern = Pattern.compile( type );
                for ( RelationshipType relationshipType : allRelationshipTypes )
                {
                    if ( relationshipType.name().equals( type ) ||
                    	matches( typePattern, relationshipType.name(),
                        false, true ) )
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
