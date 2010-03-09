package org.neo4j.graphmatching;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.commons.iterator.FilteringIterable;
import org.neo4j.graphdb.Node;
import org.neo4j.graphmatching.filter.AbstractFilterExpression;
import org.neo4j.graphmatching.filter.FilterBinaryNode;
import org.neo4j.graphmatching.filter.FilterExpression;
import org.neo4j.graphmatching.filter.FilterValueGetter;

public class PatternMatcher
{
	private static PatternMatcher matcher = new PatternMatcher();
	
	private PatternMatcher()
	{
	}
	
	public static PatternMatcher getMatcher()
	{
		return matcher;
	}
	
    public Iterable<PatternMatch> match( PatternNode start, 
        Node startNode )
    {
        return match( start, startNode, null );
    }
    
	public Iterable<PatternMatch> match( PatternNode start, 
		Node startNode, Map<String, PatternNode> objectVariables )
	{
		return match( start, startNode, objectVariables,
		    ( Collection<PatternNode> ) null );
	}
	
    public Iterable<PatternMatch> match( PatternNode start,
            Map<String, PatternNode> objectVariables,
            PatternNode... optional )
    {
        return match( start, objectVariables,
            Arrays.asList( optional ) );
    }
    
	public Iterable<PatternMatch> match( PatternNode start,
	        Map<String, PatternNode> objectVariables,
	        Collection<PatternNode> optional ) 
    {
	    Node startNode = start.getAssociation();
        if ( startNode == null )
        {
            throw new IllegalStateException( 
                    "Associating node for start pattern node is null" );
        }
	    return match( start, startNode, objectVariables, optional );
    }
	
	public Iterable<PatternMatch> match( PatternNode start,
		Node startNode, Map<String, PatternNode> objectVariables,
		Collection<PatternNode> optional )
	{
        Node currentStartNode = start.getAssociation();
        if ( currentStartNode != null && !currentStartNode.equals( startNode ) )
        {
            throw new IllegalStateException( 
                    "Start patter node already has associated " + 
                    currentStartNode + ", can not start with " + startNode );
        }
	    Iterable<PatternMatch> result = null;
		if ( optional == null || optional.size() < 1 )
		{
			result = new PatternFinder( this, start, startNode );
		}
		else
		{
			result = new PatternFinder( this, start, startNode, false,
			    optional );
		}
		
		if ( objectVariables != null )
		{
    		// Uses the FILTER expressions
    		result = new FilteredPatternFinder( result, objectVariables );
		}
		return result;
	}
	
	public Iterable<PatternMatch> match( PatternNode start,
		Node startNode, Map<String, PatternNode> objectVariables,
		PatternNode... optional )
	{
		return match( start, startNode, objectVariables,
		    Arrays.asList( optional ) );
	}
	
	private static class SimpleRegexValueGetter implements FilterValueGetter
	{
	    private PatternMatch match;
	    private Map<String, PatternNode> labelToNode =
	        new HashMap<String, PatternNode>();
	    private Map<String, String> labelToProperty =
	        new HashMap<String, String>();
	    
	    SimpleRegexValueGetter( Map<String, PatternNode> objectVariables,
	        PatternMatch match, FilterExpression[] expressions )
	    {
            this.match = match;
            for ( FilterExpression expression : expressions )
            {
                mapFromExpression( expression );
            }
            this.labelToNode = objectVariables;
	    }
	    
	    private void mapFromExpression( FilterExpression expression )
	    {
	        if ( expression instanceof FilterBinaryNode )
	        {
	            FilterBinaryNode node = ( FilterBinaryNode ) expression;
	            mapFromExpression( node.getLeftExpression() );
	            mapFromExpression( node.getRightExpression() );
	        }
	        else
	        {
	            AbstractFilterExpression pattern =
	                ( AbstractFilterExpression ) expression;
	            labelToProperty.put( pattern.getLabel(),
	                pattern.getProperty() );
	        }
	    }

        public String[] getValues( String label )
        {
            PatternNode pNode = labelToNode.get( label );
            if ( pNode == null )
            {
                throw new RuntimeException( "No node for label '" + label +
                    "'" );
            }
            Node node = this.match.getNodeFor( pNode );
            
            String propertyKey = labelToProperty.get( label );
            if ( propertyKey == null )
            {
                throw new RuntimeException( "No property key for label '" +
                    label + "'" );
            }
            
            Object rawValue = node.getProperty( propertyKey, null );
            if ( rawValue == null )
            {
                return new String[ 0 ];
            }
            
            Collection<Object> values =
                ArrayPropertyUtil.propertyValueToCollection( rawValue );
            String[] result = new String[ values.size() ];
            int counter = 0;
            for ( Object value : values )
            {
                result[ counter++ ] = ( String ) value;
            }
            return result;
        }
	}
	
	private static class FilteredPatternFinder
	    extends FilteringIterable<PatternMatch>
	{
	    private final Map<String, PatternNode> objectVariables;
	    
        public FilteredPatternFinder( Iterable<PatternMatch> source,
            Map<String, PatternNode> objectVariables )
        {
            super( source );
            this.objectVariables = objectVariables;
        }

        @Override
        protected boolean passes( PatternMatch item )
        {
            Set<PatternGroup> calculatedGroups = new HashSet<PatternGroup>();
            for ( PatternElement element : item.getElements() )
            {
                PatternNode node = element.getPatternNode();
                PatternGroup group = node.getGroup();
                if ( calculatedGroups.add( group ) )
                {
                    FilterValueGetter valueGetter = new SimpleRegexValueGetter(
                        objectVariables, item, group.getFilters() );
                    for ( FilterExpression expression : group.getFilters() )
                    {
                        if ( !expression.matches( valueGetter ) )
                        {
                            return false;
                        }
                    }
                }
            }
            return true;
        }
	}	
}
