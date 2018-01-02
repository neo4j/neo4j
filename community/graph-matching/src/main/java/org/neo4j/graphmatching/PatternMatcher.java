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
package org.neo4j.graphmatching;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Node;
import org.neo4j.graphmatching.filter.AbstractFilterExpression;
import org.neo4j.graphmatching.filter.FilterBinaryNode;
import org.neo4j.graphmatching.filter.FilterExpression;
import org.neo4j.graphmatching.filter.FilterValueGetter;
import org.neo4j.helpers.collection.FilteringIterable;

/**
 * The PatternMatcher is the engine that performs the matching of a graph
 * pattern with the actual graph.
 */
@Deprecated
public class PatternMatcher
{
	private static PatternMatcher matcher = new PatternMatcher();

	private PatternMatcher()
	{
	}

    /**
     * Get the sole instance of the {@link PatternMatcher}.
     *
     * @return the instance of {@link PatternMatcher}.
     */
	public static PatternMatcher getMatcher()
	{
		return matcher;
	}

    /**
     * Find occurrences of the pattern defined by the given {@link PatternNode}
     * where the given {@link PatternNode} starts matching at the given
     * {@link Node}.
     *
     * @param start the {@link PatternNode} to start matching at.
     * @param startNode the {@link Node} to start matching at.
     * @return all matching instances of the pattern.
     */
    public Iterable<PatternMatch> match( PatternNode start,
        Node startNode )
    {
        return match( start, startNode, null );
    }

    /**
     * Find occurrences of the pattern defined by the given {@link PatternNode}
     * where the given {@link PatternNode} starts matching at the given
     * {@link Node}.
     *
     * @param start the {@link PatternNode} to start matching at.
     * @param startNode the {@link Node} to start matching at.
     * @param objectVariables mapping from names to {@link PatternNode}s.
     * @return all matching instances of the pattern.
     */
	public Iterable<PatternMatch> match( PatternNode start,
		Node startNode, Map<String, PatternNode> objectVariables )
	{
		return match( start, startNode, objectVariables,
		    ( Collection<PatternNode> ) null );
	}

    /**
     * Find occurrences of the pattern defined by the given {@link PatternNode}
     * where the given {@link PatternNode} starts matching at the given
     * {@link Node}.
     *
     * @param start the {@link PatternNode} to start matching at.
     * @param objectVariables mapping from names to {@link PatternNode}s.
     * @param optional nodes that form sub-patterns connected to this pattern.
     * @return all matching instances of the pattern.
     */
    public Iterable<PatternMatch> match( PatternNode start,
            Map<String, PatternNode> objectVariables,
            PatternNode... optional )
    {
        return match( start, objectVariables,
            Arrays.asList( optional ) );
    }

    /**
     * Find occurrences of the pattern defined by the given {@link PatternNode}
     * where the given {@link PatternNode} starts matching at the given
     * {@link Node}.
     *
     * @param start the {@link PatternNode} to start matching at.
     * @param objectVariables mapping from names to {@link PatternNode}s.
     * @param optional nodes that form sub-patterns connected to this pattern.
     * @return all matching instances of the pattern.
     */
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

    /**
     * Find occurrences of the pattern defined by the given {@link PatternNode}
     * where the given {@link PatternNode} starts matching at the given
     * {@link Node}.
     *
     * @param start the {@link PatternNode} to start matching at.
     * @param startNode the {@link Node} to start matching at.
     * @param objectVariables mapping from names to {@link PatternNode}s.
     * @param optional nodes that form sub-patterns connected to this pattern.
     * @return all matching instances of the pattern.
     */
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

    /**
     * Find occurrences of the pattern defined by the given {@link PatternNode}
     * where the given {@link PatternNode} starts matching at the given
     * {@link Node}.
     *
     * @param start the {@link PatternNode} to start matching at.
     * @param startNode the {@link Node} to start matching at.
     * @param objectVariables mapping from names to {@link PatternNode}s.
     * @param optional nodes that form sub-patterns connected to this pattern.
     * @return all matching instances of the pattern.
     */
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
        public FilteredPatternFinder( Iterable<PatternMatch> source,
            final Map<String, PatternNode> objectVariables )
        {
            super( source, new Predicate<PatternMatch>()
            {
                public boolean test( PatternMatch item )
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
            } );
        }
	}
}
