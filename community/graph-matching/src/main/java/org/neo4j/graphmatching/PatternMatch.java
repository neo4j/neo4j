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
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Represents one match found by the {@link PatternMatcher}. The match is
 * itself a graph which looks like the pattern fed to the {@link PatternMatcher}
 */
@Deprecated
public class PatternMatch
{
	private Map<PatternNode,PatternElement> elements =
		new HashMap<PatternNode, PatternElement>();
	private Map<PatternRelationship,Relationship> relElements =
        new HashMap<PatternRelationship,Relationship>();

	PatternMatch( Map<PatternNode,PatternElement> elements,
        Map<PatternRelationship,Relationship> relElements )
	{
		this.elements = elements;
        this.relElements = relElements;
	}

	/**
	 * @param node the {@link PatternNode} to get the {@link Node} for.
	 * @return the actual {@link Node} for this particular match, represented
     * by {@code node} in the pattern
	 */
	public Node getNodeFor( PatternNode node )
	{
		return elements.containsKey( node ) ?
			elements.get( node ).getNode() : null;
	}

    /**
     * @param rel the {@link PatternRelationship} to get the
     * {@link Relationship} for.
     * @return the actual {@link Relationship} for this particular match,
     * represented by {@code rel} in the pattern
     */
    public Relationship getRelationshipFor( PatternRelationship rel )
    {
        return relElements.containsKey( rel ) ?
            relElements.get( rel ) : null;
    }

    /**
     * Get the matched elements in this match.
     *
     * @return an iterable over the matched elements in this match instance.
     */
	public Iterable<PatternElement> getElements()
	{
		return elements.values();
	}

	/**
	 * Used to merge two matches. An example is to merge in an "optional"
	 * subgraph match into a match.
	 * @param matches the matches to merge together.
	 * @return the merged matches as one match.
	 */
	public static PatternMatch merge( Iterable<PatternMatch> matches )
	{
		Map<PatternNode, PatternElement> matchMap =
			new HashMap<PatternNode, PatternElement>();
        Map<PatternRelationship, Relationship> relElements =
            new HashMap<PatternRelationship, Relationship>();
		for ( PatternMatch match : matches )
		{
			for ( PatternNode node : match.elements.keySet() )
			{
				boolean exists = false;
				for ( PatternNode existingNode : matchMap.keySet() )
				{
					if ( node.getLabel().equals( existingNode.getLabel() ) )
					{
						exists = true;
						break;
					}
				}
				if ( !exists )
				{
					matchMap.put( node, match.elements.get( node ) );
                    relElements.put(
                        match.elements.get( node ).getFromPatternRelationship(),
                        match.elements.get( node ).getFromRelationship() );
				}
			}
		}
		PatternMatch mergedMatch = new PatternMatch( matchMap, relElements );
		return mergedMatch;
	}

    /**
     * Used to merge matches. An example is to merge in an "optional" subgraph
     * match into a match.
     * 
     * @param matches the matches to merge together.
     * @return the merged matches as one match.
     */
	public static PatternMatch merge( PatternMatch... matches )
	{
		return merge( Arrays.asList( matches ) );
	}
}
