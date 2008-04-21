package org.neo4j.util.matching;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.api.core.Node;

public class PatternMatch
{
	private Map<PatternNode,PatternElement> elements = 
		new HashMap<PatternNode, PatternElement>();
	
	PatternMatch( Map<PatternNode,PatternElement> elements )
	{
		this.elements = elements;
	}
	
	public Node getNodeFor( PatternNode node )
	{
		return elements.containsKey( node ) ?
			elements.get( node ).getNode() : null;
	}
	
	public Iterable<PatternElement> getElements()
	{
		return elements.values();
	}

	public static PatternMatch merge( Iterable<PatternMatch> matches )
	{
		Map<PatternNode, PatternElement> matchMap =
			new HashMap<PatternNode, PatternElement>();
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
				}
			}
		}
		PatternMatch mergedMatch = new PatternMatch( matchMap );
		return mergedMatch;
	}

	public static PatternMatch merge( PatternMatch... matches )
	{
		return merge( Arrays.asList( matches ) );
	}
}
