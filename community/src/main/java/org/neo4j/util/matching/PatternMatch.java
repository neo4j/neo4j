package org.neo4j.util.matching;

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
		return elements.get( node ).getNode();
	}
	
	public Iterable<PatternElement> getElements()
	{
		return elements.values();
	}
}
