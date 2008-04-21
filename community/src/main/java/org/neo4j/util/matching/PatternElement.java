package org.neo4j.util.matching;

import org.neo4j.api.core.Node;

public class PatternElement
{
	private PatternNode pNode;
	private Node node;
	
	PatternElement( PatternNode pNode, Node node )
	{
		this.pNode = pNode;
		this.node = node;
	}
	
	public PatternNode getPatternNode()
	{
		return pNode;
	}
	
	public Node getNode()
	{
		return node;
	}
	
	@Override
	public String toString()
	{
		return pNode.toString();
	}
}
