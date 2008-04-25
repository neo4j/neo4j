package org.neo4j.util.matching;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;

public class PatternElement
{
	private PatternNode pNode;
	private Node node;
    private PatternRelationship prevPatternRel = null;
    private Relationship prevRel = null;
	
	PatternElement( PatternNode pNode, PatternRelationship pRel, 
        Node node, Relationship rel )
	{
		this.pNode = pNode;
		this.node = node;
        this.prevPatternRel = pRel;
        this.prevRel = rel;
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

    public PatternRelationship getFromPatternRelationship()
    {
        return prevPatternRel;
    }
    
    public Relationship getFromRelationship()
    {
        return prevRel;
    }
}
