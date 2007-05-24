package org.neo4j.impl.nioneo.store;

/**
 * Wrapper class for the data contained in a relationship record.
 */
public class RelationshipData
{
	private int id;
	private int firstNode;
	private int secondNode;
	private int relType;
	private int firstNodePreviousRelId;
	private int firstNodeNextRelId;
	private int secondNodePreviousRelId;
	private int secondNodeNextRelId;
	private int nextPropertyId;

	/**
	 * @param id The id of the relationship
	 * @param directed Set to true if directed 
	 * @param firstNode The id of the first node
	 * @param secondNode The id of the second node
	 * @param relType The id of the relationship type
	 * @param firstNodePreviousRelId The id of the first node's previous 
	 * relationship, -1 if this is first relationship
	 * @param firstNodeNextRelId The id of the first node's next 
	 * relationship, -1 if this is last relationship
	 * @param secondNodePreviousRelId The id of the second node's previous 
	 * relationship, -1 if this is first relationship
	 * @param secondNodeNextRelId The id of the second node's next
	 * relationship, -1 if this is last relationship
	 * @param nextPropertyId id of this relationships first property, -1
	 * if no property
	 */
	public RelationshipData( int id, int firstNode, 
		int secondNode, int relType, 
		int firstNodePreviousRelId, int firstNodeNextRelId, 
		int secondNodePreviousRelId, int secondNodeNextRelId, 
		int nextPropertyId )
	{
		this.id = id;
		this.firstNode = firstNode;
		this.secondNode = secondNode;
		this.relType = relType;
		this.firstNodePreviousRelId = firstNodePreviousRelId;
		this.firstNodeNextRelId = firstNodeNextRelId;
		this.secondNodePreviousRelId = secondNodePreviousRelId;
		this.secondNodeNextRelId = secondNodeNextRelId;
		this.nextPropertyId = nextPropertyId;
	}
	
	public int getId()
	{
		return id;
	}
	
	public int firstNode()
	{
		return firstNode;
	}
	
	public int secondNode()
	{
		return secondNode;
	}
	
	public int relationshipType()
	{
		return relType;
	}
	
	public int firstNodePreviousRelationshipId()
	{
		return firstNodePreviousRelId;
	}

	public int firstNodeNextRelationshipId()
	{
		return firstNodeNextRelId;
	}
	
	public int secondNodePreviousRelationshipId()
	{
		return secondNodePreviousRelId;
	}

	public int secondNodeNextRelationshipId()
	{
		return secondNodeNextRelId;
	}
	
	public int nextPropertyId()
	{
		return nextPropertyId;
	}
	
	public String toString()
	{
		return "R[" + firstNode + 
			"," + secondNode + "] fN:" + firstNodeNextRelationshipId() + 
			" fP:" + firstNodePreviousRelationshipId() + 
			" sN:" + secondNodeNextRelationshipId() +
			" sP:" + secondNodePreviousRelationshipId();
	}
}

