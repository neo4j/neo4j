package org.neo4j.impl.nioneo.store;

/**
 * Wrapper class for the data contained in a relationship record.
 */
public class RelationshipData
{
	private final int id;
	private final int firstNode;
	private final int secondNode;
	private final int relType;

	/**
	 * @param id The id of the relationship
	 * @param directed Set to true if directed 
	 * @param firstNode The id of the first node
	 * @param secondNode The id of the second node
	 * @param relType The id of the relationship type
	 */
	public RelationshipData( int id, int firstNode, 
		int secondNode, int relType )
	{
		this.id = id;
		this.firstNode = firstNode;
		this.secondNode = secondNode;
		this.relType = relType;
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
	
	public String toString()
	{
		return "R[" + firstNode + 
			"," + secondNode + "," + relType + "] fN:";
	}
}

