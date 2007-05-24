package org.neo4j.api.core;

/**
 * Defines relationship directions used to when getting relationships from a 
 * node or when creating traversers.
 * <p>
 * A relationship has a direction from a node's point of view. If a node is 
 * the start node of a relationship it will be an {@link #OUTGOING}
 * relationship from that node's point of view. If a node is the end node of 
 * a relationship it will be an {@link #INCOMING} relationship from that 
 * node's point of view. The {@link #BOTH} direction is used when direction
 * is of no imporance, such as "give me all" or "traverse all" relationships
 * that are either {@link #OUTGOING} or {@link #INCOMING}. 
 */
public enum Direction
{
	/**
	 * Defines outgoing relationships.
	 */
	OUTGOING,
	/**
	 * Defines incoming relationships.
	 */
	INCOMING,
	/**
	 * Defines both incoming and outgoing relationships.
	 */
	BOTH;
	
	/**
	 * Reverses the direction returning {@link #INCOMING} if this equals 
	 * {@link #OUTGOING}, {@link #OUTGOING} if this equals {@link #INCOMING} or
	 * {@link #BOTH} if this equals {@link #BOTH}.
	 * 
	 * @return The reversed direction.
	 */
	public Direction reverse()
	{
		switch ( this )
		{
			case OUTGOING: return INCOMING;
			case INCOMING: return OUTGOING;
			case BOTH: return BOTH;
			default: throw new IllegalStateException( "Unknown Direction " +
				"enum: " + this );
		}
	}
}
