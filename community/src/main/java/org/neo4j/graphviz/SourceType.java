package org.neo4j.graphviz;

/**
 * Represents where a Property is locates, {@link #NODE} or
 * {@link #RELATIONSHIP}.
 * @author Tobias Ivarsson
 */
public enum SourceType
{
	/**
	 * Represents that a property comes from a node.
	 */
	NODE,
	/**
	 * Represents that a property comes from a relationship.
	 */
	RELATIONSHIP
}
