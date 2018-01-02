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

import java.util.LinkedList;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

/**
 * Represents a pattern for matching a {@link Node}.
 */
@Deprecated
public class PatternNode extends AbstractPatternObject<Node>
{
    /**
     * The default {@link PatternGroup}.
     */
    // Should this really EVER be used? - mutable global state!!!!
    public static final PatternGroup DEFAULT_PATTERN_GROUP = new PatternGroup()
    /*{
        @Override
        public void addFilter( FilterExpression regexRepression )
        {
            throw new UnsupportedOperationException(
                    "Cannot add filter to default pattern group." );
        };
    }*/;

	private LinkedList<PatternRelationship> relationships =
		new LinkedList<PatternRelationship>();
	private LinkedList<PatternRelationship> optionalRelationships =
		new LinkedList<PatternRelationship>();

	private final PatternGroup group;

    /**
     * Create a new pattern node in the default {@link PatternGroup} with a
     * blank label.
     */
	public PatternNode()
	{
	    this( DEFAULT_PATTERN_GROUP, "" );
	}

    /**
     * Create a new pattern node in the default {@link PatternGroup} with the
     * specified label.
     *
     * @param label the label of this pattern node.
     */
	public PatternNode( String label )
	{
	    this( DEFAULT_PATTERN_GROUP, label );
	}

    /**
     * Create a new pattern node in the specified {@link PatternGroup} with a
     * blank label.
     *
     * @param group the {@link PatternGroup} of this pattern node.
     */
	public PatternNode( PatternGroup group )
	{
	    this( group, "" );
	}

    /**
     * Create a new pattern node in the specified {@link PatternGroup} with the
     * specified label.
     *
     * @param group the {@link PatternGroup} of this pattern node.
     * @param label the label of this pattern node.
     */
	public PatternNode( PatternGroup group, String label )
	{
	    this.group = group;
	    this.label = label;
	}

    /**
     * Get the {@link PatternGroup} of this pattern node.
     *
     * @return the {@link PatternGroup} this pattern node belongs to.
     */
	public PatternGroup getGroup()
	{
	    return this.group;
	}

    /**
     * Get all {@link PatternRelationship}s associated with this pattern node.
     * This includes both the required and the optional
     * {@link PatternRelationship}s.
     *
     * @return the {@link PatternRelationship}s associated with this pattern
     *         node.
     */
	public Iterable<PatternRelationship> getAllRelationships()
	{
		LinkedList<PatternRelationship> allRelationships =
			new LinkedList<PatternRelationship>();
		allRelationships.addAll( relationships );
		allRelationships.addAll( optionalRelationships );

		return allRelationships;
	}

    /**
     * Get the optional or the required {@link PatternRelationship}s associated
     * with this pattern node.
     *
     * @param optional if <code>true</code> return only the optional
     *            {@link PatternRelationship}s, else return only the required.
     * @return the set of optional or required {@link PatternRelationship}s.
     */
	public Iterable<PatternRelationship> getRelationships( boolean optional )
	{
		return optional ? optionalRelationships : relationships;
	}

	void addRelationship( PatternRelationship relationship, boolean optional )
	{
		if ( optional )
		{
			optionalRelationships.add( relationship );
		}
		else
		{
			relationships.add( relationship );
		}
	}

	void removeRelationship(
		PatternRelationship relationship, boolean optional )
	{
		if ( optional )
		{
			optionalRelationships.remove( relationship );
		}
		else
		{
			relationships.remove( relationship );
		}
	}

    /**
     * Create a directed, required {@link PatternRelationship} from this node,
     * to the specified other node.
     *
     * @param otherNode the node at the other end of the relationship.
     * @return the newly created {@link PatternRelationship}.
     */
    public PatternRelationship createRelationshipTo(
        PatternNode otherNode )
    {
        return this.createRelationshipTo( otherNode, false, true );
    }

    /**
     * Create a required {@link PatternRelationship} between this node and the
     * specified other node, with the specified direction.
     *
     * @param otherNode the node at the other end of the relationship.
     * @param dir the direction of the relationship. Use
     *            {@link Direction#OUTGOING} to create a relationship from this
     *            node to the other node. Use {@link Direction#INCOMING} to
     *            create a relationship from the other node to this node. Use
     *            {@link Direction#BOTH} to create a relationship where the
     *            direction does not matter.
     * @return the newly created {@link PatternRelationship}.
     */
    public PatternRelationship createRelationshipTo(
            PatternNode otherNode, Direction dir )
    {
        if ( dir == Direction.INCOMING )
            return otherNode.createRelationshipTo( this );
        return this.createRelationshipTo( otherNode, false,
                dir == Direction.BOTH ? false : true );
    }

    /**
     * Create a directed, required {@link PatternRelationship} of the specified
     * {@link RelationshipType} from this node to the specified other node.
     *
     * @param otherNode the node at the other end of the relationship.
     * @param type the {@link RelationshipType} of the relationship.
     * @return the newly created {@link PatternRelationship}.
     */
	public PatternRelationship createRelationshipTo(
		PatternNode otherNode, RelationshipType type )
	{
		return this.createRelationshipTo( otherNode, type, false, true );
	}

    /**
     * Create a required {@link PatternRelationship} of the specified
     * {@link RelationshipType} between this node and the specified other node,
     * with the specified direction.
     *
     * @param otherNode the node at the other end of the relationship.
     * @param type the {@link RelationshipType} of the relationship.
     * @param dir the direction of the relationship. Use
     *            {@link Direction#OUTGOING} to create a relationship from this
     *            node to the other node. Use {@link Direction#INCOMING} to
     *            create a relationship from the other node to this node. Use
     *            {@link Direction#BOTH} to create a relationship where the
     *            direction does not matter.
     * @return the newly created {@link PatternRelationship}.
     */
    public PatternRelationship createRelationshipTo(
        PatternNode otherNode, RelationshipType type, Direction dir )
    {
        if ( dir == Direction.INCOMING )
            return otherNode.createRelationshipTo( this, type );
        return this.createRelationshipTo( otherNode, type, false,
                dir == Direction.BOTH ? false : true );
    }

    /**
     * Create a directed, optional {@link PatternRelationship} from this node,
     * to the specified other node.
     *
     * @param otherNode the node at the other end of the relationship.
     * @return the newly created {@link PatternRelationship}.
     */
    public PatternRelationship createOptionalRelationshipTo(
        PatternNode otherNode )
    {
        return this.createRelationshipTo( otherNode, true, true );
    }

    /**
     * Create an optional {@link PatternRelationship} between this node and the
     * specified other node, with the specified direction.
     *
     * @param otherNode the node at the other end of the relationship.
     * @param dir the direction of the relationship. Use
     *            {@link Direction#OUTGOING} to create a relationship from this
     *            node to the other node. Use {@link Direction#INCOMING} to
     *            create a relationship from the other node to this node. Use
     *            {@link Direction#BOTH} to create a relationship where the
     *            direction does not matter.
     * @return the newly created {@link PatternRelationship}.
     */
    public PatternRelationship createOptionalRelationshipTo(
            PatternNode otherNode, Direction dir )
    {
        return this.createRelationshipTo( otherNode, true,
                dir == Direction.BOTH ? false : true );
    }

    /**
     * Create a directed, optional {@link PatternRelationship} of the specified
     * {@link RelationshipType} from this node to the specified other node.
     *
     * @param otherNode the node at the other end of the relationship.
     * @param type the {@link RelationshipType} of the relationship.
     * @return the newly created {@link PatternRelationship}.
     */
    public PatternRelationship createOptionalRelationshipTo(
        PatternNode otherNode, RelationshipType type )
    {
        return this.createRelationshipTo( otherNode, type, true, true );
    }

    /**
     * Create an optional {@link PatternRelationship} of the specified
     * {@link RelationshipType} between this node and the specified other node,
     * with the specified direction.
     *
     * @param otherNode the node at the other end of the relationship.
     * @param type the {@link RelationshipType} of the relationship.
     * @param dir the direction of the relationship. Use
     *            {@link Direction#OUTGOING} to create a relationship from this
     *            node to the other node. Use {@link Direction#INCOMING} to
     *            create a relationship from the other node to this node. Use
     *            {@link Direction#BOTH} to create a relationship where the
     *            direction does not matter.
     * @return the newly created {@link PatternRelationship}.
     */
    public PatternRelationship createOptionalRelationshipTo(
        PatternNode otherNode, RelationshipType type, Direction dir )
    {
        return this.createRelationshipTo( otherNode, type, true,
                dir == Direction.BOTH ? false : true );
    }

    PatternRelationship createRelationshipTo( PatternNode otherNode,
            boolean optional, boolean directed )
    {
        PatternRelationship relationship =
            new PatternRelationship( this, otherNode, optional, directed );
        addRelationship( relationship, optional );
        otherNode.addRelationship( relationship, optional );
        return relationship;
    }

    PatternRelationship createRelationshipTo(
            PatternNode otherNode, RelationshipType type, boolean optional,
            boolean directed )
    {
        PatternRelationship relationship =
            new PatternRelationship( type, this, otherNode, optional, directed );
        addRelationship( relationship, optional );
        otherNode.addRelationship( relationship, optional );
        return relationship;
    }

	@Override
	public String toString()
	{
		return this.label;
	}
}