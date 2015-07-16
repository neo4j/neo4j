/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.messaging.v1.infrastructure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * A ValuePath is an implementation of the Path interface that is used to
 * represent a walk through a graph within the context of remoting. It is
 * particularly important to ensure a compact (though obviously lossless)
 * on-wire representation of a Path as a naive implementation may contain
 * duplicate information thereby increasing the number of bytes required
 * to be transmitted.
 * <p/>
 * The representation used by Neo4jPack and expected by the ValuePath
 * constructor here consists of three parts: a unique set of nodes within
 * the path, a similar unique set of unbound relationships (i.e. without
 * start and end information) and a list of path sequence information that
 * refers to the entities themselves. In PackStream structural representation,
 * this is as follows:
 * <p/>
 * <pre>
 *     Structure Path 'P' {
 *         List&lt;Node&gt; nodes
 *         List&lt;UnboundRelationship&gt; relationships
 *         List&lt;Integer&gt; sequence
 *     }
 *
 *     Structure UnboundRelationship 'r' {
 *         Identity identity
 *         String type
 *         Map&lt;String,Object&gt; properties
 *     }
 * </pre>
 * <p/>
 * Within the Path structure, several caveats apply. Firstly, while the nodes
 * may generally be listed in any order, a valid Path must contain at least
 * one node and the first such node must correspond to the path's start node.
 * The relationship list however may be empty (for zero length paths) and
 * relationships may appear in any order with no restriction.
 * <p/>
 * The sequence information contains alternating pointers to nodes and
 * relationships in the order in which they appear in the path. Each pointer
 * takes the form of an integer, which in the case of nodes simply references
 * the zero-based index of that node.
 * <p/>
 * Relationship pointers are less straightforward, using a one-based indexing
 * system (i.e. relationship #1 refers to the item 0 from the relationship
 * list. Additionally, the sign of the index is relevant: a positive index
 * denotes that the relationship was traversed with its direction, a
 * negative index denotes that the relationship was traversed against its
 * direction. Note that relationship items do not contain endpoint information,
 * this is described by this sequential context.
 * <p/>
 * Finally, the first sequence value will always be 0 by definition. Therefore,
 * this value is never actually transmitted and is instead reconstituted by the
 * receiving agent.
 * <p/>
 * Take as an example, the following path:
 * <pre>
 *     (a)--&gt;(b)&lt;--(a)--&gt;(c)--&gt;(d)
 * </pre>
 * Here, the nodes would be uniquely listed as [(a), (b), (c), (d)] and the
 * relationships as [(a:b), (a:c), (c:d)]. Applying zero-based and one-based
 * indexing to these lists respectively, we get:
 * <pre>
 *       0    1    2    3         1      2      3
 *     [(a), (b), (c), (d)]    [(a:b), (a:c), (c:d)]
 * </pre>
 * Applying these indexes to the original path then gives us:
 * <pre>
 *               (a)--&gt;(b)&lt;--(a)--&gt;(c)--&gt;(d)
 *     Node ID :  0     1     0     2     3
 *      Rel ID :     1     1     2     3
 * </pre>
 * And negating relationships that are traversed backwards, we get:
 * <pre>
 *               (a)--&gt;(b)&lt;--(a)--&gt;(c)--&gt;(d)
 *     Node ID :  0     1     0     2     3
 *      Rel ID :    +1    -1    +2    +3
 * </pre>
 * Finally, we can drop the leading 0 from the Path structure and combine the
 * sequence information with the other path data. This results in an overall
 * Path representation as below:
 * <pre>
 *     Structure Path 'P' {
 *         nodes         = [(a), (b), (c), (d)]
 *         relationships = [(a:b), (a:c), (c:d)]
 *         sequence      = [+1, 1, -1, 0, +2, 2, +3, 3]
 *     }
 * </pre>
 * Therefore, for the original path containing five nodes and four relationships,
 * we end up transmitting only four nodes, three unbound relationships and eight
 * bytes of sequence data (small integers require only a single byte in PackStream).
 */
public class ValuePath implements Path
{
    private final int length;
    private final List<Node> nodes;
    private final List<Relationship> relationships;

    // Used for iteration, built lazily
    private List<PropertyContainer> entities = null;

    public ValuePath( List<Node> nodes, List<Relationship> relationships, List<Integer> sequence )
    {
        // Check we have at least one node, the minimum required for a valid path.
        assert nodes.size() >= 1;

        // The full sequence length should always be odd (alternating nodes and relationships
        // starting and ending with a node. But our definition allows us to infer that the
        // first node will be at index 0, so we prepend that value rather than transmitting it.
        List<Integer> fullSequence = new ArrayList<>( sequence );
        if ( fullSequence.size() % 2 == 0 )
        {
            fullSequence.add( 0, 0 );
        }

        // Now that we have the full sequence, we can determine the overall path length.
        int sequenceLength = fullSequence.size();
        this.length = sequenceLength / 2;

        // Load the nodes by pulling from the node list based on alternate sequence items.
        this.nodes = new ArrayList<>( this.length + 1 );
        for ( int i = 0; i < sequenceLength; i += 2 )
        {
            int index = fullSequence.get( i );
            this.nodes.add( nodes.get( index ) );
        }

        // Similarly, load the relationships based on all the other sequence items.
        this.relationships = new ArrayList<>( this.length );
        for ( int i = 0; i < this.length; i++ )
        {
            // The relationship index value will not only be one-based but will be negative if
            // the traversal happened against the direction of the underlying relationship.
            int oneBasedIndex = fullSequence.get( 2 * i + 1 );
            Relationship relationship;
            Node startNode;
            Node endNode;
            if ( oneBasedIndex < 0 )
            {
                // Backward traversal (flip the sign)
                relationship = relationships.get( (-oneBasedIndex) - 1 );
                startNode = this.nodes.get( i + 1 );
                endNode = this.nodes.get( i );
            }
            else
            {
                // Forward traversal (keep the sign)
                relationship = relationships.get( oneBasedIndex - 1 );
                startNode = this.nodes.get( i );
                endNode = this.nodes.get( i + 1 );
            }
            if ( relationship instanceof UnboundRelationship )
            {
                // Convert an unbound relationship to a bound one
                UnboundRelationship unboundRel = (UnboundRelationship) relationship;
                relationship = unboundRel.bind( startNode, endNode );
            }
            this.relationships.add( relationship );
        }
    }

    @Override
    public Node startNode()
    {
        return nodes.get( 0 );
    }

    @Override
    public Node endNode()
    {
        return nodes.get( length );
    }

    @Override
    public Relationship lastRelationship()
    {
        if ( length == 0 )
        {
            return null;
        }
        else
        {
            return relationships.get( length - 1 );
        }
    }

    @Override
    public Iterable<Relationship> relationships()
    {
        return relationships;
    }

    @Override
    public Iterable<Relationship> reverseRelationships()
    {
        // TODO: implement this
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Node> nodes()
    {
        return nodes;
    }

    @Override
    public Iterable<Node> reverseNodes()
    {
        // TODO: implement this
        throw new UnsupportedOperationException();
    }

    @Override
    public int length()
    {
        return length;
    }

    @Override
    public Iterator<PropertyContainer> iterator()
    {
        // Build the entity list lazily only as and when we need it...
        if ( entities == null )
        {
            entities = new ArrayList<>( 2 * length + 1 );
            for ( int i = 0; i < length; i++ )
            {
                entities.add( nodes.get( i ) );
                entities.add( relationships.get( i ) );
            }
            entities.add( nodes.get( length ) );
        }
        return entities.iterator();
    }

    @Override
    public String toString()
    {
        return "ValuePath{" +
                "nodes=" + nodes +
                ", relationships=" + relationships +
                '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o ) return true;
        if ( o == null || getClass() != o.getClass() ) return false;

        ValuePath that = (ValuePath) o;

        return nodes.equals( that.nodes ) && relationships.equals( that.relationships );

    }

    @Override
    public int hashCode()
    {
        int result = nodes.hashCode();
        result = 31 * result + relationships.hashCode();
        return result;
    }
}
