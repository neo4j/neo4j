package org.neo4j.graphdb.traversal;

import java.util.Iterator;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

/**
 * This interface represents the traverser which is used to step through the
 * results of a traversal. Each step can be represented in different ways. The
 * default is as {@link Path} objects which all other representations can be
 * derived from, i.e {@link Node} or {@link Relationship}. Each step
 * can also be represented in one of those representations directly.
 */
public interface Traverser extends Iterable<Path>
{
    /**
     * Represents the traversal in the form of {@link Node}s. This is a
     * convenient way to iterate over {@link Path}s and get the
     * {@link Path#endNode()} for each position.
     * 
     * @return the traversal in the form of {@link Node} objects.
     */
    Iterable<Node> nodes();

    /**
     * Represents the traversal in the form of {@link Relationship}s. This is a
     * convenient way to iterate over {@link Path}s and get the
     * {@link Path#lastRelationship()} for each position.
     * 
     * @return the traversal in the form of {@link Relationship} objects.
     */
    Iterable<Relationship> relationships();

    /**
     * Represents the traversal in the form of {@link Path}s.
     *
     * @return the traversal in the form of {@link Path} objects.
     */
    Iterator<Path> iterator();
}
