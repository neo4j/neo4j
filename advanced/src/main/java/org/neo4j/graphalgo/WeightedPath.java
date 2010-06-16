package org.neo4j.graphalgo;

import org.neo4j.graphdb.Path;

/**
 * A {@link Path} that has an associated weight.
 *
 * @author Tobias Ivarsson
 */
public interface WeightedPath extends Path
{
    /**
     * Returns the weight of the path.
     * 
     * @return the weight of the path.
     */
    double weight();
}
