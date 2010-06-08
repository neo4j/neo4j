package org.neo4j.graphmatching.filter;

import org.neo4j.graphmatching.PatternNode;

/**
 * A hook for getting values from a node (Neo4j node) represented by
 * a {@link PatternNode} with a certain label.
 */
public interface FilterValueGetter
{
    /**
     * Returns an array of values.
     * @param label the {@link PatternNode} label.
     * @return an array of values.
     */
    Object[] getValues( String label );
}
