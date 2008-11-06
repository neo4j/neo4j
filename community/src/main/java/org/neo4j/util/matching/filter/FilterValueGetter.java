package org.neo4j.util.matching.filter;

import org.neo4j.util.matching.PatternNode;

/**
 * A hook for getting values from a node (real neo node) represented by
 * a {@link PatternNode} with a certain label.
 */
public interface FilterValueGetter
{
    Object[] getValues( String label );
}
