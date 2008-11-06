package org.neo4j.util.matching.regex;

import org.neo4j.util.matching.PatternNode;

/**
 * A hook for getting values from a node (real neo node) represented by
 * a {@link PatternNode} with a certain label.
 */
public interface RegexValueGetter
{
    String[] getValues( String label );
}
