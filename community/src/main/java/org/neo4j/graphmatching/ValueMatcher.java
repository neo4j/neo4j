package org.neo4j.graphmatching;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public interface ValueMatcher
{
    /**
     * Tries to match {@code value} to see if it matches an expected value.
     * {@code value} is {@code null} if the property wasn't found on the
     * {@link Node} or {@link Relationship} it came from.
     * 
     * The value can be of type array, where {@link ArrayPropertyUtil} can
     * be of help.
     * 
     * @param value the value from a {@link Node} or {@link Relationship} to
     * match against an expected value.
     * @return {@code true} if the value matches, else {@code false}.
     */
    boolean matches( Object value );
}
