package org.neo4j.graphmatching.filter;

/**
 * Is either a regex leaf, i.e. a real regex pattern or an abstraction of
 * two {@link FilterExpression}s which are ANDed or ORed together.
 */
public interface FilterExpression
{
    /**
     * Matches a value from a {@code valueGetter} and returns whether or not
     * there was a match.
     * @param valueGetter the getter which fetches the value to match.
     * @return whether or not the value from {@code valueGetter} matches
     * the criterias found in this expression.
     */
    boolean matches( FilterValueGetter valueGetter );
}
