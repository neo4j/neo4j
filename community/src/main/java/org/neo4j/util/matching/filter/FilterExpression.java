package org.neo4j.util.matching.filter;

/**
 * Is either a regex leaf, i.e. a real regex pattern or an abstraction of
 * two {@link FilterExpression}s which are ANDed or ORed together.
 */
public interface FilterExpression
{
    boolean matches( FilterValueGetter valueGetter );
}
