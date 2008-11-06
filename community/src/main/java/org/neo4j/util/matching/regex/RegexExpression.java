package org.neo4j.util.matching.regex;

/**
 * Is either a regex leaf, i.e. a real regex pattern or an abstraction of
 * two {@link RegexExpression}s which are ANDed or ORed together.
 */
public interface RegexExpression
{
    boolean matches( RegexValueGetter valueGetter );
}
