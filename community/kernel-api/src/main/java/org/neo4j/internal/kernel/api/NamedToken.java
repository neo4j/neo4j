package org.neo4j.internal.kernel.api;

/**
 * A token with its associated name.
 */
public interface NamedToken
{
    /**
     * Id of token
     * @return the id of the token
     */
    int id();

    /**
     * The name associated with the token
     * @return The name corresponding to the token
     */
    String name();
}
