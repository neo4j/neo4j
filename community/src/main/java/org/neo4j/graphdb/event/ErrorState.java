package org.neo4j.graphdb.event;

/**
 * An object that describes a state from which a Neo4j Graph Database cannot
 * continue.
 *
 * @author Tobias Ivarsson
 */
public enum ErrorState
{
    /**
     * The Graph Database failed since the storage media where the graph
     * database data is stored is full and cannot be written to.
     */
    STORAGE_MEDIA_FULL,
}
