package org.neo4j.storageengine.api;

/**
 * A {@link StorageCursor} for entities, i.e. which has properties.
 */
public interface StorageEntityCursor extends StorageCursor
{
    /**
     * @return {@code true} if the entity the cursor is at has any properties, otherwise {@code false}.
     */
    boolean hasProperties();

    /**
     * @return a {@code long} reference to start reading properties for the entity this cursor is at.
     */
    long propertiesReference();
}
