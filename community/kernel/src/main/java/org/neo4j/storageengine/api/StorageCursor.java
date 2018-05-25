package org.neo4j.storageengine.api;

/**
 * Base interface for a cursor accessing and reading data as part of {@link StorageReader}.
 */
public interface StorageCursor extends AutoCloseable
{
    /**
     * Positions this cursor and reads the next item that it has been designated to read.
     * @return {@code true} if the item was read and in use, otherwise {@code false}.
     */
    boolean next();

    /**
     * Closes the cursor so that calls to {@link #next()} will not return {@code true} anymore.
     * After this point this cursor can still be used, by initializing it with any of the cursor-specific
     * initialization method.
     */
    @Override
    void close();

    /**
     * Releases resources allocated by this cursor so that it cannot be initialized or used again after this call.
     */
    void release();

    /**
     * Resets the current data for this cursor so that it returns an unused uninitialized item.
     */
    void reset();
}
