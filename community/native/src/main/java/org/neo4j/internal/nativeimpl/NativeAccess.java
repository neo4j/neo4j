/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.nativeimpl;

public interface NativeAccess
{
    /**
     * Operation completed successfully
     */
    int SUCCESS = 0;

    /**
     * Generic operation error
     */
    int ERROR = -1;

    /**
     * Check if native access is available
     * @return true if available, false otherwise
     */
    boolean isAvailable();

    /**
     * Try to evict cached pages of file referenced by provided file descriptor.
     * Useful for files that we do not need to access ever again. For example: log files, WAL files, etc.
     * @param fd file descriptor
     * @return returns zero on success, or an error number on failure
     */
    NativeCallResult tryEvictFromCache( int fd );

    /**
     * Try to advice that file referenced by provided file descriptor will be accessed in a sequential fashion.
     * Useful for files that we will read from start to the end sequentially. For example: WAL files.
     * @param fd file descriptor
     * @return returns zero on success, or an error number on failure
     */
    NativeCallResult tryAdviseSequentialAccess( int fd );

    /**
     * Try to advice that file referenced by provided file descriptor will be accessed again in the near future and we will need those pages again.
     * Useful for files that we will read from start to the end sequentially. For example: WAL files.
     * @param fd file descriptor
     * @return returns zero on success, or an error number on failure
     */
    NativeCallResult tryAdviseToKeepInCache( int fd );

    /**
     * Try to preallocate disk space for file referenced by provided file descriptor.
     * @param fd file descriptor
     * @param bytes number of bytes to preallocate
     * @return returns zero on success, or an error number on failure
     */
    NativeCallResult tryPreallocateSpace( int fd, long bytes );

    /**
     * Details about native access provider
     * @return details about native access
     */
    String describe();
}
