/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.io.pagecache.monitoring;

/**
 * The PageCacheMonitor exposes internal counters from the page cache.
 * The data for these counters is sourced through the PageCacheTracer API.
 */
public interface PageCacheMonitor
{
    /**
     * @return The number of page faults observed thus far.
     */
    public long countFaults();

    /**
     * @return The number of page evictions observed thus far.
     */
    public long countEvictions();

    /**
     * @return The number of page pins observed thus far.
     */
    public long countPins();

    /**
     * @return The number of page unpins observed thus far.
     */
    public long countUnpins();

    /**
     * @return The number of page flushes observed thus far.
     */
    public long countFlushes();

    /**
     * @return The sum total of bytes read in through page faults thus far.
     */
    public long countBytesRead();

    /**
     * @return The sum total of bytes written through flushes thus far.
     */
    public long countBytesWritten();

    /**
     * @return The number of file mappings observed thus far.
     */
    public long countFilesMapped();

    /**
     * @return The number of file unmappings observed thus far.
     */
    public long countFilesUnmapped();

    /**
     * @return The number of page evictions that have thrown exceptions thus far.
     */
    public long countEvictionExceptions();
}
