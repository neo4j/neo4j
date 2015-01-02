/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.io.pagecache.PageSwapper;

/**
 * The eviction of a page has begun.
 */
public interface EvictionEvent extends AutoCloseablePageCacheMonitorEvent
{
    /**
     * The file page id the evicted page was bound to.
     */
    public void setFilePageId( long filePageId );

    /**
     * The swapper the evicted page was bound to.
     */
    public void setSwapper( PageSwapper swapper );

    /**
     * Eviction implies an opportunity to flush.
     */
    public FlushEventOpportunity flushEventOpportunity();

    /**
     * Indicates that the eviction caused an exception to be thrown.
     * This can happen if some kind of IO error occurs.
     */
    public void threwException( IOException exception );

    /**
     * The cache page id of the evicted page.
     */
    public void setCachePageId( int cachePageId );
}
