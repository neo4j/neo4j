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
package org.neo4j.io.pagecache.impl.muninn;

/**
 * This Runnable runs the eviction algorithm. Only one is expected for each page cache.
 *
 * Interrupting the thread running this runnable, will be interpreted as a shutdown signal.
 *
 * @see MuninnPageCache#continuouslySweepPages()
 */
final class EvictionTask extends BackgroundTask
{
    public EvictionTask( MuninnPageCache pageCache )
    {
        super( pageCache );
    }

    @Override
    protected void run( MuninnPageCache pageCache )
    {
        pageCache.continuouslySweepPages();
    }
}
