/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.cache;

public class HPCMemoryConfig
{
    private final long nodeCacheSize;
    private final long relCacheSize;
    private final float nodeLookupTableFraction;
    private final float relLookupTableFraction;
    private final Source source;

    /**
     * Used to track how this configuration came about, which is in turn used to trigger some user warnings if she
     * is inadvertently overriding her own settings.
     */
    public enum Source
    {
        /** Config is complete vanilla, the user has not specified anything. */
        DEFAULT_MEMORY_RATIO,
        /** Config comes from user specifying only the memory ratio to allocate to caching. */
        EXPLICIT_MEMORY_RATIO,
        /** Specific settings for each cache part provided by user, and ratio was not specified by user. */
        SPECIFIC,
        /** Highlighting that ratio is configured as well, but was overridden by explicit config. */
        SPECIFIC_OVERRIDING_RATIO
    }

    public HPCMemoryConfig( long nodeCacheSize, long relCacheSize,
                            float nodeLookupTableFraction, float relLookupTableFraction, Source source )
    {
        this.nodeCacheSize = nodeCacheSize;
        this.relCacheSize = relCacheSize;
        this.nodeLookupTableFraction = nodeLookupTableFraction;
        this.relLookupTableFraction = relLookupTableFraction;
        this.source = source;
    }

    public Source source()
    {
        return source;
    }

    public long nodeCacheSize()
    {
        return nodeCacheSize;
    }

    public long relCacheSize()
    {
        return relCacheSize;
    }

    public float nodeLookupTableFraction()
    {
        return nodeLookupTableFraction;
    }

    public float relLookupTableFraction()
    {
        return relLookupTableFraction;
    }

    public long total()
    {
        return (long) (nodeCacheSize + relCacheSize +
                (((relLookupTableFraction + nodeLookupTableFraction) / 100)) * Runtime.getRuntime().maxMemory());
    }
}
