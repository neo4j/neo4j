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

import org.junit.Test;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.cache.HighPerformanceCacheSettings.cache_memory;
import static org.neo4j.kernel.impl.cache.HighPerformanceCacheSettings.node_cache_size;

public class HighPerformanceCacheProviderTest
{
    @Test
    public void shouldWarnIfUserOverridesHerOwnConfig() throws Exception
    {
        // Given
        TestLogger logger = new TestLogger();
        Config configWithOverridenMemoryRatio = new Config( stringMap(
                cache_memory.name(), "25",
                node_cache_size.name(), "10M" ), HighPerformanceCacheSettings.class );

        // When
        new HighPerformanceCacheProvider().newNodeCache( logger, configWithOverridenMemoryRatio, new Monitors() );

        // Then
        logger.assertExactly( TestLogger.LogCall.warn("Explicit cache memory ratio configuration is ignored, " +
                "because advanced cache memory configuration has been specified. Please specify either only the " +
                "ratio configuration, or only the advanced configuration.") );
    }
}
