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
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.kernel.configuration.Config;

import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.cache.HPCSettingFunctions.CACHE_MEMORY_RATIO;
import static org.neo4j.kernel.impl.cache.HighPerformanceCacheSettings.cache_memory;
import static org.neo4j.kernel.impl.cache.HighPerformanceCacheSettings.node_cache_size;

public class HighPerformanceCacheSettingsTest
{
    @Test
    public void cacheMemoryRatioShouldCalculateSaneRatio() throws Exception
    {
        // Given
        HPCMemoryConfig config = CACHE_MEMORY_RATIO.apply( "100.0" );

        // Then
        assertThat( config.nodeCacheSize(), greaterThan( 0l ) );
        assertThat( config.relCacheSize(), greaterThan( 0l ) );
        assertThat( config.nodeLookupTableFraction(), greaterThan( 0f ) );
        assertThat( config.relLookupTableFraction(), greaterThan( 0f ) );

        // And when I give a different ratio
        HPCMemoryConfig otherRatio = CACHE_MEMORY_RATIO.apply( "50.0" );

        // Then
        assertThat( (double)otherRatio.nodeCacheSize(),           closeTo( config.nodeCacheSize() / 2, 10.0 ));
        assertThat( (double)otherRatio.relCacheSize(),            closeTo( config.relCacheSize() / 2, 10.0 ));
        assertThat( (double)otherRatio.nodeLookupTableFraction(), closeTo(config.nodeLookupTableFraction()/2, 10.0));
        assertThat( (double)otherRatio.relLookupTableFraction(),  closeTo(config.relLookupTableFraction()/2, 10.0));
    }

    @Test
    public void explicitCacheConfigShouldOverrideRatioDefaults() throws Exception
    {
        // Given
        Config conf = new Config(stringMap( node_cache_size.name(), "200M" ), HighPerformanceCacheSettings.class );

        // When
        HPCMemoryConfig memoryConfig = conf.get( cache_memory );

        // Then
        assertThat(memoryConfig.source(), equalTo( HPCMemoryConfig.Source.SPECIFIC ));
        assertThat(memoryConfig.nodeCacheSize(), equalTo(209715200L));

        assertThat((double)memoryConfig.relCacheSize(), closeTo( Runtime.getRuntime().maxMemory() / 8, 100.0 ));
        assertThat(memoryConfig.relLookupTableFraction(), equalTo( 1.0f ));
        assertThat(memoryConfig.nodeLookupTableFraction(), equalTo(1.0f));
    }

    @Test
    public void explicitCacheConfigShouldOverrideExplicitRatio() throws Exception
    {
        // Given
        Config conf = new Config(stringMap(
                node_cache_size.name(), "200M",
                cache_memory.name(), "25" ), HighPerformanceCacheSettings.class );

        // When
        HPCMemoryConfig memoryConfig = conf.get( cache_memory );

        // Then
        assertThat(memoryConfig.source(), equalTo( HPCMemoryConfig.Source.SPECIFIC_OVERRIDING_RATIO ));
        assertThat(memoryConfig.nodeCacheSize(), equalTo(209715200L));

        assertThat((double)memoryConfig.relCacheSize(), closeTo(Runtime.getRuntime().maxMemory() / 8, 100.0));
        assertThat(memoryConfig.relLookupTableFraction(), equalTo(1.0f));
        assertThat(memoryConfig.nodeLookupTableFraction(), equalTo(1.0f));
    }

    @Test
    public void totalConfigLargerThanHeapIsNotAllowed() throws Exception
    {
        // When
        try
        {
            new Config(stringMap(node_cache_size.name(), "" + (Runtime.getRuntime().maxMemory() * 100)),
                    HighPerformanceCacheSettings.class );
            fail("Should not have allowed getting this config.");
        // Then
        } catch(InvalidSettingException e)
        {
            assertThat(e.getMessage(), containsString( "Configured object cache memory limits" ));
            assertThat(e.getMessage(), containsString( "exceeds 80% of available heap space" ));
        }
    }
}
