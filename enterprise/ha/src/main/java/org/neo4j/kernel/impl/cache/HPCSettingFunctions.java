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

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Function2;
import org.neo4j.kernel.impl.util.function.Optional;
import org.neo4j.kernel.impl.util.function.Optionals;

import static org.neo4j.helpers.Settings.BYTES;
import static org.neo4j.helpers.Settings.FLOAT;

public class HPCSettingFunctions
{
    @SuppressWarnings("RedundantStringConstructorCall")
    static String DEFAULT = new String("__DEFAULT__"); // Note explicit object creation to allow instance equality check

    /** Config parser that provides HPCMemoryConfig from a ratio-of-old-gen config setting. */
    static Function<String, HPCMemoryConfig> CACHE_MEMORY_RATIO = new Function<String, HPCMemoryConfig>()
    {
        @Override
        public HPCMemoryConfig apply( String s )
        {
            HPCMemoryConfig.Source source = HPCMemoryConfig.Source.EXPLICIT_MEMORY_RATIO;
            //noinspection StringEquality
            if ( s == DEFAULT ) // Note instance equality check is on purpose
            {
                s = "50.0" ;
                source = HPCMemoryConfig.Source.DEFAULT_MEMORY_RATIO;
            }

            long oldGenSize = memoryPoolMax( "java.lang:type=MemoryPool,name=G1 Old Gen" )
                    .or( memoryPoolMax( "java.lang:type=MemoryPool,name=PS Old Gen" ) )
                    .or( heap() / 2 ) // Conservative, to be on the safe side
                    .get();

            long allocated = (long)((FLOAT.apply( s ) / 100.0) * oldGenSize);

            return new HPCMemoryConfig(
                    /* Node cache size */ (long)(allocated * 0.3),
                    /* Rel cache size  */ (long)(allocated * 0.4),
                    /* Node lookup table */ asPercentageOfHeap( allocated * 0.1 ),
                    /* Rel lookup table  */ asPercentageOfHeap( allocated * 0.1 ), source );
        }

        private float asPercentageOfHeap( double bytes )
        {
            return (float) ((bytes / heap()) * 100);
        }

        @Override
        public String toString()
        {
            return "a value between 0 and 100";
        }
    };
    /** Overrides another hpc config setting if any explicit config is specified. */
    static Function2<HPCMemoryConfig, Function<String, String>, HPCMemoryConfig> OTHER_CACHE_SETTINGS_OVERRIDE = new Function2<HPCMemoryConfig, Function<String, String>, HPCMemoryConfig>()
    {
        @Override
        public HPCMemoryConfig apply( HPCMemoryConfig basedOnRatio, Function<String, String> settings )
        {
            String explicitNodeCacheSize     = settings.apply( HighPerformanceCacheSettings.node_cache_size.name() );
            String explicitRelCacheSize      = settings.apply( HighPerformanceCacheSettings.relationship_cache_size.name() );
            String explicitNodeArrayFraction = settings.apply(
                    HighPerformanceCacheSettings.node_cache_array_fraction.name() );
            String explicitRelArrayFraction  = settings.apply(
                    HighPerformanceCacheSettings.relationship_cache_array_fraction.name() );

            if( explicitNodeCacheSize != null
                    || explicitRelCacheSize != null
                    || explicitNodeArrayFraction != null
                    || explicitRelArrayFraction != null )
            {
                // At least one explicit config set, swap to explicit mode
                long nodeCacheBytes = explicitNodeCacheSize != null ? BYTES.apply( explicitNodeCacheSize ) : heap() / 8;
                long relCacheBytes  = explicitRelCacheSize  != null ? BYTES.apply( explicitRelCacheSize )  : heap() / 8;
                float nodeArrRatio = explicitNodeArrayFraction != null ? FLOAT.apply( explicitNodeArrayFraction) : 1.0f;
                float relArrRatio  = explicitRelArrayFraction  != null ? FLOAT.apply( explicitRelArrayFraction ) : 1.0f;

                // Figure out if user is inadvertently overwriting her own configuration
                HPCMemoryConfig.Source source = basedOnRatio.source() == HPCMemoryConfig.Source.DEFAULT_MEMORY_RATIO ? HPCMemoryConfig.Source.SPECIFIC :
                        HPCMemoryConfig.Source.SPECIFIC_OVERRIDING_RATIO;
                return new HPCMemoryConfig( nodeCacheBytes, relCacheBytes, nodeArrRatio, relArrRatio, source);
            }

            return basedOnRatio;
        }

        @Override
        public String toString()
        {
            return "cannot be used in conjunction with other object cache size settings";
        }
    };
    /** Disallows cache total size to go beyond the available heap. */
    static Function2<HPCMemoryConfig, Function<String, String>, HPCMemoryConfig> TOTAL_NOT_ALLOWED_ABOVE_HEAP = new Function2<HPCMemoryConfig, Function<String, String>, HPCMemoryConfig>()
    {
        @Override
        public HPCMemoryConfig apply( HPCMemoryConfig input, Function<String, String> settings )
        {
            if(input.total() > (heap() * 0.8))
            {
                throw new InvalidSettingException( HighPerformanceCacheSettings.cache_memory.name(),
                        String.format( "Configured object cache memory limits (node=%s, relationship=%s, " +
                                "total=%s) exceeds 80%% of available heap space (%s)",
                        input.nodeCacheSize(), input.relCacheSize(), input.total(), heap() ) );
            }
            return input;
        }

        @Override
        public String toString()
        {
            return "may not be set to more than 80% of the available heap space";
        }
    };

    private static Optional<Long> memoryPoolMax( final String bean )
    {
        try
        {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            CompositeDataSupport attribute = (CompositeDataSupport) server.getAttribute(
                    new ObjectName( bean ), "Usage" );
            return Optionals.some( (Long) attribute.get( "max" ) );
        }
        catch ( Exception e )
        {
            return Optionals.none();
        }
    }

    private static long heap()
    {
        return Runtime.getRuntime().maxMemory();
    }
}
