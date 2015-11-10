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
package org.neo4j.metrics.source;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.metrics.MetricsSettings;

import static com.codahale.metrics.MetricRegistry.name;

public class EntityCountMetrics extends LifecycleAdapter
{
    private static final String COUNTS_PREFIX = "neo4j.ids_in_use";
    private static final String COUNTS_RELATIONSHIP_TYPE = name( COUNTS_PREFIX, "relationship_type" );
    private static final String COUNTS_PROPERTY = name( COUNTS_PREFIX, "property" );
    private static final String COUNTS_RELATIONSHIP = name( COUNTS_PREFIX, "relationship" );
    private static final String COUNTS_NODE = name( COUNTS_PREFIX, "node" );

    private final MetricRegistry registry;
    private final Config config;
    private final IdGeneratorFactory idGeneratorFactory;

    public EntityCountMetrics( MetricRegistry registry, Config config, IdGeneratorFactory idGeneratorFactory )
    {
        this.registry = registry;
        this.config = config;
        this.idGeneratorFactory = idGeneratorFactory;
    }

    @Override
    public void start() throws Throwable
    {
        if ( config.get( MetricsSettings.neoCountsEnabled ) )
        {
            registry.register( COUNTS_NODE, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return idGeneratorFactory.get( IdType.NODE ).getNumberOfIdsInUse();
                }
            } );

            registry.register( COUNTS_RELATIONSHIP, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return idGeneratorFactory.get( IdType.RELATIONSHIP ).getNumberOfIdsInUse();
                }
            } );

            registry.register( COUNTS_PROPERTY, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return idGeneratorFactory.get( IdType.PROPERTY ).getNumberOfIdsInUse();
                }
            } );

            registry.register( COUNTS_RELATIONSHIP_TYPE, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return idGeneratorFactory.get( IdType.RELATIONSHIP_TYPE_TOKEN ).getNumberOfIdsInUse();
                }
            } );
        }
    }

    @Override
    public void stop() throws IOException
    {
        if ( config.get( MetricsSettings.neoCountsEnabled ) )
        {
            registry.remove( COUNTS_NODE );
            registry.remove( COUNTS_RELATIONSHIP );
            registry.remove( COUNTS_PROPERTY );
            registry.remove( COUNTS_RELATIONSHIP_TYPE );
        }
    }
}
