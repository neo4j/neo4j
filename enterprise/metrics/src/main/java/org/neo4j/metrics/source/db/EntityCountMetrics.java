/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database Data Metrics" )
public class EntityCountMetrics extends LifecycleAdapter
{
    private static final String COUNTS_PREFIX = "neo4j.ids_in_use";

    @Documented( "The total number of different relationship types stored in the database" )
    public static final String COUNTS_RELATIONSHIP_TYPE = name( COUNTS_PREFIX, "relationship_type" );
    @Documented( "The total number of different property names used in the database" )
    public static final String COUNTS_PROPERTY = name( COUNTS_PREFIX, "property" );
    @Documented( "The total number of relationships stored in the database" )
    public static final String COUNTS_RELATIONSHIP = name( COUNTS_PREFIX, "relationship" );
    @Documented( "The total number of nodes stored in the database" )
    public static final String COUNTS_NODE = name( COUNTS_PREFIX, "node" );

    private final MetricRegistry registry;
    private final StoreEntityCounters storeEntityCounters;

    public EntityCountMetrics( MetricRegistry registry, StoreEntityCounters storeEntityCounters )
    {
        this.registry = registry;
        this.storeEntityCounters = storeEntityCounters;
    }

    @Override
    public void start()
    {
        registry.register( COUNTS_NODE, (Gauge<Long>) storeEntityCounters::nodes );
        registry.register( COUNTS_RELATIONSHIP, (Gauge<Long>) storeEntityCounters::relationships );
        registry.register( COUNTS_PROPERTY, (Gauge<Long>) storeEntityCounters::properties );
        registry.register( COUNTS_RELATIONSHIP_TYPE, (Gauge<Long>) storeEntityCounters::relationshipTypes );
    }

    @Override
    public void stop()
    {
        registry.remove( COUNTS_NODE );
        registry.remove( COUNTS_RELATIONSHIP );
        registry.remove( COUNTS_PROPERTY );
        registry.remove( COUNTS_RELATIONSHIP_TYPE );
    }
}
