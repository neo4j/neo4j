/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database data metrics" )
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
