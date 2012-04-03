/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.gcr_cache_min_log_interval;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.node_cache_array_fraction;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.node_cache_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.relationship_cache_array_fraction;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.relationship_cache_size;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.util.StringLogger;

@Service.Implementation( CacheProvider.class )
public class GCResistantCacheProvider extends CacheProvider
{
    public static final String NAME = "gcr";
    
    public GCResistantCacheProvider()
    {
        super( NAME, "GC resistant cache" );
    }

    @Override
    public Cache<NodeImpl> newNodeCache( StringLogger logger, Config config )
    {
        long available = Runtime.getRuntime().maxMemory();
        long defaultMem = ( available / 4);
        long node = config.isSet( node_cache_size ) ? config.getSize( node_cache_size ) : defaultMem;
        long rel = config.isSet( relationship_cache_size ) ? config.getSize( relationship_cache_size ) : defaultMem;
        checkMemToUse( logger, node, rel, available );
        return new GCResistantCache<NodeImpl>( node, config.getFloat( node_cache_array_fraction ), config.getDuration( gcr_cache_min_log_interval ),
                NODE_CACHE_NAME, logger );
    }

    @Override
    public Cache<RelationshipImpl> newRelationshipCache( StringLogger logger, Config config )
    {
        long available = Runtime.getRuntime().maxMemory();
        long defaultMem = ( available / 4);
        long node = config.isSet( node_cache_size ) ? config.getSize( node_cache_size ) : defaultMem;
        long rel = config.isSet( relationship_cache_size ) ? config.getSize( relationship_cache_size ) : defaultMem;
        checkMemToUse( logger, node, rel, available );
        return new GCResistantCache<RelationshipImpl>( rel, config.getFloat( relationship_cache_array_fraction ), config.getDuration( gcr_cache_min_log_interval ),
                RELATIONSHIP_CACHE_NAME, logger );
    }

    @SuppressWarnings( "boxing" )
    private void checkMemToUse( StringLogger logger, long node, long rel, long available )
    {
        long advicedMax = available / 2;
        long total = 0;
        node = Math.max( GCResistantCache.MIN_SIZE, node );
        total += node;
        rel = Math.max( GCResistantCache.MIN_SIZE, rel );
        total += rel;
        if ( total > available )
            throw new IllegalArgumentException(
                    String.format( "Configured cache memory limits (node=%s, relationship=%s, total=%s) exceeds available heap space (%s)",
                            node, rel, total, available ) );
        if ( total > advicedMax )
            logger.logMessage( String.format( "Configured cache memory limits(node=%s, relationship=%s, total=%s) exceeds recommended limit (%s)",
                    node, rel, total, advicedMax ) );
    }
}
