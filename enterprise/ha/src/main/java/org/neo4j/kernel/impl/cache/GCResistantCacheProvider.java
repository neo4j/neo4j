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

import java.util.Map;

import org.neo4j.helpers.Service;
import org.neo4j.helpers.TimeUtil;
import org.neo4j.kernel.HaConfig;
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
    public Cache<NodeImpl> newNodeCache( StringLogger logger, Map<Object, Object> params )
    {
        long mem = memToUse( logger, params, true );
        return new GCResistantCache<NodeImpl>( mem, fraction( params, HaConfig.NODE_CACHE_ARRAY_FRACTION ),
                minLogInterval( params ), NODE_CACHE_NAME, logger );
    }

    @Override
    public Cache<RelationshipImpl> newRelationshipCache( StringLogger logger, Map<Object, Object> params )
    {
        long mem = memToUse( logger, params, false );
        return new GCResistantCache<RelationshipImpl>( mem, fraction( params,
                HaConfig.RELATIONSHIP_CACHE_ARRAY_FRACTION ), minLogInterval( params ),
                RELATIONSHIP_CACHE_NAME, logger );
    }

    private long minLogInterval( Map<Object, Object> params )
    {
        String interval = (String) params.get( HaConfig.GCR_CACHE_MIN_LOG_INTERVAL );
        long result = 60000; // Default: a minute
        try
        {
            if ( interval != null ) result = TimeUtil.parseTimeMillis( interval );
        }
        catch ( Exception e )
        {
            throw new IllegalArgumentException( "Invalid configuration value [" + interval + "] for "
                                                + HaConfig.GCR_CACHE_MIN_LOG_INTERVAL, e );
        }
        if ( result < 0 )
        {
            throw new IllegalArgumentException( "Invalid configuration value [" + interval + "] for "
                                                + HaConfig.GCR_CACHE_MIN_LOG_INTERVAL );
        }
        return result;
    }

    @SuppressWarnings( "boxing" )
    private long memToUse( StringLogger logger, Map<Object, Object> params, boolean isNode )
    {
        Long node = memory( params, HaConfig.NODE_CACHE_SIZE );
        Long rel = memory( params, HaConfig.RELATIONSHIP_CACHE_SIZE );
        final long available = Runtime.getRuntime().maxMemory(), advicedMax = available / 2, advicedMaxPerCache = advicedMax / 2;
        if ( ( node == null && isNode ) || ( rel == null && !isNode ) )
        {
            return advicedMaxPerCache;
        }
        long total = 0;
        node = node != null ? node : advicedMaxPerCache;
        rel = rel != null ? rel : advicedMaxPerCache;
        node = Math.max( GCResistantCache.MIN_SIZE, node );
        total += node;
        rel = Math.max( GCResistantCache.MIN_SIZE, rel );
        total += rel;
        if ( total > available )
            throw new IllegalArgumentException(
                    String.format(
                            "Configured cache memory limits (node=%s, relationship=%s, total=%s) exceeds available heap space (%s)",
                            node, rel, total, available ) );
        if ( total > advicedMax )
            logger.logMessage( String.format(
                    "Configured cache memory limits(node=%s, relationship=%s, total=%s) exceeds recommended limit (%s)",
                    node, rel, total, advicedMax ) );
        if ( node == null )
        {
            node = Math.max( GCResistantCache.MIN_SIZE, advicedMax - rel );
        }
        if ( rel == null )
        {
            rel = Math.max( GCResistantCache.MIN_SIZE, advicedMax - node );
        }
        return isNode ? node : rel;
    }

    private float fraction( Map<Object, Object> params, String param )
    {
        String fraction = (String) params.get( HaConfig.NODE_CACHE_ARRAY_FRACTION );
        float result = 1;
        try
        {
            if ( fraction != null ) result = Float.parseFloat( fraction );
        }
        catch ( NumberFormatException e )
        {
            throw new IllegalArgumentException( "Invalid configuration value [" + fraction + "] for " + param, e );
        }
        if ( result < 1 || result > 10 )
        {
            throw new IllegalArgumentException( "Invalid configuration value [" + fraction + "] for " + param );
        }
        return result;
    }

    @SuppressWarnings( "boxing" )
    private Long memory( Map<Object, Object> params, String param )
    {
        Object config = params.get( param );
        if ( config != null )
        {
            String mem = config.toString();
            mem = mem.trim().toLowerCase();
            long multiplier = 1;
            if ( mem.endsWith( "m" ) )
            {
                multiplier = 1024 * 1024;
                mem = mem.substring( 0, mem.length() - 1 );
            }
            else if ( mem.endsWith( "k" ) )
            {
                multiplier = 1024;
                mem = mem.substring( 0, mem.length() - 1 );
            }
            else if ( mem.endsWith( "g" ) )
            {
                multiplier = 1024 * 1024 * 1024;
                mem = mem.substring( 0, mem.length() - 1 );
            }
            try
            {
                return Long.parseLong( mem ) * multiplier;
            }
            catch ( NumberFormatException e )
            {
                throw new IllegalArgumentException( "Invalid configuration value [" + mem + "] for " + param, e );
            }
        }
        else
        {
            return null;
        }
    }
}