/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

public class DefaultCaches implements Caches
{
    private CacheProvider provider;
    private Config config;
    private final StringLogger logger;
    private final Monitors monitors;
    private Cache<NodeImpl> nodeCache;
    private Cache<RelationshipImpl> relCache;

    public DefaultCaches( StringLogger logger, Monitors monitors )
    {
        this.logger = logger;
        this.monitors = monitors;
    }

    @Override
    public void configure( CacheProvider provider, Config config )
    {
        this.provider = provider;
        this.config = config;
    }

    @Override
    public Cache<NodeImpl> node()
    {
        if ( nodeCache == null )
        {
            nodeCache = provider.newNodeCache( logger, config, monitors );
        }
        return nodeCache;
    }

    @Override
    public Cache<RelationshipImpl> relationship()
    {
        if ( relCache == null )
        {
            relCache = provider.newRelationshipCache( logger, config, monitors );
        }
        return relCache;
    }

    @Override
    public void clear()
    {
        if ( nodeCache != null )
        {
            nodeCache.clear();
        }
        if ( relCache != null )
        {
            relCache.clear();
        }
    }

    @Override
    public void invalidate()
    {
    }

    @Override
    public CacheProvider getProvider()
    {
        return provider;
    }
}
