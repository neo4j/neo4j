/**
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
        return provider.newNodeCache( logger, config, monitors );
    }

    @Override
    public Cache<RelationshipImpl> relationship()
    {
        return provider.newRelationshipCache( logger, config, monitors );
    }

    @Override
    public void invalidate()
    {
    }
}
