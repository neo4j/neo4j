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
package org.neo4j.kernel.ha;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.HighPerformanceCacheProvider;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.impl.cache.HighPerformanceCacheSettings.node_cache_array_fraction;
import static org.neo4j.kernel.impl.cache.HighPerformanceCacheSettings.node_cache_size;
import static org.neo4j.kernel.impl.cache.HighPerformanceCacheSettings.relationship_cache_array_fraction;
import static org.neo4j.kernel.impl.cache.HighPerformanceCacheSettings.relationship_cache_size;

public class HaCaches implements Caches
{
    private CacheProvider type;
    private Config config;
    private Cache<NodeImpl> node;
    private Cache<RelationshipImpl> relationship;
    private final StringLogger logger;
    private final Monitors monitors;

    public HaCaches( StringLogger logger, Monitors monitors )
    {
        this.logger = logger;
        this.monitors = monitors;
    }

    @Override
	public void configure( CacheProvider newType, Config config )
    {
        if ( !cacheConfigSame( newType, config ) )
        {
            node = newType.newNodeCache( logger, config, monitors );
            relationship = newType.newRelationshipCache( logger, config, monitors );
        }
        else
        {
            node.clear();
            relationship.clear();
        }
        this.type = newType;
        this.config = config;
    }

    private boolean cacheConfigSame( CacheProvider type, Config config )
    {
        return
                this.type != null && this.type.getName().equals( type.getName() ) &&
                
                // Only reuse array caches, since the other ones are cheap to recreate
                HighPerformanceCacheProvider.NAME.equals( this.type.getName() ) &&
                
                mySettingIsSameAs(config, node_cache_array_fraction ) &&
                mySettingIsSameAs(config, relationship_cache_array_fraction ) &&
                mySettingIsSameAs(config, node_cache_size ) &&
        		mySettingIsSameAs(config, relationship_cache_size );
    }

    private boolean mySettingIsSameAs(Config otherConfig, Setting<?> setting) {
		Object myValue = config.get(setting);
		Object otherValue = otherConfig.get(setting);
		
		return myValue.equals(otherValue);
	}

	@Override
	public Cache<NodeImpl> node()
    {
        return node;
    }
    
    @Override
	public Cache<RelationshipImpl> relationship()
    {
        return relationship;
    }
    
    @Override
	public void invalidate()
    {
        type = null;
        config = null;
    }

    @Override
    public void clear()
    {
        node.clear();
        relationship.clear();
    }

    @Override
    public CacheProvider getProvider()
    {
        return type;
    }
}
