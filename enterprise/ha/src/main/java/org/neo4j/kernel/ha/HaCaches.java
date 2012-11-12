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
package org.neo4j.kernel.ha;

import java.util.Map;

import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.GCResistantCacheProvider;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.util.StringLogger;

public class HaCaches implements Caches
{
    private CacheProvider type;
    private Map<Object, Object> config;
    private Cache<NodeImpl> node;
    private Cache<RelationshipImpl> relationship;
    private final StringLogger logger;

    public HaCaches( StringLogger logger )
    {
        this.logger = logger;
    }

    public void configure( CacheProvider newType, Map<Object, Object> config )
    {
        if ( !cacheConfigSame( newType, config ) )
        {
            node = newType.newNodeCache( logger, config );
            relationship = newType.newRelationshipCache( logger, config );
        }
        else
        {
            node.clear();
            relationship.clear();
        }
        this.type = newType;
        this.config = config;
    }

    private boolean cacheConfigSame( CacheProvider type, Map<Object, Object> config )
    {
        return this.type != null
               && this.type.getName().equals( type.getName() )
               &&

               // Only reuse array caches, since the other ones are cheap to
               // recreate
               GCResistantCacheProvider.NAME.equals( this.type.getName() )
               &&

               Float.parseFloat( (String) this.config.get( HaConfig.NODE_CACHE_ARRAY_FRACTION ) ) == Float.parseFloat( (String) config.get( HaConfig.NODE_CACHE_ARRAY_FRACTION ) )
               && Float.parseFloat( (String) this.config.get( HaConfig.RELATIONSHIP_CACHE_ARRAY_FRACTION ) ) == Float.parseFloat( (String) config.get( HaConfig.RELATIONSHIP_CACHE_ARRAY_FRACTION ) )
               && stringSettingSame( config, HaConfig.NODE_CACHE_SIZE )
               && stringSettingSame( config, HaConfig.RELATIONSHIP_CACHE_SIZE );

    }

    private boolean stringSettingSame( Map<Object, Object> otherConfig, String setting )
    {
        String myValue = (String) this.config.get( setting );
        String otherValue = (String) config.get( setting );
        return myValue == otherValue || ( myValue != null && myValue.equals( otherValue ) );
    }

    public Cache<NodeImpl> node()
    {
        return node;
    }

    public Cache<RelationshipImpl> relationship()
    {
        return relationship;
    }

    public void invalidate()
    {
        type = null;
        config = null;
    }
}