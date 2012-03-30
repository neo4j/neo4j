/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.node_cache_array_fraction;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.node_cache_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.relationship_cache_array_fraction;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.relationship_cache_size;

import org.neo4j.graphdb.factory.GraphDatabaseSetting.StringSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.core.NodeManager.CacheType;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * A class for holding cache objects so that reuse between sessions is possible
 * if the configuration stays the same. This helps when there's a cache which is
 * very expensive to create, like {@link CacheType#gcr}.
 * 
 * @author Mattias Persson
 */
public class Caches
{
    private CacheType type;
    private Config config;
    private Cache<NodeImpl> node;
    private Cache<RelationshipImpl> relationship;
    private final StringLogger logger;
    
    public Caches( StringLogger logger )
    {
        this.logger = logger;
    }
    
    public void config( Config config )
    {
        CacheType newType = config.getEnum( CacheType.class, GraphDatabaseSettings.cache_type );
        if ( !cacheConfigSame( newType, config ) )
        {
            node = newType.node( logger, config );
            relationship = newType.relationship( logger, config );
        }
        else
        {
            node.clear();
            relationship.clear();
        }
        this.type = newType;
        this.config = config;
    }

    private boolean cacheConfigSame( CacheType type, Config config )
    {
        return
                this.type == type &&
                
                // Only reuse array caches, since the other ones are cheap to recreate
                this.type == CacheType.gcr &&
                
                this.config.getFloat( node_cache_array_fraction ) == config.getFloat( node_cache_array_fraction ) &&
                this.config.getFloat( relationship_cache_array_fraction ) == config.getFloat( relationship_cache_array_fraction ) &&
                stringSettingSame( config, node_cache_size ) &&
                stringSettingSame( config, relationship_cache_size );
                
    }
    
    private boolean stringSettingSame( Config otherConfig, StringSetting setting )
    {
        String myValue = this.config.get( setting );
        String otherValue = config.get( setting );
        return myValue == otherValue || (myValue != null && myValue.equals( otherValue ) );
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
