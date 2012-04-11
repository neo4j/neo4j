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

import java.util.Arrays;
import java.util.Map;

import org.neo4j.kernel.Config;
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
    private Map<Object, Object> config;
    private Cache<NodeImpl> node;
    private Cache<RelationshipImpl> relationship;
    private final StringLogger logger;

    public Caches( StringLogger logger )
    {
        this.logger = logger;
    }

    public void config( Map<Object, Object> config )
    {
        String newTypeName = (String) config.get( Config.CACHE_TYPE );
        CacheType newType = null;
        try
        {
            newType = newTypeName != null ? CacheType.valueOf( newTypeName ) : CacheType.soft;
        }
        catch ( IllegalArgumentException e )
        {
            throw new IllegalArgumentException( "Invalid cache type, please use one of: "
                                                + Arrays.asList( CacheType.values() ) + " or keep empty for default ("
                                                + CacheType.soft + ")", e.getCause() );
        }

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

    private boolean cacheConfigSame( CacheType type, Map<Object, Object> config )
    {
        return this.type == type
               &&

               // Only reuse array caches, since the other ones are cheap to
               // recreate
               this.type == CacheType.gcr
               &&

               Float.parseFloat( (String) this.config.get( Config.NODE_CACHE_ARRAY_FRACTION ) ) == Float.parseFloat( (String) config.get( Config.NODE_CACHE_ARRAY_FRACTION ) )
               && Float.parseFloat( (String) this.config.get( Config.RELATIONSHIP_CACHE_ARRAY_FRACTION ) ) == Float.parseFloat( (String) config.get( Config.RELATIONSHIP_CACHE_ARRAY_FRACTION ) )
               && stringSettingSame( config, Config.NODE_CACHE_SIZE )
               && stringSettingSame( config, Config.RELATIONSHIP_CACHE_SIZE );

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