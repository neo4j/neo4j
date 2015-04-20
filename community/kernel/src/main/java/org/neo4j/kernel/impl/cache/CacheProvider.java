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
package org.neo4j.kernel.impl.cache;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.HasSettings;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;

public abstract class CacheProvider extends Service implements HasSettings
{
    protected static final String NODE_CACHE_NAME = "NodeCache";
    protected static final String RELATIONSHIP_CACHE_NAME = "RelationshipCache";

    private final String name;
    private final String description;

    protected CacheProvider( String key, String description )
    {
        super( key );
        this.name = key;
        this.description = description;
    }
    
    public abstract Cache<NodeImpl> newNodeCache( StringLogger logger, Config config, Monitors monitors );

    public abstract Cache<RelationshipImpl> newRelationshipCache( StringLogger logger, Config config,
            Monitors monitors );
    
    public String getName()
    {
        return name;
    }
    
    public String getDescription()
    {
        return description;
    }

    @Override
    public Class getSettingsClass()
    {
        return null;
    }
}
