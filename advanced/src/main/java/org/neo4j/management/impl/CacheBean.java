/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.management.impl;

import java.security.AccessControlException;

import javax.management.MBeanOperationInfo;
import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.management.Cache;

@Service.Implementation( ManagementBeanProvider.class )
public class CacheBean extends ManagementBeanProvider
{
    public CacheBean()
    {
        super( Cache.class );
    }

    @Override
    protected Neo4jMBean createMBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new CacheManager( this, kernel );
    }

    @Description( "Information about the caching in Neo4j" )
    private class CacheManager extends Neo4jMBean implements Cache
    {
        CacheManager( ManagementBeanProvider provider, KernelData kernel )
                throws NotCompliantMBeanException
        {
            super( provider, kernel );
            this.nodeManager = kernel.getConfig().getGraphDbModule().getNodeManager();
        }

        private final NodeManager nodeManager;

        @Description( "The type of cache used by Neo4j" )
        public String getCacheType()
        {
            return nodeManager.getCacheType().getDescription();
        }

        @Description( "The number of Nodes currently in cache" )
        public int getNodeCacheSize()
        {
            return nodeManager.getNodeCacheSize();
        }

        @Description( "The number of Relationships currently in cache" )
        public int getRelationshipCacheSize()
        {
            return nodeManager.getRelationshipCacheSize();
        }

        @Description( value = "Clears the Neo4j caches", impact = MBeanOperationInfo.ACTION )
        public void clear()
        {
            if ( true )
                throw new AccessControlException( "Clearing cache through JMX not permitted." );
            nodeManager.clearCache();
        }
    }
}
