/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
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
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new CacheManager( management );
    }

    private class CacheManager extends Neo4jMBean implements Cache
    {
        CacheManager( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.nodeManager = management.getKernelData().getConfig().getGraphDbModule().getNodeManager();
        }

        private final NodeManager nodeManager;

        public String getCacheType()
        {
            return nodeManager.getCacheType().getDescription();
        }

        public int getNodeCacheSize()
        {
            return nodeManager.getNodeCacheSize();
        }

        public int getRelationshipCacheSize()
        {
            return nodeManager.getRelationshipCacheSize();
        }

        public void clear()
        {
            nodeManager.clearCache();
        }
    }
}
