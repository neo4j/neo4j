/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.management.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.management.MemoryMapping;
import org.neo4j.management.WindowPoolInfo;

@Service.Implementation( ManagementBeanProvider.class )
public final class MemoryMappingBean extends ManagementBeanProvider
{
    public MemoryMappingBean()
    {
        super( MemoryMapping.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new MemoryMappingImpl( management );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        return new MemoryMappingImpl( management, true );
    }

    private static class MemoryMappingImpl extends Neo4jMBean implements MemoryMapping
    {
        private final NeoStoreDataSource datasource;

        MemoryMappingImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.datasource = neoDataSource( management );
        }

        private NeoStoreDataSource neoDataSource( ManagementData management )
        {
            return management.resolveDependency( DataSourceManager.class ).getDataSource();
        }

        MemoryMappingImpl( ManagementData management, boolean isMxBean )
        {
            super( management, isMxBean );
            this.datasource = neoDataSource( management );
        }

        @Override
        public WindowPoolInfo[] getMemoryPools()
        {
            return getMemoryPoolsImpl( datasource );
        }

        public static WindowPoolInfo[] getMemoryPoolsImpl( NeoStoreDataSource datasource )
        {
            return new WindowPoolInfo[0];
        }
    }
}
