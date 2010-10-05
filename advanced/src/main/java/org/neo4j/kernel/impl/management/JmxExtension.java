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

package org.neo4j.kernel.impl.management;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

@Service.Implementation( KernelExtension.class )
public final class JmxExtension extends KernelExtension
{
    public JmxExtension()
    {
        super( "kernel jmx" );
    }

    @Override
    protected void load( final KernelData kernel )
    {
        final NodeManager nodeManager = kernel.getConfig().getGraphDbModule().getNodeManager();
        kernel.setState(
                this,
                Neo4jMBean.initMBeans( new Neo4jMBean.Creator(
                        kernel.instanceId(),
                        kernel.version(),
                        (NeoStoreXaDataSource) kernel.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                                "nioneodb" ) )
                {
                    @Override
                    protected void create( Neo4jMBean.Factory jmx )
                    {
                        jmx.createDynamicConfigurationMBean( kernel.getConfigParams() );
                        jmx.createPrimitiveMBean( nodeManager );
                        jmx.createStoreFileMBean();
                        jmx.createCacheMBean( nodeManager );
                        jmx.createLockManagerMBean( kernel.getConfig().getLockManager() );
                        jmx.createTransactionManagerMBean( kernel.getConfig().getTxModule() );
                        jmx.createMemoryMappingMBean( kernel.getConfig().getTxModule().getXaDataSourceManager() );
                        jmx.createXaManagerMBean( kernel.getConfig().getTxModule().getXaDataSourceManager() );
                    }
                } ) );
    }

    @Override
    protected void unload( KernelData kernel )
    {
        ( (Runnable) kernel.getState( this ) ).run();
    }

    public <T> T getBean( KernelData kernel, Class<T> beanClass )
    {
        if ( !isLoaded( kernel ) ) throw new IllegalStateException( "Not Loaded!" );
        return Neo4jMBean.getBean( kernel.instanceId(), beanClass );
    }
}
