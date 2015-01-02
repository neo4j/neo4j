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
package org.neo4j.jmx.impl;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.neo4j.jmx.Kernel;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.DataSourceRegistrationListener;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class KernelBean extends Neo4jMBean implements Kernel
{
    private final long kernelStartTime;
    private final String kernelVersion;
    private final ObjectName query;
    private final String instanceId;

    private final DataSourceInfo dataSourceInfo;
    private boolean isReadOnly;
    private long storeCreationDate = -1;
    private long storeId = -1;
    private String storeDir = null;
    private long storeLogVersion;

    KernelBean( KernelData kernel, ManagementSupport support ) throws NotCompliantMBeanException
    {
        super( Kernel.class, kernel, support );
        dataSourceInfo = new DataSourceInfo();
        kernel.graphDatabase().getDependencyResolver().resolveDependency( XaDataSourceManager.class )
                .addDataSourceRegistrationListener( dataSourceInfo );
        this.kernelVersion = kernel.version().toString();
        this.instanceId = kernel.instanceId();
        this.query = support.createMBeanQuery( instanceId );

        kernelStartTime = new Date().getTime();
    }

    String getInstanceId()
    {
        return instanceId;
    }

    public ObjectName getMBeanQuery()
    {
        return query;
    }

    public Date getKernelStartTime()
    {
        return new Date( kernelStartTime );
    }

    public Date getStoreCreationDate()
    {
        return new Date( storeCreationDate );
    }

    public String getStoreId()
    {
        return Long.toHexString( storeId );
    }

    public long getStoreLogVersion()
    {
        return storeLogVersion;
    }

    public String getKernelVersion()
    {
        return kernelVersion;
    }

    public boolean isReadOnly()
    {
        return isReadOnly;
    }

    public String getStoreDirectory()
    {
        return storeDir;
    }

    private class DataSourceInfo
            implements DataSourceRegistrationListener
    {
        @Override
        public void registeredDataSource( XaDataSource ds )
        {
            if ( ds instanceof NeoStoreXaDataSource )
            {
                NeoStoreXaDataSource datasource = (NeoStoreXaDataSource) ds;
                storeCreationDate = datasource.getCreationTime();
                storeLogVersion = datasource.getCurrentLogVersion();
                isReadOnly = datasource.isReadOnly();
                storeId = datasource.getRandomIdentifier();

                try
                {
                    storeDir = new File( datasource.getStoreDir() ).getCanonicalFile().getAbsolutePath();
                }
                catch ( IOException e )
                {
                    storeDir = new File( datasource.getStoreDir() ).getAbsolutePath();
                }
            }
        }

        @Override
        public void unregisteredDataSource( XaDataSource ds )
        {
            if ( ds instanceof NeoStoreXaDataSource )
            {
                storeCreationDate = -1;
                storeLogVersion = -1;
                isReadOnly = false;
                storeId = -1;
                storeDir = null;
            }
        }
    }
}
