/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.KernelData;

public class KernelBean extends Neo4jMBean implements Kernel
{
    private final long kernelStartTime;
    private final String kernelVersion;
    private final ObjectName query;
    private final String instanceId;

    private boolean isReadOnly;
    private long storeCreationDate = -1;
    private long storeId = -1;
    private String databaseName;
    private long storeLogVersion;

    KernelBean( KernelData kernel, ManagementSupport support ) throws NotCompliantMBeanException
    {
        super( Kernel.class, kernel, support );
        kernel.graphDatabase().getDependencyResolver().resolveDependency( DataSourceManager.class )
                .addListener( new DataSourceInfo() );
        this.kernelVersion = kernel.version().toString();
        this.instanceId = kernel.instanceId();
        this.query = support.createMBeanQuery( instanceId );

        kernelStartTime = new Date().getTime();
    }

    String getInstanceId()
    {
        return instanceId;
    }

    @Override
    public ObjectName getMBeanQuery()
    {
        return query;
    }

    @Override
    public Date getKernelStartTime()
    {
        return new Date( kernelStartTime );
    }

    @Override
    public Date getStoreCreationDate()
    {
        return new Date( storeCreationDate );
    }

    @Override
    public String getStoreId()
    {
        return Long.toHexString( storeId );
    }

    @Override
    public long getStoreLogVersion()
    {
        return storeLogVersion;
    }

    @Override
    public String getKernelVersion()
    {
        return kernelVersion;
    }

    @Override
    public boolean isReadOnly()
    {
        return isReadOnly;
    }

    @Override
    public String getDatabaseName()
    {
        return databaseName;
    }

    private class DataSourceInfo
            implements DataSourceManager.Listener
    {
        @Override
        public void registered( NeoStoreDataSource ds )
        {
            StoreId id = ds.getStoreId();
            storeLogVersion =
                    ds.getDependencyResolver().resolveDependency( LogVersionRepository.class ).getCurrentLogVersion();
            storeCreationDate = id.getCreationTime();
            isReadOnly = ds.isReadOnly();
            storeId = id.getRandomId();

            File storeDir = ds.getStoreDir();
            try
            {
                storeDir = storeDir.getCanonicalFile();
            }
            catch ( IOException ignored )
            {
            }

            databaseName = storeDir.getName();
        }

        @Override
        public void unregistered( NeoStoreDataSource ds )
        {
            storeCreationDate = -1;
            storeLogVersion = -1;
            isReadOnly = false;
            storeId = -1;
        }
    }
}
