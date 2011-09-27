/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.neo4j.kernel.Config;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;

public class KernelBean extends Neo4jMBean implements Kernel
{
    private final long kernelStartTime;
    private final long storeCreationDate;
    private final long storeId;
    private final long storeLogVersion;
    private final boolean isReadOnly;
    private final String kernelVersion;
    private final String storeDir;
    private final ObjectName query;
    private final String instanceId;

    KernelBean( KernelData kernel, ManagementSupport support ) throws NotCompliantMBeanException
    {
        super( Kernel.class, kernel, support );
        NeoStoreXaDataSource datasource = getNeoDataSource( kernel );
        this.kernelVersion = kernel.version().toString();
        this.instanceId = kernel.instanceId();
        this.query = support.createMBeanQuery( instanceId );
        storeCreationDate = datasource.getCreationTime();
        storeLogVersion = datasource.getCurrentLogVersion();
        isReadOnly = datasource.isReadOnly();
        storeId = datasource.getRandomIdentifier();

        @SuppressWarnings( "hiding" ) String storeDir;
        try
        {
            storeDir = new File( datasource.getStoreDir() ).getCanonicalFile().getAbsolutePath();
        }
        catch ( IOException e )
        {
            storeDir = new File( datasource.getStoreDir() ).getAbsolutePath();
        }
        this.storeDir = storeDir;

        kernelStartTime = new Date().getTime();
    }

    String getInstanceId()
    {
        return instanceId;
    }

    public static NeoStoreXaDataSource getNeoDataSource( KernelData kernel )
    {
        XaDataSourceManager mgr = kernel.getConfig().getTxModule().getXaDataSourceManager();
        return (NeoStoreXaDataSource) mgr.getXaDataSource( Config.DEFAULT_DATA_SOURCE_NAME );
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
}
