/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Date;
import java.util.function.Supplier;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.neo4j.jmx.Kernel;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.LogVersionRepository;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.storageengine.api.StoreId;

import static org.neo4j.function.Suppliers.lazySingleton;

@Deprecated
public class KernelBean extends Neo4jMBean implements Kernel
{
    private final long kernelStartTime;
    private final String kernelVersion;
    private final ObjectName query;
    private final String instanceId;
    private final Supplier<DatabaseInfo> databaseInfoSupplier;

    KernelBean( KernelData kernel, Database database, ManagementSupport support ) throws NotCompliantMBeanException
    {
        super( Kernel.class, kernel, support );
        this.kernelVersion = kernel.version().toString();
        this.instanceId = kernel.instanceId();
        this.query = support.createMBeanQuery( instanceId );
        this.kernelStartTime = new Date().getTime();
        this.databaseInfoSupplier = lazySingleton( () ->
        {
            StoreId storeId = database.getStoreId();
            LogVersionRepository versionRepository = database.getDependencyResolver().resolveDependency( LogVersionRepository.class );
            return new DatabaseInfo( database.isReadOnly(), storeId.getCreationTime(), storeId.getRandomId(), versionRepository.getCurrentLogVersion(),
                    database.getDatabaseName() );
        } );
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
        return new Date( databaseInfoSupplier.get().getStoreCreationDate() );
    }

    @Override
    public String getStoreId()
    {
        return Long.toHexString( databaseInfoSupplier.get().getStoreId() );
    }

    @Override
    public long getStoreLogVersion()
    {
        return databaseInfoSupplier.get().getStoreLogVersion();
    }

    @Override
    public String getKernelVersion()
    {
        return kernelVersion;
    }

    @Override
    public boolean isReadOnly()
    {
        return databaseInfoSupplier.get().isReadOnly();
    }

    @Override
    public String getDatabaseName()
    {
        return databaseInfoSupplier.get().getDatabaseName();
    }

    private class DatabaseInfo
    {
        private final boolean readOnly;
        private final long storeCreationDate;
        private final long storeId;
        private final long storeLogVersion;
        private final String databaseName;

        DatabaseInfo( boolean isReadOnly, long storeCreationDate, long storeId, long storeLogVersion, String databaseName )
        {
            this.readOnly = isReadOnly;
            this.storeCreationDate = storeCreationDate;
            this.storeId = storeId;
            this.storeLogVersion = storeLogVersion;
            this.databaseName = databaseName;
        }

        public boolean isReadOnly()
        {
            return readOnly;
        }

        long getStoreCreationDate()
        {
            return storeCreationDate;
        }

        public long getStoreId()
        {
            return storeId;
        }

        long getStoreLogVersion()
        {
            return storeLogVersion;
        }

        public String getDatabaseName()
        {
            return databaseName;
        }
    }

}
