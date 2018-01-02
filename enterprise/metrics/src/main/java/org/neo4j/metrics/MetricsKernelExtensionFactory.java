/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.metrics;

import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;

public class MetricsKernelExtensionFactory
        extends KernelExtensionFactory<MetricsKernelExtensionFactory.Dependencies>
{
    public interface Dependencies
    {
        Config configuration();

        LogService logService();

        // Things to get metrics from
        TransactionCounters transactionCounters();

        PageCacheMonitor pageCacheCounters();

        CheckPointerMonitor checkPointerCounters();

        LogRotationMonitor logRotationCounters();

        IdGeneratorFactory idGeneratorFactory();

        Monitors monitors();

        KernelContext kernelContext();
    }

    public MetricsKernelExtensionFactory()
    {
        super( "metrics" );
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        return new MetricsExtension( dependencies );
    }

    @Override
    public Class getSettingsClass()
    {
        return MetricsSettings.class;
    }
}
