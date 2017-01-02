/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage.id;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;
import org.neo4j.kernel.impl.store.id.BufferingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Storage id controller that provide buffering possibilities to be able so safely free and reuse ids.
 * Allows perform clear and maintenance operations over currently buffered set of ids.
 * @see BufferingIdGeneratorFactory
 */
public class BufferedIdController extends LifecycleAdapter implements IdController
{

    private final BufferingIdGeneratorFactory bufferingIdGeneratorFactory;
    private final JobScheduler scheduler;
    private JobScheduler.JobHandle jobHandle;

    public BufferedIdController( IdGeneratorFactory idGeneratorFactory,
            Supplier<KernelTransactionsSnapshot> transactionsSnapshotSupplier, IdReuseEligibility eligibleForReuse,
            IdTypeConfigurationProvider idTypeConfigurationProvider, JobScheduler scheduler )
    {
        this.scheduler = scheduler;
        bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory(
                idGeneratorFactory, transactionsSnapshotSupplier, eligibleForReuse, idTypeConfigurationProvider );
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return bufferingIdGeneratorFactory;
    }

    @Override
    public void start() throws Throwable
    {
        jobHandle = scheduler.scheduleRecurring( JobScheduler.Groups.storageMaintenance, this::maintenance, 1,
                TimeUnit.SECONDS );
    }

    @Override
    public void stop() throws Throwable
    {
        jobHandle.cancel( false );
    }

    @Override
    public void clear()
    {
        bufferingIdGeneratorFactory.clear();
    }

    @Override
    public void maintenance()
    {
        bufferingIdGeneratorFactory.maintenance();
    }
}
