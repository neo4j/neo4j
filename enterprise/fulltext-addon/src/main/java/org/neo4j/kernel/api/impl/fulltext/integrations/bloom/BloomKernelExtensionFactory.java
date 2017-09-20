/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.scheduler.JobScheduler;

/**
 * A {@link KernelExtensionFactory} for the bloom fulltext addon.
 *
 * @see BloomProcedures
 * @see LoadableBloomFulltextConfig
 */
public class BloomKernelExtensionFactory extends KernelExtensionFactory<BloomKernelExtensionFactory.Dependencies>
{

    private static final String SERVICE_NAME = "bloom";

    public interface Dependencies
    {
        Config getConfig();

        GraphDatabaseService db();

        FileSystemAbstraction fileSystem();

        Procedures procedures();

        LogService logService();

        AvailabilityGuard availabilityGuard();

        JobScheduler scheduler();
    }

    BloomKernelExtensionFactory()
    {
        super( SERVICE_NAME );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        FileSystemAbstraction fs = dependencies.fileSystem();
        File storeDir = context.storeDir();
        Config config = dependencies.getConfig();
        GraphDatabaseService db = dependencies.db();
        Procedures procedures = dependencies.procedures();
        LogService logService = dependencies.logService();
        AvailabilityGuard availabilityGuard = dependencies.availabilityGuard();
        JobScheduler scheduler = dependencies.scheduler();
        return new BloomKernelExtension(
                fs, storeDir, config, db, procedures, logService, availabilityGuard, scheduler );
    }
}
