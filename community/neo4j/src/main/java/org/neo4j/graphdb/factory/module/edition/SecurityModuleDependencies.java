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
package org.neo4j.graphdb.factory.module.edition;

import org.neo4j.common.DependencySatisfier;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

class SecurityModuleDependencies implements SecurityModule.Dependencies
{
    private final GlobalModule globalModule;
    private final GlobalProcedures globalProcedures;

    SecurityModuleDependencies( GlobalModule globalModule, GlobalProcedures globalProcedures )
    {
        this.globalModule = globalModule;
        this.globalProcedures = globalProcedures;
    }

    @Override
    public LogService logService()
    {
        return globalModule.getLogService();
    }

    @Override
    public Config config()
    {
        return globalModule.getGlobalConfig();
    }

    @Override
    public GlobalProcedures procedures()
    {
        return globalProcedures;
    }

    @Override
    public JobScheduler scheduler()
    {
        return globalModule.getJobScheduler();
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return globalModule.getFileSystem();
    }

    @Override
    public DependencySatisfier dependencySatisfier()
    {
        return globalModule.getGlobalDependencies();
    }

    @Override
    public GlobalTransactionEventListeners transactionEventListeners()
    {
        return globalModule.getTransactionEventListeners();
    }
}
