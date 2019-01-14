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

import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.DependencySatisfier;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

class SecurityModuleDependenciesDependencies implements SecurityModule.Dependencies
{
    private final PlatformModule platformModule;
    private final AbstractEditionModule editionModule;
    private final Procedures procedures;

    SecurityModuleDependenciesDependencies( PlatformModule platformModule, AbstractEditionModule editionModule, Procedures procedures )
    {
        this.platformModule = platformModule;
        this.editionModule = editionModule;
        this.procedures = procedures;
    }

    @Override
    public LogService logService()
    {
        return platformModule.logging;
    }

    @Override
    public Config config()
    {
        return platformModule.config;
    }

    @Override
    public Procedures procedures()
    {
        return procedures;
    }

    @Override
    public JobScheduler scheduler()
    {
        return platformModule.jobScheduler;
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return platformModule.fileSystem;
    }

    @Override
    public DependencySatisfier dependencySatisfier()
    {
        return platformModule.dependencies;
    }

    @Override
    public AccessCapability accessCapability()
    {
        return editionModule.accessCapability;
    }
}
