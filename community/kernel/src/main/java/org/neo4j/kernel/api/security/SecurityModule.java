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
package org.neo4j.kernel.api.security;

import java.io.IOException;

import org.neo4j.annotations.service.Service;
import org.neo4j.common.DependencySatisfier;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.NamedService;

@Service
public abstract class SecurityModule implements Lifecycle, SecurityProvider, NamedService
{
    protected final LifeSupport life = new LifeSupport();

    public abstract void setup( Dependencies dependencies ) throws KernelException, IOException;

    @Override
    public void init()
    {
        life.init();
    }

    @Override
    public void start() throws Exception
    {
        life.start();
    }

    @Override
    public void stop() throws Exception
    {
        life.stop();
    }

    @Override
    public void shutdown()
    {
        life.shutdown();
    }

    public interface Dependencies
    {
        LogService logService();

        Config config();

        GlobalProcedures procedures();

        JobScheduler scheduler();

        FileSystemAbstraction fileSystem();

        DependencySatisfier dependencySatisfier();

        AccessCapability accessCapability();
    }
}
