/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.spi;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.udc.UsageDataKeys;

/**
 * Default implementation of {@link KernelContext}
 */
public class SimpleKernelContext implements KernelContext
{
    private final FileSystemAbstraction fileSystem;
    private final File storeDir;
    private final UsageDataKeys.OperationalMode operationalMode;

    public SimpleKernelContext( FileSystemAbstraction fileSystem, File storeDir, UsageDataKeys.OperationalMode operationalMode )
    {
        this.fileSystem = fileSystem;
        this.storeDir = storeDir;
        this.operationalMode = operationalMode;
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return fileSystem;
    }

    @Override
    public File storeDir()
    {
        return storeDir;
    }

    @Override
    public UsageDataKeys.OperationalMode operationalMode()
    {
        return operationalMode;
    }
}
