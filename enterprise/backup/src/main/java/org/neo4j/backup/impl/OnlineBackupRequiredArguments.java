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
package org.neo4j.backup.impl;

import java.nio.file.Path;

import org.neo4j.kernel.impl.util.OptionalHostnamePort;

class OnlineBackupRequiredArguments
{
    private final OptionalHostnamePort address;
    private final Path directory;
    private final String name;
    private final boolean fallbackToFull;
    private final boolean doConsistencyCheck;
    private final long timeout;
    private final Path reportDir;
    private final SelectedBackupProtocol selectedBackupProtocol;

    OnlineBackupRequiredArguments( OptionalHostnamePort address, Path directory, String name, SelectedBackupProtocol selectedBackupProtocol,
            boolean fallbackToFull, boolean doConsistencyCheck, long timeout, Path reportDir )
    {
        this.address = address;
        this.directory = directory;
        this.name = name;
        this.fallbackToFull = fallbackToFull;
        this.doConsistencyCheck = doConsistencyCheck;
        this.timeout = timeout;
        this.reportDir = reportDir;
        this.selectedBackupProtocol = selectedBackupProtocol;
    }

    public OptionalHostnamePort getAddress()
    {
        return address;
    }

    public Path getDirectory()
    {
        return directory;
    }

    public String getName()
    {
        return name;
    }

    public boolean isFallbackToFull()
    {
        return fallbackToFull;
    }

    public boolean isDoConsistencyCheck()
    {
        return doConsistencyCheck;
    }

    public long getTimeout()
    {
        return timeout;
    }

    public Path getReportDir()
    {
        return reportDir;
    }

    public Path getResolvedLocationFromName()
    {
        return directory.resolve( name );
    }

    public SelectedBackupProtocol getSelectedBackupProtocol()
    {
        return selectedBackupProtocol;
    }
}
