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
package org.neo4j.backup;

import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.helpers.OptionalHostnamePort;

public class OnlineBackupRequiredArguments
{
    private final OptionalHostnamePort address;
    private final Path folder;
    private final String name;
    private final boolean fallbackToFull;
    private final boolean doConsistencyCheck;
    private final long timeout;
    private final Optional<Path> additionalConfig;
    private final Path reportDir;

    public OnlineBackupRequiredArguments( OptionalHostnamePort address, Path folder, String name, boolean fallbackToFull, boolean doConsistencyCheck,
            long timeout,
            Optional<Path> additionalConfig, Path reportDir )
    {
        this.address = address;
        this.folder = folder;
        this.name = name;
        this.fallbackToFull = fallbackToFull;
        this.doConsistencyCheck = doConsistencyCheck;
        this.timeout = timeout;
        this.additionalConfig = additionalConfig;
        this.reportDir = reportDir;
    }

    public OptionalHostnamePort getAddress()
    {
        return address;
    }

    public Path getFolder()
    {
        return folder;
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

    public Optional<Path> getAdditionalConfig()
    {
        return additionalConfig;
    }

    public Path getReportDir()
    {
        return reportDir;
    }
}
