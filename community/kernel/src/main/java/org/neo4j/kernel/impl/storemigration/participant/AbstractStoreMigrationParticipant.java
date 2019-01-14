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
package org.neo4j.kernel.impl.storemigration.participant;

import java.io.File;
import java.io.IOException;

import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;

/**
 * Default empty implementation of StoreMigrationParticipant.
 * Base class for all StoreMigrationParticipant implementations.
 *
 * @see org.neo4j.kernel.impl.storemigration.StoreUpgrader
 */
public class AbstractStoreMigrationParticipant implements StoreMigrationParticipant
{
    protected final String name;

    public AbstractStoreMigrationParticipant( String name )
    {
        this.name = name;
    }

    @Override
    public void migrate( File storeDir, File migrationDir, ProgressReporter progressMonitor,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToMigrateFrom,
            String versionToMigrateTo ) throws IOException
    {
    }

    @Override
    public void cleanup( File migrationDir ) throws IOException
    {
    }

    @Override
    public String getName()
    {
        return name;
    }
}
