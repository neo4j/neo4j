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
package org.neo4j.storageengine.migration;

import java.io.IOException;
import java.util.List;

import org.neo4j.common.ProgressReporter;
import org.neo4j.exceptions.KernelException;
import org.neo4j.io.layout.DatabaseLayout;

public class CompositeStoreMigrationParticipant extends AbstractStoreMigrationParticipant
{
    private final List<StoreMigrationParticipant> participantList;

    public CompositeStoreMigrationParticipant( String name, List<StoreMigrationParticipant> participantList )
    {
        super( name );
        this.participantList = participantList;
    }

    @Override
    public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, ProgressReporter progress, String versionToMigrateFrom,
            String versionToMigrateTo ) throws IOException, KernelException
    {
        for ( StoreMigrationParticipant participant : participantList )
        {
            participant.migrate( directoryLayout, migrationLayout, progress, versionToMigrateFrom, versionToMigrateTo );
        }
    }

    @Override
    public void moveMigratedFiles( DatabaseLayout migrationLayout, DatabaseLayout directoryLayout, String versionToMigrateFrom, String versionToMigrateTo )
            throws IOException
    {
        for ( StoreMigrationParticipant participant : participantList )
        {
            participant.moveMigratedFiles( migrationLayout, directoryLayout, versionToMigrateFrom, versionToMigrateTo );
        }
    }

    @Override
    public void cleanup( DatabaseLayout migrationLayout ) throws IOException
    {
        for ( StoreMigrationParticipant participant : participantList )
        {
            participant.cleanup( migrationLayout );
        }
    }
}
