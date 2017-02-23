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
package org.neo4j.backup.stresstests;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import org.neo4j.backup.BackupHelper;
import org.neo4j.backup.BackupResult;
import org.neo4j.helper.RepeatUntilCallable;

class BackupLoad extends RepeatUntilCallable
{

    private final String backupHostname;
    private final int backupPort;
    private final File backupDir;

    BackupLoad( BooleanSupplier keepGoing, Runnable onFailure, String backupHostname, int backupPort, File backupDir )
    {
        super( keepGoing, onFailure );
        this.backupHostname = backupHostname;
        this.backupPort = backupPort;
        this.backupDir = backupDir;
    }

    @Override
    protected void doWork()
    {
        BackupResult backupResult = BackupHelper.backup( backupHostname, backupPort, backupDir );
        if ( !backupResult.isConsistent() )
        {
            throw new RuntimeException( "Inconsistent backup" );
        }
        if ( backupResult.isTransientErrorOnBackup() )
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
        }
    }
}
