/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup.stresstests;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.backup.BackupHelper;
import org.neo4j.backup.BackupResult;
import org.neo4j.causalclustering.stresstests.Control;
import org.neo4j.helper.Workload;

class BackupLoad extends Workload
{

    private final String backupHostname;
    private final int backupPort;
    private final Path backupDir;

    BackupLoad( Control control, String backupHostname, int backupPort, Path backupDir )
    {
        super( control );
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
