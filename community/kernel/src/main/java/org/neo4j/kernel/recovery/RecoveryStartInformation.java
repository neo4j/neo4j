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
package org.neo4j.kernel.recovery;

import org.neo4j.kernel.impl.transaction.log.LogPosition;

public class RecoveryStartInformation
{
    static final RecoveryStartInformation NO_RECOVERY_REQUIRED = new RecoveryStartInformation( LogPosition.UNSPECIFIED, -1 );
    static final RecoveryStartInformation MISSING_LOGS = new RecoveryStartInformation( null, -1, true );

    private final long firstTxIdAfterLastCheckPoint;
    private final LogPosition recoveryPosition;
    private final boolean missingLogs;

    public RecoveryStartInformation( LogPosition recoveryPosition, long firstTxIdAfterLastCheckPoint )
    {
        this( recoveryPosition, firstTxIdAfterLastCheckPoint, false );
    }

    private RecoveryStartInformation( LogPosition recoveryPosition, long firstTxIdAfterLastCheckPoint, boolean missingLogs )
    {
        this.firstTxIdAfterLastCheckPoint = firstTxIdAfterLastCheckPoint;
        this.recoveryPosition = recoveryPosition;
        this.missingLogs = missingLogs;
    }

    public boolean isRecoveryRequired()
    {
        return recoveryPosition != LogPosition.UNSPECIFIED;
    }

    long getFirstTxIdAfterLastCheckPoint()
    {
        return firstTxIdAfterLastCheckPoint;
    }

    LogPosition getRecoveryPosition()
    {
        return recoveryPosition;
    }

    boolean isMissingLogs()
    {
        return missingLogs;
    }
}
