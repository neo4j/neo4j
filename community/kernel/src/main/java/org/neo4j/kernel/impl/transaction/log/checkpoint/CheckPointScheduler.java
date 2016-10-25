/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.io.IOException;

import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.PeriodicBackgroundTask;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.util.JobScheduler.Groups.checkPoint;

public class CheckPointScheduler extends PeriodicBackgroundTask
{
    private final CheckPointer checkPointer;

    public CheckPointScheduler( CheckPointer checkPointer, JobScheduler scheduler, long recurringPeriodMillis,
                                LogProvider logProvider )
    {
        super( scheduler, recurringPeriodMillis, checkPoint, logProvider );
        this.checkPointer = checkPointer;
    }

    @Override
    protected void performTask() throws IOException
    {
        checkPointer.checkPointIfNeeded( new SimpleTriggerInfo( "scheduler" ) );
    }
}
