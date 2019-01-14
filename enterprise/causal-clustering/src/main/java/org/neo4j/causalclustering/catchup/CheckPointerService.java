/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.LockingExecutor;

public class CheckPointerService
{
    private final Supplier<CheckPointer> checkPointerSupplier;
    private final Executor lockingCheckpointExecutor;

    public CheckPointerService( Supplier<CheckPointer> checkPointerSupplier, JobScheduler jobScheduler, JobScheduler.Group group )
    {
        this.checkPointerSupplier = checkPointerSupplier;
        this.lockingCheckpointExecutor = new LockingExecutor( jobScheduler, group );
    }

    public CheckPointer getCheckPointer()
    {
        return checkPointerSupplier.get();
    }

    public long lastCheckPointedTransactionId()
    {
        return checkPointerSupplier.get().lastCheckPointedTransactionId();
    }

    public void tryAsyncCheckpoint( Consumer<IOException> exceptionHandler )
    {
        lockingCheckpointExecutor.execute( () ->
        {
            try
            {
                getCheckPointer().tryCheckPoint( new SimpleTriggerInfo( "Store file copy" ) );
            }
            catch ( IOException e )
            {
                exceptionHandler.accept( e );
            }
        } );
    }
}
