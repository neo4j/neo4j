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
package org.neo4j.backup.stresstests;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.causalclustering.stresstests.Control;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helper.Workload;

import static org.junit.Assert.assertTrue;

class StartStop extends Workload
{
    private final AtomicReference<GraphDatabaseService> dbRef;
    private final Factory<GraphDatabaseService> factory;

    StartStop( Control control, Factory<GraphDatabaseService> factory, AtomicReference<GraphDatabaseService> dbRef )
    {
        super( control );
        this.factory = factory;
        this.dbRef = dbRef;
    }

    @Override
    protected void doWork()
    {
        GraphDatabaseService db = dbRef.get();
        db.shutdown();
        LockSupport.parkNanos( TimeUnit.SECONDS.toNanos( 5 ) );
        boolean replaced = dbRef.compareAndSet( db, factory.newInstance() );
        assertTrue( replaced );
    }
}
