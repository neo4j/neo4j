/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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


import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helper.RepeatUntilCallable;

import static org.junit.Assert.assertTrue;

class StartStop extends RepeatUntilCallable
{
    private final AtomicReference<GraphDatabaseService> dbRef;
    private final Factory<GraphDatabaseService> factory;

    StartStop( BooleanSupplier keepGoing, Runnable onFailure, Factory<GraphDatabaseService> factory,
            AtomicReference<GraphDatabaseService> dbRef )
    {
        super( keepGoing, onFailure );
        this.factory = factory;
        this.dbRef = dbRef;
    }

    @Override
    protected void doWork()
    {
        GraphDatabaseService db = dbRef.get();
        db.shutdown();
        LockSupport.parkNanos( 5_000_000_000L );
        boolean replaced = dbRef.compareAndSet( db, factory.newInstance() );
        assertTrue( replaced );
    }
}
