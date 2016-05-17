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
package org.neo4j.kernel.impl.locking;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.kernel.impl.api.tx.TxTermination;
import org.neo4j.kernel.impl.locking.Locks.Client;

import static org.junit.Assert.fail;

@Ignore("Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite.")
public class CloseCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public CloseCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    public void shouldNotBeAbleToHandOutClientsIfShutDown() throws Throwable
    {
        // GIVEN a lock manager and working clients
        try ( Client client = locks.newClient( TxTermination.NONE ) )
        {
            client.acquireExclusive( ResourceTypes.NODE, 0 );
        }

        // WHEN
        locks.stop();
        locks.shutdown();

        // THEN
        try
        {
            locks.newClient( TxTermination.NONE );
            fail( "Should fail" );
        }
        catch ( IllegalStateException e )
        {
            // Good
        }
    }
}
