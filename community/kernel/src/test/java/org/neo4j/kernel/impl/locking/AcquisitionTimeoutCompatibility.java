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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@Ignore( "Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite." )
public class AcquisitionTimeoutCompatibility extends LockingCompatibilityTestSuite.Compatibility
{

    private final long TEST_TIMEOUT = 4000;
    private FakeClock clock;
    private Config customConfig;
    private Locks lockManager;
    private Locks.Client client;
    private Locks.Client client2;

    public AcquisitionTimeoutCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Before
    public void setUp()
    {
        customConfig = new Config( MapUtil.stringMap( GraphDatabaseSettings.lock_acquisition_timeout.name(), "100ms"
        ) );
        clock = Clocks.fakeClock(100000, TimeUnit.MINUTES);
        lockManager = suite.createLockManager( customConfig, clock );
        client = lockManager.newClient();
        client2 = lockManager.newClient();
    }

    @After
    public void tearDown()
    {
        client2.close();
        client.close();
        lockManager.close();
    }

    @Test( timeout = TEST_TIMEOUT )
    public void terminateSharedLockAcquisition() throws ExecutionException, InterruptedException
    {
        client.acquireExclusive( ResourceTypes.NODE, 1 );
        Future<Boolean> sharedLockAcquisition = threadB.execute( state ->
        {
            client2.acquireShared( ResourceTypes.NODE, 1 );
            return true;
        } );

        assertFalse( sharedLockAcquisition.isDone() );
        clock.forward( 101, TimeUnit.MILLISECONDS );

        verifyAcquisitionFailure( sharedLockAcquisition );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void terminateExclusiveLockAcquisitionForExclusivelyLockedResource() throws InterruptedException
    {
        client.acquireExclusive( ResourceTypes.NODE, 1 );
        Future<Boolean> exclusiveLockAcquisition = threadB.execute( state ->
        {
            client2.acquireExclusive( ResourceTypes.NODE, 1 );
            return true;
        } );

        assertFalse( exclusiveLockAcquisition.isDone() );
        clock.forward( 101, TimeUnit.MILLISECONDS );

        verifyAcquisitionFailure( exclusiveLockAcquisition );
    }

    @Test (timeout = TEST_TIMEOUT)
    public void terminateExclusiveLockAcquisitionForSharedLockedResource() throws InterruptedException
    {
        client.acquireShared( ResourceTypes.NODE, 1 );
        Future<Boolean> exclusiveLockAcquisition = threadB.execute( state ->
        {
            client2.acquireExclusive( ResourceTypes.NODE, 1 );
            return true;
        } );

        assertFalse( exclusiveLockAcquisition.isDone() );
        clock.forward( 101, TimeUnit.MILLISECONDS );

        verifyAcquisitionFailure( exclusiveLockAcquisition );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void terminateExclusiveLockAcquisitionForSharedLockedResourceWithSharedLockHeld() throws InterruptedException
    {
        client.acquireShared( ResourceTypes.NODE, 1 );
        client2.acquireShared( ResourceTypes.NODE, 1 );
        Future<Boolean> exclusiveLockAcquisition = threadB.execute( state ->
        {
            client2.acquireExclusive( ResourceTypes.NODE, 1 );
            return true;
        } );

        assertFalse( exclusiveLockAcquisition.isDone() );
        clock.forward( 101, TimeUnit.MILLISECONDS );

        verifyAcquisitionFailure( exclusiveLockAcquisition );
    }

    private void verifyAcquisitionFailure( Future<Boolean> lockAcquisition ) throws InterruptedException
    {
        try
        {
            lockAcquisition.get();
            fail("Lock acquisition should fail.");
        }
        catch ( ExecutionException e )
        {
            assertThat( Exceptions.rootCause( e ), instanceOf( LockAcquisitionTimeoutException.class ) );
        }
    }
}
