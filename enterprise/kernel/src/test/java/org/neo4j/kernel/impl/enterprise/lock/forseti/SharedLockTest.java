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
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class SharedLockTest
{

    @Test
    public void shouldUpgradeToUpdateLock()
    {
        // Given
        ForsetiClient clientA = mock( ForsetiClient.class );
        ForsetiClient clientB = mock( ForsetiClient.class );

        SharedLock lock = new SharedLock( clientA );
        lock.acquire( clientB );

        // When
        assertTrue( lock.tryAcquireUpdateLock( clientA ) );

        // Then
        assertThat( lock.numberOfHolders(), equalTo( 2 ) );
        assertThat( lock.isUpdateLock(), equalTo( true ) );
    }

    @Test
    public void shouldReleaseSharedLock()
    {
        // Given
        ForsetiClient clientA = mock( ForsetiClient.class );
        SharedLock lock = new SharedLock( clientA );

        // When
        assertTrue( lock.release( clientA ) );

        // Then
        assertThat( lock.numberOfHolders(), equalTo( 0 ) );
        assertThat( lock.isUpdateLock(), equalTo( false ) );
    }

}
