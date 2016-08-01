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
package org.neo4j.coreedge.core.state;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertTrue;

public class CoreStateApplierTest
{
    @Test
    public void shouldBeAbortable() throws Exception
    {
        // given
        CoreStateApplier applier = new CoreStateApplier( NullLogProvider.getInstance() );
        CountDownLatch latch = new CountDownLatch( 1 );

        applier.submit( status -> () -> {
            while ( !status.isCancelled() )
            {
                try
                {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                }
            }

            latch.countDown();
        } );

        // when
        applier.sync( true );

        // then
        assertTrue( latch.await( 10, TimeUnit.SECONDS ) );
    }
}
