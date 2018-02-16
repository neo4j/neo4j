/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.readreplica;

import org.junit.Test;

import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class UnfreezeReadReplicaProcedureTest
{
    @Test
    public void shouldRespondUnfreezeWhenSuccessfullyCalled() throws Exception
    {
        // given
        CatchupPollingProcess catchupPollingProcess = mock( CatchupPollingProcess.class );
        UnfreezeReadReplicaProcedure proc = new UnfreezeReadReplicaProcedure( catchupPollingProcess, NullLogProvider.getInstance().getLog( getClass() ) );

        // when
        RawIterator<Object[],ProcedureException> rawIterator = proc.apply( null, null );

        // then
        assertEquals( "unfrozen", rawIterator.next()[0] );
    }

    @Test
    public void shouldRespondFailedAndLogWhenFailedToCall() throws Throwable
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        Log log = logProvider.getLog( getClass() );

        CatchupPollingProcess catchupPollingProcess = mock( CatchupPollingProcess.class );
        doThrow( new Exception() ).when( catchupPollingProcess ).start();

        UnfreezeReadReplicaProcedure proc = new UnfreezeReadReplicaProcedure( catchupPollingProcess, log );

        // when
        try
        {
            RawIterator<Object[],ProcedureException> rawIterator = proc.apply( null, null );
        }
        catch ( ProcedureException expected )
        {
            // then
            assertThat( expected.getMessage(), containsString( "Failed to unfreeze read replica. Check neo4j.log for details." ) );
        }

        // then
        logProvider.assertContainsLogCallContaining( "Failed to unfreeze read replica." );
    }
}
