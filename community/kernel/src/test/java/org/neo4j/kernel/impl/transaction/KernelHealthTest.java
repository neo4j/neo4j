/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import org.junit.Test;

import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.logging.BufferingLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SingleLoggingService;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.event.ErrorState.TX_MANAGER_NOT_OK;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class KernelHealthTest
{
    @Test
    public void shouldGenerateKernelPanicEvents() throws Exception
    {
        // GIVEN
        KernelPanicEventGenerator generator = mock( KernelPanicEventGenerator.class );
        KernelHealth kernelHealth = new KernelHealth( generator, new SingleLoggingService( DEV_NULL ) );
        kernelHealth.healed();

        // WHEN
        Exception cause = new Exception( "My own fault" );
        kernelHealth.panic( cause );
        kernelHealth.panic( cause );

        // THEN
        verify( generator, times( 1 ) ).generateEvent( TX_MANAGER_NOT_OK, cause );
    }

    @Test
    public void shouldLogKernelPanicEvent() throws Exception
    {
        // GIVEN
        BufferingLogger logger = new BufferingLogger();
        Logging logging = mock( Logging.class );
        when( logging.getMessagesLog( any( Class.class ) ) ).thenReturn( logger );
        KernelHealth kernelHealth = new KernelHealth( mock( KernelPanicEventGenerator.class ), logging );
        kernelHealth.healed();

        // WHEN
        String message = "Listen everybody... panic!";
        kernelHealth.panic( new Exception( message ) );

        // THEN
        assertThat( logger.toString(), containsString( message ) );
    }
}
