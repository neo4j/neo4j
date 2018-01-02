/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.graphdb.event.ErrorState.TX_MANAGER_NOT_OK;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class KernelHealthTest
{
    @Test
    public void shouldGenerateKernelPanicEvents() throws Exception
    {
        // GIVEN
        KernelPanicEventGenerator generator = mock( KernelPanicEventGenerator.class );
        KernelHealth kernelHealth = new KernelHealth( generator, NullLogProvider.getInstance().getLog( KernelHealth.class ) );
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
        AssertableLogProvider logProvider = new AssertableLogProvider();
        KernelHealth kernelHealth = new KernelHealth( mock( KernelPanicEventGenerator.class ), logProvider.getLog( KernelHealth.class ) );
        kernelHealth.healed();

        // WHEN
        String message = "Listen everybody... panic!";
        Exception exception = new Exception( message );
        kernelHealth.panic( exception );

        // THEN
        logProvider.assertAtLeastOnce(
                inLog( KernelHealth.class ).error(
                        is("setting TM not OK. Kernel has encountered some problem, please perform necessary action (tx recovery/restart)" ),
                        sameInstance( exception )
                )
        );
    }
}
