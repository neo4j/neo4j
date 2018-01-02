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
package org.neo4j.kernel.ha.com.slave;

import org.junit.Test;

import org.neo4j.com.IllegalProtocolVersionException;
import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.kernel.ha.MasterClient210;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.kernel.monitoring.Monitors;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class MasterClientResolverTest
{
    @Test
    public void shouldResolveMasterClientFactory() throws Exception
    {
        // Given
        MasterClientResolver resolver = new MasterClientResolver( NullLogProvider.getInstance(),
                ResponseUnpacker.NO_OP_RESPONSE_UNPACKER, mock( InvalidEpochExceptionHandler.class ), 1, 1, 1, 1024 );

        LifeSupport life = new LifeSupport();
        try
        {
            life.start();
            MasterClient masterClient1 =
                    resolver.instantiate( "cluster://localhost", 44, null, new Monitors(), StoreId.DEFAULT, life );
            assertThat( masterClient1, instanceOf( MasterClient214.class ) );
        }
        finally
        {
            life.shutdown();
        }

        IllegalProtocolVersionException illegalProtocolVersionException = new IllegalProtocolVersionException(
                MasterClient210.PROTOCOL_VERSION.getApplicationProtocol(),
                MasterClient214.PROTOCOL_VERSION.getApplicationProtocol(),
                "Protocol is too modern" );

        // When
        resolver.handle( illegalProtocolVersionException );

        // Then
        life = new LifeSupport();
        try
        {
            life.start();
            MasterClient masterClient2 =
                    resolver.instantiate( "cluster://localhost", 55, null, new Monitors(), StoreId.DEFAULT, life );

            assertThat( masterClient2, instanceOf( MasterClient210.class ) );
        }
        finally
        {
            life.shutdown();
        }
    }
}
