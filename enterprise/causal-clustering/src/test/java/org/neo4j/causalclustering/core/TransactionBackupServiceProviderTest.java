/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.core;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import static org.junit.Assert.assertEquals;

public class TransactionBackupServiceProviderTest
{
    private static final HostnamePort DEFAULT = Config.defaults().get( OnlineBackupSettings.online_backup_server );

    @Test
    public void backupWithRangeCanBeProcessed()
    {
        // given
        TransactionBackupServiceAddressResolver subject = new TransactionBackupServiceAddressResolver();

        // and config with range
        Config config = Config.defaults();
        config.augment( OnlineBackupSettings.online_backup_server, "127.0.0.1:6362-6372" );

        // then
        assertEquals( new ListenSocketAddress( "127.0.0.1", 6362 ), subject.backupAddressForTxProtocol( config ) );
    }

    @Test
    public void backupOverrideWithoutPortGetsDefaultPort()
    {
        // with params
        List<String> params = Arrays.asList( "127.0.0.1:", "127.0.0.1" );
        for ( String testedValue : params )
        {
            // given
            TransactionBackupServiceAddressResolver subject = new TransactionBackupServiceAddressResolver();

            // and config without a port
            Config config = Config.defaults();
            config.augment( OnlineBackupSettings.online_backup_server, testedValue );

            // when
            ListenSocketAddress resolvedAddress = subject.backupAddressForTxProtocol( config );

            // then
            assertEquals( new ListenSocketAddress( "127.0.0.1", DEFAULT.getPort() ), resolvedAddress );
        }
    }
}
