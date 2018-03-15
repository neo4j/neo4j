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
package org.neo4j.causalclustering.core;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

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
        Assert.assertEquals( new ListenSocketAddress( "127.0.0.1", 6362 ), subject.backupAddressForTxProtocol( config ) );
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
            Assert.assertEquals( new ListenSocketAddress( "127.0.0.1", DEFAULT.getPort() ), resolvedAddress );
        }
    }
}
