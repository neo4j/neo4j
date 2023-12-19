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
package org.neo4j.backup;

import java.util.Map;

import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactoryContractTest;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

public class OnlineBackupExtensionIT extends KernelExtensionFactoryContractTest
{
    public OnlineBackupExtensionIT()
    {
        super( OnlineBackupExtensionFactory.KEY, OnlineBackupExtensionFactory.class );
    }

    @Override
    protected Map<String, String> configuration( boolean shouldLoad, int instance )
    {
        Map<String, String> configuration = super.configuration( shouldLoad, instance );
        if ( shouldLoad )
        {
            configuration.put( OnlineBackupSettings.online_backup_enabled.name(), Settings.TRUE );
            configuration.put( OnlineBackupSettings.online_backup_server.name(), "127.0.0.1:" + PortAuthority.allocatePort() );
        }
        return configuration;
    }
}
