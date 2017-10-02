/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.backup;

import java.util.Map;

import org.neo4j.com.ports.allocation.PortAuthority;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactoryContractTest;
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
