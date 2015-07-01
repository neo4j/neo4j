/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.embedded.TestGraphDatabase;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactoryContractTest;

public class TestOnlineBackupExtension extends KernelExtensionFactoryContractTest
{
    public TestOnlineBackupExtension()
    {
        super( OnlineBackupExtensionFactory.KEY, OnlineBackupExtensionFactory.class );
    }

    @Override
    protected void configure( TestGraphDatabase.EphemeralBuilder builder, boolean shouldLoad, int instance )
    {
        super.configure( builder, shouldLoad, instance );
        if ( shouldLoad )
        {
            builder.withSetting( OnlineBackupSettings.online_backup_enabled, Settings.TRUE );
            builder.withSetting( OnlineBackupSettings.online_backup_server, ":" + (BackupServer.DEFAULT_PORT + instance) );
        }
    }
}
