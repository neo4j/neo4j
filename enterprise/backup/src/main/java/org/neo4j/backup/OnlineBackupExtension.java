/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.KernelExtension;

@Service.Implementation( KernelExtension.class )
public class OnlineBackupExtension extends KernelExtension<BackupServer>
{
    static final String KEY = "online backup";
    
    public interface Configuration
    {
        boolean online_backup_enabled(boolean def);
        int online_backup_port(int def);
    }

    public OnlineBackupExtension()
    {
        super( KEY );
    }

    @Override
    public Class getSettingsClass()
    {
        return OnlineBackupSettings.class;
    }

    @Override
    protected void loadConfiguration( KernelData kernel )
    {
    }

    @Override
    protected BackupServer load( KernelData kernel )
    {
        if (kernel.getConfig().getBoolean( OnlineBackupSettings.online_backup_enabled ))
        {
            TheBackupInterface backup = new BackupImpl( kernel.graphDatabase() );
            return new BackupServer( backup, kernel.getConfig().getInteger( OnlineBackupSettings.online_backup_port ),
                                                    kernel.graphDatabase().getMessageLog());
        } else
            return null;
    }

    @Override
    protected void unload( BackupServer server )
    {
        server.shutdown();
    }
}
