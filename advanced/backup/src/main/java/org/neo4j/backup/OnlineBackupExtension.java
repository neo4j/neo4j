/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.neo4j.kernel.KernelExtension;

@Service.Implementation( KernelExtension.class )
public class OnlineBackupExtension extends KernelExtension
{
    public OnlineBackupExtension()
    {
        super( "online backup" );
    }
    
    @Override
    protected void load( KernelData kernel )
    {
        String configValue = (String) kernel.getConfig().getParams().get( "enable_online_backup" );
        configValue = configValue == null ? "true" : configValue;
        boolean enabled = Boolean.parseBoolean( configValue );
        if ( enabled )
        {
            TheBackupInterface backup = new BackupImpl( kernel.graphDatabase() );
            BackupServer server = new BackupServer( backup, BackupServer.DEFAULT_PORT, null );
            kernel.setState( this, server );
        }
    }
    
    @Override
    protected void unload( KernelData kernel )
    {
        BackupServer server = (BackupServer) kernel.getState( this );
        if ( server != null )
        {
            server.shutdown();
        }
    }
}
