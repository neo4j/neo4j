/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.dbms.identity;

import java.util.UUID;

import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;

public class DefaultIdentityModule extends AbstractIdentityModule
{
    private final ServerId serverId;

    public static DefaultIdentityModule fromGlobalModule( GlobalModule globalModule )
    {
        return new DefaultIdentityModule( globalModule.getLogService(),
                globalModule.getFileSystem(),
                globalModule.getNeo4jLayout(),
                globalModule.getOtherMemoryPool().getPoolMemoryTracker() );
    }

    DefaultIdentityModule( LogService logService, FileSystemAbstraction fs, Neo4jLayout layout, MemoryTracker memoryTracker )
    {
        var log = logService.getInternalLog( getClass() );
        var storage = createServerIdStorage( fs, layout.serverIdFile() );
        this.serverId = readOrGenerate( storage, log, ServerId.class, ServerId::new, UUID::randomUUID );
        logService.getUserLog( getClass() ).info( "This instance is %s (%s)", serverId, serverId.uuid() );
    }

    @Override
    public ServerId serverId()
    {
        return serverId;
    }
}
