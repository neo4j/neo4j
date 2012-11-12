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

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.ENABLE_ONLINE_BACKUP;

import org.neo4j.kernel.EmbeddedGraphDatabase;

public class EmbeddedServer implements ServerInterface
{
    private EmbeddedGraphDatabase db;

    public EmbeddedServer( String storeDir, String backupConfigValue )
    {
        if ( backupConfigValue == null )
        {
            this.db = new EmbeddedGraphDatabase( storeDir );
        }
        else
        {
            this.db = new EmbeddedGraphDatabase( storeDir, stringMap( ENABLE_ONLINE_BACKUP, backupConfigValue ) );
        }
    }
    
    public void shutdown()
    {
        db.shutdown();
    }

    public void awaitStarted()
    {
    }
}
