/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.StoreWriter;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.GraphDatabaseAPI;

class BackupImpl implements TheBackupInterface
{
    private final GraphDatabaseAPI graphDb;

    public BackupImpl( GraphDatabaseAPI graphDb )
    {
        this.graphDb = graphDb;
    }
    
    public Response<Void> fullBackup( StoreWriter writer )
    {
        RequestContext context = ServerUtil.rotateLogsAndStreamStoreFiles( graphDb, false, writer );
        writer.done();
        return packResponse( context );
    }
    
    public Response<Void> incrementalBackup( RequestContext context )
    {
        return packResponse( context );
    }
    
    private Response<Void> packResponse( RequestContext context )
    {
        // On Windows there's a problem extracting logs from the current log version
        // where a rotation is requested during the time of extracting transactions
        // from it, especially if the extraction process is waiting for the client
        // to catch up on reading them. On Linux/Mac this isn't a due to a more flexible
        // file handling system. Solution: rotate before doing an incremental backup
        // in Windows to avoid running into that problem.
        if ( Settings.osIsWindows() )
        {
            ServerUtil.rotateLogs( graphDb );
        }
        return ServerUtil.packResponse( graphDb, context, null, ServerUtil.ALL );
    }
}
