/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerFailureException;
import org.neo4j.com.storecopy.StoreCopyServer;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.com.storecopy.ResponsePacker;
import org.neo4j.kernel.monitoring.BackupMonitor;
import org.neo4j.kernel.monitoring.Monitors;

class BackupImpl implements TheBackupInterface
{
    private final BackupMonitor backupMonitor;
    private final StoreCopyServer storeCopyServer;
    private final ResponsePacker responsePacker;

    public BackupImpl( StoreCopyServer storeCopyServer, ResponsePacker responsePacker,
            Monitors monitors )
    {
        this.storeCopyServer = storeCopyServer;
        this.responsePacker = responsePacker;
        this.backupMonitor = monitors.newMonitor( BackupMonitor.class, getClass() );
    }

    @Override
    public Response<Void> fullBackup( StoreWriter writer )
    {
        try ( StoreWriter storeWriter = writer )
        {
            backupMonitor.startCopyingFiles();
            RequestContext context = storeCopyServer.flushStoresAndStreamStoreFiles( storeWriter );
            return responsePacker.packResponse( context, null/*no response object*/ );
        }
        catch ( IOException e )
        {
            throw new ServerFailureException( e );
        }
    }

    @Override
    public Response<Void> incrementalBackup( RequestContext context )
    {
        try
        {
            return responsePacker.packResponse( context, null );
        }
        catch ( IOException e )
        {
            throw new ServerFailureException( e );
        }
    }
}
