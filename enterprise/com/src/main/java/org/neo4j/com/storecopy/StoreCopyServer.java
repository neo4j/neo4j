/**
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
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerFailureException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.LogRotationControl;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.neo4j.com.RequestContext.anonymous;
import static org.neo4j.io.fs.FileUtils.getMostCanonicalFile;
import static org.neo4j.io.fs.FileUtils.relativePath;

/**
 * Is able to feed store files in a consistent way to a {@link Response} to be picked up by a
 * {@link StoreCopyClient}, for example.
 *
 * @see StoreCopyClient
 */
public class StoreCopyServer
{
    private final TransactionIdStore transactionIdStore;
    private final NeoStoreDataSource dataSource;
    private final LogRotationControl logRotationControl;
    private final FileSystemAbstraction fileSystem;
    private final File storeDirectory;

    public StoreCopyServer( TransactionIdStore transactionIdStore,
            NeoStoreDataSource dataSource, LogRotationControl logRotationControl, FileSystemAbstraction fileSystem,
            File
            storeDirectory )
    {
        this.transactionIdStore = transactionIdStore;
        this.dataSource = dataSource;
        this.logRotationControl = logRotationControl;
        this.fileSystem = fileSystem;
        this.storeDirectory = getMostCanonicalFile( storeDirectory );
    }

    /**
     * @return a {@link RequestContext} specifying at which point the store copy started.
     */
    public RequestContext flushStoresAndStreamStoreFiles( StoreWriter writer )
    {
        try
        {
            long transactionIdWhenStartingCopy = transactionIdStore.getLastCommittedTransactionId();
            logRotationControl.forceEverything();
            ByteBuffer temporaryBuffer = ByteBuffer.allocateDirect( 1024 * 1024 );

            // Copy the store files
            try ( ResourceIterator<File> files = dataSource.listStoreFiles() )
            {
                while ( files.hasNext() )
                {
                    File file = files.next();
                    try ( StoreChannel fileChannel = fileSystem.open( file, "r" ) )
                    {
                        writer.write( relativePath( storeDirectory, file ), fileChannel,
                                temporaryBuffer, file.length() > 0 );
                    }
                }
            }

            return anonymous( transactionIdWhenStartingCopy );
        }
        catch ( IOException e )
        {
            throw new ServerFailureException( e );
        }
    }
}
