/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.batchinsert.internal;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.batchimport.LogFilesInitializer;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.Lifespan;

public class TransactionLogsInitializer implements LogFilesInitializer
{
    public static final LogFilesInitializer INSTANCE = new TransactionLogsInitializer();

    private TransactionLogsInitializer()
    {
    }

    @Override
    public void initializeLogFiles( Config config, DatabaseLayout databaseLayout, NeoStores neoStores, FileSystemAbstraction fileSystem )
    {
        try
        {
            final LogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
            LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fileSystem )
                    .withLogEntryReader( logEntryReader )
                    .withTransactionIdStore( neoStores.getMetaDataStore() )
                    .withLogVersionRepository( neoStores.getMetaDataStore() )
                    .withConfig( config ).build();
            new Lifespan( logFiles ).close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Fail to create empty transaction log file.", e );
        }
    }
}
