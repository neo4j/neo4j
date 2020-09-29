/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.recovery;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointInfo;
import org.neo4j.storageengine.api.StorageEngineFactory;

public final class RecoveryHelpers
{
     private RecoveryHelpers()
     { // non-constructable
     }

     public static void removeLastCheckpointRecordFromLastLogFile( DatabaseLayout dbLayout, FileSystemAbstraction fs ) throws IOException
     {
          LogFiles logFiles = buildLogFiles( dbLayout, fs );
          var checkpointFile = logFiles.getCheckpointFile();
          Optional<CheckpointInfo> latestCheckpoint = checkpointFile.findLatestCheckpoint();
          latestCheckpoint.ifPresent( checkpointInfo ->
              {
              LogPosition entryPosition = checkpointInfo.getCheckpointEntryPosition();
              try ( StoreChannel storeChannel = fs.write( checkpointFile.getCurrentFile() ) )
              {
                  storeChannel.truncate( entryPosition.getByteOffset() );
              }
              catch ( IOException e )
              {
                  throw new UncheckedIOException( e );
              }
          } );
     }

     private static LogFiles buildLogFiles( DatabaseLayout dbLayout, FileSystemAbstraction fs ) throws IOException
     {
          return LogFilesBuilder
                  .logFilesBasedOnlyBuilder( dbLayout.getTransactionLogsDirectory(), fs )
                  .withCommandReaderFactory( StorageEngineFactory.selectStorageEngine().commandReaderFactory() )
                  .build();
     }
}
