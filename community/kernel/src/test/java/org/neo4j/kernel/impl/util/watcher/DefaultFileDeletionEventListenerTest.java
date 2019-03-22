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
package org.neo4j.kernel.impl.util.watcher;

import org.junit.jupiter.api.Test;

import java.nio.file.WatchKey;

import org.neo4j.io.fs.watcher.resource.WatchedFile;
import org.neo4j.io.fs.watcher.resource.WatchedResource;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;

import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterators.asSet;

class DefaultFileDeletionEventListenerTest
{
    private final WatchKey key = mock( WatchKey.class );
    private final WatchedResource watchedResource = new WatchedFile( key );

    @Test
    void notificationInLogAboutFileDeletion()
    {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider( false );
        DefaultFileDeletionEventListener listener = buildListener( internalLogProvider );
        listener.fileDeleted( key, "testFile" );
        listener.fileDeleted( key, "anotherDirectory" );

        internalLogProvider.assertLogStringContains(
                "'testFile' which belongs to the 'testDatabase' database was deleted while it was running." );
        internalLogProvider.assertLogStringContains(
                "'anotherDirectory' which belongs to the 'testDatabase' database was deleted while it was running." );
    }

    @Test
    void noNotificationForTransactionLogs()
    {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider( false );
        DefaultFileDeletionEventListener listener = buildListener( internalLogProvider );
        listener.fileDeleted( key, TransactionLogFilesHelper.DEFAULT_NAME + ".0" );
        listener.fileDeleted( key, TransactionLogFilesHelper.DEFAULT_NAME + ".1" );

        internalLogProvider.assertNoLoggingOccurred();
    }

    private DefaultFileDeletionEventListener buildListener( AssertableLogProvider internalLogProvider )
    {
        SimpleLogService logService = new SimpleLogService( NullLogProvider.getInstance(), internalLogProvider );
        return new DefaultFileDeletionEventListener( new DatabaseId( "testDatabase" ), asSet( watchedResource ), logService,
                filename -> filename.startsWith( TransactionLogFilesHelper.DEFAULT_NAME ) );
    }
}
