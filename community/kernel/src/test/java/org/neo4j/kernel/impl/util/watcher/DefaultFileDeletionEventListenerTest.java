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

import org.junit.Test;

import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

public class DefaultFileDeletionEventListenerTest
{
    @Test
    public void notificationInLogAboutFileDeletion()
    {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider( false );
        DefaultFileDeletionEventListener listener = buildListener( internalLogProvider );
        listener.fileDeleted( "testFile.db" );
        listener.fileDeleted( "anotherDirectory" );

        internalLogProvider.assertContainsMessageContaining(
                "'testFile.db' which belongs to the store was deleted while database was running." );
        internalLogProvider.assertContainsMessageContaining(
                "'anotherDirectory' which belongs to the store was deleted while database was running." );
    }

    @Test
    public void noNotificationForTransactionLogs()
    {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider( false );
        DefaultFileDeletionEventListener listener = buildListener( internalLogProvider );
        listener.fileDeleted( TransactionLogFiles.DEFAULT_NAME + ".0" );
        listener.fileDeleted( TransactionLogFiles.DEFAULT_NAME + ".1" );

        internalLogProvider.assertNoLoggingOccurred();
    }

    private DefaultFileDeletionEventListener buildListener( AssertableLogProvider internalLogProvider )
    {
        SimpleLogService logService = new SimpleLogService( NullLogProvider.getInstance(), internalLogProvider );
        return new DefaultFileDeletionEventListener( logService,
                filename -> filename.startsWith( TransactionLogFiles.DEFAULT_NAME ) );
    }
}
