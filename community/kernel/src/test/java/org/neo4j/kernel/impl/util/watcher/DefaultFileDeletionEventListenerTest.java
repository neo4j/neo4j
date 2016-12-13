/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

public class DefaultFileDeletionEventListenerTest
{

    @Test
    public void notificationInLogAboutFileDeletion() throws Exception
    {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider( false );
        SimpleLogService logService = new SimpleLogService( NullLogProvider.getInstance(), internalLogProvider );
        DefaultFileDeletionEventListener listener = new DefaultFileDeletionEventListener( logService );
        listener.fileDeleted( "testFile" );
        listener.fileDeleted( "anotherFile" );

        internalLogProvider.assertContainsMessageContaining( "Store file 'testFile' was " +
                "deleted while database was online." );
        internalLogProvider.assertContainsMessageContaining( "Store file 'anotherFile' was " +
                "deleted while database was online." );
    }
}
