/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCountsCommand;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.test.mockito.matcher.LogMatchers.checkPoint;
import static org.neo4j.test.mockito.matcher.LogMatchers.commandEntry;
import static org.neo4j.test.mockito.matcher.LogMatchers.commitEntry;
import static org.neo4j.test.mockito.matcher.LogMatchers.containsExactly;
import static org.neo4j.test.mockito.matcher.LogMatchers.logEntries;
import static org.neo4j.test.mockito.matcher.LogMatchers.startEntry;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class KernelRecoveryTest
{
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldHandleWritesProperlyAfterRecovery() throws Exception
    {
        // Given
        GraphDatabaseService db = newDB( fileSystem );

        long node1 = createNode( db );

        // And given the power goes out
        try ( EphemeralFileSystemAbstraction crashedFs = fileSystem.snapshot() )
        {
            db.shutdown();
            db = newDB( crashedFs );
            LogFiles logFiles = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( LogFiles.class );
            File logFile = logFiles.getHighestLogFile();
            long node2 = createNode( db );
            db.shutdown();

            // Then the logical log should be in sync
            assertThat( logEntries( crashedFs, logFile ), containsExactly(
                    // Tx before recovery
                    startEntry( -1, -1 ), commandEntry( node1, NodeCommand.class ),
                    commandEntry( StatementConstants.ANY_LABEL, NodeCountsCommand.class ), commitEntry( 2 ),

                    // checkpoint after recovery
                    checkPoint( new LogPosition( 0, 133 ) ),

                    // Tx after recovery
                    startEntry( -1, -1 ), commandEntry( node2, NodeCommand.class ),
                    commandEntry( StatementConstants.ANY_LABEL, NodeCountsCommand.class ), commitEntry( 3 ),

                    // checkpoint
                    checkPoint( new LogPosition( 0, 268 ) ) ) );
        }
    }

    private GraphDatabaseService newDB( FileSystemAbstraction fs )
    {
        return new TestGraphDatabaseFactory()
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                .newImpermanentDatabase( testDirectory.databaseDir() );
    }

    private static long createNode( GraphDatabaseService db )
    {
        long node1;
        try ( Transaction tx = db.beginTx() )
        {
            node1 = db.createNode().getId();
            tx.success();
        }
        return node1;
    }
}
