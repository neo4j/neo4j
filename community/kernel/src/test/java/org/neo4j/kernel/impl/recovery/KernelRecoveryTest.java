/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.recovery;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCountsCommand;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.kernel.impl.transaction.LogMatchers.checkPoint;
import static org.neo4j.kernel.impl.transaction.LogMatchers.commandEntry;
import static org.neo4j.kernel.impl.transaction.LogMatchers.commitEntry;
import static org.neo4j.kernel.impl.transaction.LogMatchers.containsExactly;
import static org.neo4j.kernel.impl.transaction.LogMatchers.logEntries;
import static org.neo4j.kernel.impl.transaction.LogMatchers.startEntry;

public class KernelRecoveryTest
{
    @Rule public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final File storeDir = new File( "dir" ).getAbsoluteFile();

    @Test
    public void shouldHandleWritesProperlyAfterRecovery() throws Exception
    {
        // Given
        EphemeralFileSystemAbstraction fs = fsRule.get();
        GraphDatabaseService db = newDB( fs );

        long node1 = createNode( db );

        // And given the power goes out
        EphemeralFileSystemAbstraction crashedFs = fs.snapshot();
        db.shutdown();
        db = newDB( crashedFs );

        long node2 = createNode( db );
        db.shutdown();

        // Then the logical log should be in sync
        File logFile = new File( storeDir,
                PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "0" );
        assertThat(
                logEntries( crashedFs, logFile ),
                containsExactly(
                    // Tx before recovery
                    startEntry( -1, -1 ),
                    commandEntry( node1, NodeCommand.class ),
                    commandEntry( ReadOperations.ANY_LABEL, NodeCountsCommand.class ),
                    commitEntry( 2 ),

                    // Tx after recovery
                    startEntry( -1, -1 ),
                    commandEntry( node2, NodeCommand.class ),
                    commandEntry( ReadOperations.ANY_LABEL, NodeCountsCommand.class ),
                    commitEntry( 3 ),

                    // checkpoint
                    checkPoint( new LogPosition(0, 250) )
                )
        );
    }

    private GraphDatabaseService newDB( EphemeralFileSystemAbstraction fs )
    {
        fs.mkdirs( storeDir );
        return new TestGraphDatabaseFactory()
                    .setFileSystem( fs )
                    .newImpermanentDatabase( storeDir );
    }

    private long createNode( GraphDatabaseService db )
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
