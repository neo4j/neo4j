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
package org.neo4j.kernel.lifecycle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class GitHub1304Test
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void givenBatchInserterWhenArrayPropertyUpdated4TimesThenShouldNotFail() throws IOException
    {
        BatchInserter batchInserter = BatchInserters.inserter( testDirectory.databaseLayout(), fileSystem );

        long nodeId = batchInserter.createNode( Collections.emptyMap() );

        for ( int i = 0; i < 4; i++ )
        {
            batchInserter.setNodeProperty( nodeId, "array", new byte[]{2, 3, 98, 1, 43, 50, 3, 33, 51, 55, 116, 16, 23,
                    56, 9, -10, 1, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1} );
        }

        batchInserter.getNodeProperties( nodeId );   //fails here
        batchInserter.shutdown();
    }
}
