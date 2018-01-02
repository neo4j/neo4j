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
package org.neo4j.kernel.impl.storemigration.legacystore.v20;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;

public class Legacy20NodeStoreReaderTest
{
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldReadNodeRecords() throws IOException
    {
        File storeDir = testDirectory.graphDbDir();
        MigrationTestUtils.find20FormatStoreDirectory( storeDir );
        Legacy20NodeStoreReader nodeStoreReader =
                new Legacy20NodeStoreReader( fs, new File( storeDir, "neostore.nodestore.db" ) );
        assertEquals( 1003, nodeStoreReader.getMaxId() );

        int nodeCount = 0;
        Iterator<NodeRecord> iterator = nodeStoreReader.iterator();
        while ( iterator.hasNext() )
        {
            NodeRecord record = iterator.next();
            if ( record.inUse() )
            {
                nodeCount++;
            }
        }
        assertEquals( 502, nodeCount );
        nodeStoreReader.close();
    }
}
