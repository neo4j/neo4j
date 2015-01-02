/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.legacystore;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;

import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class ReadRecordsTestIT
{
    @Test
    public void shouldReadNodeRecords() throws IOException
    {
        File file = exampleDbStore( "neostore.nodestore.db" );

        try ( LegacyNodeStoreReader nodeStoreReader = new LegacyNodeStoreReader( fs, file ) )
        {
            assertEquals( 1002, nodeStoreReader.getMaxId() );
            Iterator<NodeRecord> records = nodeStoreReader.readNodeStore();
            int nodeCount = 0;
            for ( NodeRecord record : loop( records ) )
            {
                if ( record.inUse() )
                    nodeCount++;
            }
            assertEquals( 501, nodeCount );
        }
    }

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    private File exampleDbStore( String fileName ) throws IOException
    {
        return new File( exampleDbStore(), fileName );
    }

    private File exampleDbStore() throws IOException
    {
        return MigrationTestUtils.findOldFormatStoreDirectory();
    }
}
