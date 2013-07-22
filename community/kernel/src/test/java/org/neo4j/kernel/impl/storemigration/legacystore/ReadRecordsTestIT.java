/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.net.URL;
import java.util.Iterator;

import org.junit.Test;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

public class ReadRecordsTestIT
{
    @Test
    public void shouldReadNodeRecords() throws IOException
    {
        URL nodeStoreFile = getClass().getResource( "exampledb/neostore.nodestore.db" );

        LegacyNodeStoreReader nodeStoreReader = new LegacyNodeStoreReader( fs, new File( nodeStoreFile.getFile() ) );
        assertEquals( 1002, nodeStoreReader.getMaxId() );
        Iterator<NodeRecord> records = nodeStoreReader.readNodeStore();
        int nodeCount = 0;
        for ( NodeRecord record : loop( records ) )
        {
            if ( record.inUse() )
                nodeCount++;
        }
        assertEquals( 501, nodeCount );
        nodeStoreReader.close();
    }
    
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
}
