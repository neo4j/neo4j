/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.storage.layout;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class IndexFolderLayoutTest
{
    private final File indexRoot = new File( "indexRoot" );

    @Test
    public void testIndexFolder()
    {
        IndexFolderLayout indexLayout = createTestIndex();
        File indexFolder = indexLayout.getIndexFolder();

        assertEquals( indexRoot, indexFolder );
    }

    @Test
    public void testIndexPartitionFolder()
    {
        IndexFolderLayout indexLayout = createTestIndex();

        File indexFolder = indexLayout.getIndexFolder();
        File partitionFolder1 = indexLayout.getPartitionFolder( 1 );
        File partitionFolder3 = indexLayout.getPartitionFolder( 3 );

        assertEquals( partitionFolder1.getParentFile(), partitionFolder3.getParentFile() );
        assertEquals( indexFolder, partitionFolder1.getParentFile() );
        assertEquals( "1", partitionFolder1.getName() );
        assertEquals( "3", partitionFolder3.getName() );
    }

    private IndexFolderLayout createTestIndex()
    {
        return new IndexFolderLayout( indexRoot );
    }
}
