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

        assertEquals( indexRoot, indexFolder.getParentFile() );
        assertEquals( "testIndex", indexFolder.getName() );
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
        return new IndexFolderLayout( indexRoot, "testIndex" );
    }

}