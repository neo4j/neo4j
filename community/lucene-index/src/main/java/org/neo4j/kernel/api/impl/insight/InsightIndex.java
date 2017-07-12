package org.neo4j.kernel.api.impl.insight;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.builder.LuceneIndexStorageBuilder;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;

import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;

public class InsightIndex
{
    private final InsightLuceneIndex nodeIndex;

    public InsightIndex( FileSystemAbstraction fileSystem, int[] properties ) throws IOException
    {
        LuceneIndexStorageBuilder storageBuilder = LuceneIndexStorageBuilder.create();
        storageBuilder.withFileSystem( fileSystem ).withIndexIdentifier( "insightNodes" )
                .withDirectoryFactory( directoryFactory( false, fileSystem ) )
                .withIndexRootFolder( Paths.get( "insightindex" ).toFile() );

        WritableIndexPartitionFactory partitionFactory = new WritableIndexPartitionFactory(
                IndexWriterConfigs::population );
        String[] propertyIdsAsStrings = Arrays.stream( properties ).sorted().mapToObj( String::valueOf )
                .toArray( String[]::new );
        nodeIndex = new InsightLuceneIndex( storageBuilder.build(), partitionFactory, propertyIdsAsStrings );
        nodeIndex.open();
    }

    public InsightIndexTransactionEventUpdater getUpdater() throws IOException
    {
        WritableDatabaseInsightIndex writableDatabaseInsightIndex = new WritableDatabaseInsightIndex( nodeIndex );
        return new InsightIndexTransactionEventUpdater( writableDatabaseInsightIndex.getIndexWriter() );
    }

    public InsightIndexReader getReader() throws IOException
    {
        return nodeIndex.getIndexReader();
    }
}
