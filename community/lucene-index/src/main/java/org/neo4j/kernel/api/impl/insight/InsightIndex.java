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
package org.neo4j.kernel.api.impl.insight;

import java.io.File;
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

    public InsightIndex( FileSystemAbstraction fileSystem, File file, int[] properties ) throws IOException
    {
        LuceneIndexStorageBuilder storageBuilder = LuceneIndexStorageBuilder.create();
        storageBuilder.withFileSystem( fileSystem ).withIndexIdentifier( "insightNodes" )
                .withDirectoryFactory( directoryFactory( false, fileSystem ) )
                .withIndexRootFolder( Paths.get( file.getAbsolutePath(),"insightindex" ).toFile() );

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
        return new InsightIndexTransactionEventUpdater( writableDatabaseInsightIndex );
    }

    public InsightIndexReader getReader() throws IOException
    {
        return nodeIndex.getIndexReader();
    }
}
