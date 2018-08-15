/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.io.fs.FileUtils.relativePath;

public class PrepareStoreCopyFiles implements AutoCloseable
{
    private final NeoStoreDataSource neoStoreDataSource;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final CloseablesListener closeablesListener = new CloseablesListener();

    PrepareStoreCopyFiles( NeoStoreDataSource neoStoreDataSource, FileSystemAbstraction fileSystemAbstraction )
    {
        this.neoStoreDataSource = neoStoreDataSource;
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    LongSet getNonAtomicIndexIds()
    {
        return LongSets.immutable.empty();
    }

    StoreResource[] getAtomicFilesSnapshot() throws IOException
    {
        ResourceIterator<StoreFileMetadata> neoStoreFilesIterator =
                closeablesListener.add( neoStoreDataSource.getNeoStoreFileListing().builder().excludeAll().includeNeoStoreFiles().build() );
        ResourceIterator<StoreFileMetadata> indexIterator = closeablesListener.add( neoStoreDataSource
                .getNeoStoreFileListing()
                .builder()
                .excludeAll()
                .includeExplicitIndexStoreStoreFiles()
                .includeAdditionalProviders()
                .includeLabelScanStoreFiles()
                .includeSchemaIndexStoreFiles()
                .build() );

        return Stream.concat( neoStoreFilesIterator.stream().filter( isCountFile( neoStoreDataSource.getDatabaseLayout() ) ), indexIterator.stream() ).map(
                mapToStoreResource() ).toArray( StoreResource[]::new );
    }

    private Function<StoreFileMetadata,StoreResource> mapToStoreResource()
    {
        return storeFileMetadata ->
        {
            try
            {
                return toStoreResource( storeFileMetadata );
            }
            catch ( IOException e )
            {
                throw new IllegalStateException( "Unable to create store resource", e );
            }
        };
    }

    File[] listReplayableFiles() throws IOException
    {
        try ( Stream<StoreFileMetadata> stream = neoStoreDataSource.getNeoStoreFileListing().builder().excludeLogFiles()
                .excludeExplicitIndexStoreFiles().excludeSchemaIndexStoreFiles().excludeAdditionalProviders().build().stream() )
        {
            return stream.filter( isCountFile( neoStoreDataSource.getDatabaseLayout() ).negate() ).map( StoreFileMetadata::file ).toArray( File[]::new );
        }
    }

    private static Predicate<StoreFileMetadata> isCountFile( DatabaseLayout databaseLayout )
    {
        return storeFileMetadata -> databaseLayout.countStoreB().equals( storeFileMetadata.file() ) ||
                databaseLayout.countStoreA().equals( storeFileMetadata.file() );
    }

    private StoreResource toStoreResource( StoreFileMetadata storeFileMetadata ) throws IOException
    {
        File databaseDirectory = neoStoreDataSource.getDatabaseLayout().databaseDirectory();
        File file = storeFileMetadata.file();
        String relativePath = relativePath( databaseDirectory, file );
        return new StoreResource( file, relativePath, storeFileMetadata.recordSize(), fileSystemAbstraction );
    }

    @Override
    public void close()
    {
        closeablesListener.close();
    }
}
