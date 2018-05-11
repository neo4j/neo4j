/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.io.fs.FileUtils.relativePath;

public class PrepareStoreCopyFiles implements AutoCloseable
{
    private final NeoStoreDataSource neoStoreDataSource;
    private final PageCache pageCache;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final CloseablesListener closeablesListener = new CloseablesListener();

    PrepareStoreCopyFiles( NeoStoreDataSource neoStoreDataSource, PageCache pageCache, FileSystemAbstraction fileSystemAbstraction )
    {
        this.neoStoreDataSource = neoStoreDataSource;
        this.pageCache = pageCache;
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    PrimitiveLongSet getNonAtomicIndexIds()
    {
        return Primitive.longSet();
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

        return Stream.concat( neoStoreFilesIterator.stream().filter( isCountFile() ), indexIterator.stream() ).map( mapToStoreResource() ).toArray(
                StoreResource[]::new );
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
            return stream.filter( isCountFile().negate() ).map( StoreFileMetadata::file ).toArray( File[]::new );
        }
    }

    private static Predicate<StoreFileMetadata> isCountFile()
    {
        return storeFileMetadata -> StoreType.typeOf( storeFileMetadata.file().getName() ).filter( f -> f == StoreType.COUNTS ).isPresent();
    }

    private StoreResource toStoreResource( StoreFileMetadata storeFileMetadata ) throws IOException
    {
        File storeDir = neoStoreDataSource.getStoreDir();
        File file = storeFileMetadata.file();
        String relativePath = relativePath( storeDir, file );
        return new StoreResource( file, relativePath, storeFileMetadata.recordSize(), pageCache, fileSystemAbstraction );
    }

    @Override
    public void close()
    {
        closeablesListener.close();
    }
}
