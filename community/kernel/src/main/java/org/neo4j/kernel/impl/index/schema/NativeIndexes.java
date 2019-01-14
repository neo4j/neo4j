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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;

import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_POPULATING;

public class NativeIndexes
{
    private NativeIndexes()
    {}

    public static InternalIndexState readState( PageCache pageCache, File indexFile ) throws IOException
    {
        NativeIndexHeaderReader headerReader = new NativeIndexHeaderReader( NO_HEADER_READER );
        GBPTree.readHeader( pageCache, indexFile, headerReader );
        switch ( headerReader.state )
        {
        case BYTE_FAILED:
            return InternalIndexState.FAILED;
        case BYTE_ONLINE:
            return InternalIndexState.ONLINE;
        case BYTE_POPULATING:
            return InternalIndexState.POPULATING;
        default:
            throw new IllegalStateException( "Unexpected initial state byte value " + headerReader.state );
        }
    }

    static String readFailureMessage( PageCache pageCache, File indexFile )
            throws IOException
    {
        NativeIndexHeaderReader headerReader = new NativeIndexHeaderReader( NO_HEADER_READER );
        GBPTree.readHeader( pageCache, indexFile, headerReader );
        return headerReader.failureMessage;
    }

    /**
     * Deletes index folder with the specific indexId, but has the option to first archive the index if it exists.
     * The zip archive will be placed next to the root directory for that index with a timestamp included in its name.
     *
     * @param fs {@link FileSystemAbstraction} this index lives in.
     * @param directoryStructure {@link IndexDirectoryStructure} knowing the directory structure for the provider owning the index.
     * @param indexId id of the index.
     * @param archiveIfExists whether or not to archive the index before deleting it, if it exists.
     * @return whether or not an archive was created.
     * @throws IOException on I/O error.
     */
    public static boolean deleteIndex( FileSystemAbstraction fs, IndexDirectoryStructure directoryStructure, long indexId, boolean archiveIfExists )
            throws IOException
    {
        File rootIndexDirectory = directoryStructure.directoryForIndex( indexId );
        if ( archiveIfExists && fs.isDirectory( rootIndexDirectory ) && fs.fileExists( rootIndexDirectory ) && fs.listFiles( rootIndexDirectory ).length > 0 )
        {
            ZipUtils.zip( fs, rootIndexDirectory,
                    new File( rootIndexDirectory.getParent(), "archive-" + rootIndexDirectory.getName() + "-" + System.currentTimeMillis() + ".zip" ) );
            return true;
        }
        fs.deleteRecursively( rootIndexDirectory );
        return false;
    }
}
