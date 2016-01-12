/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.builder;

import java.io.File;
import java.util.Objects;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;

class LuceneIndexStorageBuilder
{
    private DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
    private FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
    private File indexRootFolder;
    private String indexIdentifier;
    private PartitionedIndexStorage indexStorage;

    private LuceneIndexStorageBuilder()
    {
    }

    static LuceneIndexStorageBuilder create()
    {
        return new LuceneIndexStorageBuilder();
    }

    PartitionedIndexStorage buildIndexStorage()
    {
        if ( indexStorage == null )
        {
            Objects.requireNonNull( directoryFactory );
            Objects.requireNonNull( fileSystem );
            Objects.requireNonNull( indexRootFolder );
            Objects.requireNonNull( indexIdentifier );
            indexStorage =
                    new PartitionedIndexStorage( directoryFactory, fileSystem, indexRootFolder, indexIdentifier );
        }
        return indexStorage;
    }

    LuceneIndexStorageBuilder withIndexIdentifier( String indexIdentifier )
    {
        this.indexIdentifier = indexIdentifier;
        return this;
    }

    LuceneIndexStorageBuilder withDirectoryFactory( DirectoryFactory directoryFactory )
    {
        this.directoryFactory = directoryFactory;
        return this;
    }

    LuceneIndexStorageBuilder withFileSystem( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
        return this;
    }

    LuceneIndexStorageBuilder withIndexRootFolder( File indexRootFolder )
    {
        this.indexRootFolder = indexRootFolder;
        return this;
    }

    LuceneIndexStorageBuilder withIndexStorage( PartitionedIndexStorage indexStorage )
    {
        this.indexStorage = indexStorage;
        return this;
    }
}
