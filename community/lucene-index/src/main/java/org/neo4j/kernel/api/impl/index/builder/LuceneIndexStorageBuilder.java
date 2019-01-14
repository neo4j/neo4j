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
package org.neo4j.kernel.api.impl.index.builder;

import java.io.File;
import java.util.Objects;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;

/**
 * Helper builder class to simplify construction of lucene index storages.
 * Most of the values already have most useful default value, that still can be overridden by corresponding
 * builder methods.
 */
public class LuceneIndexStorageBuilder
{
    private DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
    private FileSystemAbstraction fileSystem;
    private File indexRootFolder;
    private PartitionedIndexStorage indexStorage;

    private LuceneIndexStorageBuilder()
    {
    }

    /**
     * Create new lucene index storage builder
     *
     * @return index builder
     */
    public static LuceneIndexStorageBuilder create()
    {
        return new LuceneIndexStorageBuilder();
    }

    /**
     * Build lucene index storage with specified configuration
     *
     * @return lucene index storage
     */
    public PartitionedIndexStorage build()
    {
        if ( indexStorage == null )
        {
            Objects.requireNonNull( directoryFactory );
            Objects.requireNonNull( fileSystem );
            Objects.requireNonNull( indexRootFolder );
            indexStorage = new PartitionedIndexStorage( directoryFactory, fileSystem, indexRootFolder );
        }
        return indexStorage;
    }

    /**
     * Specify directory factory
     *
     * @param directoryFactory directory factory
     * @return index storage builder
     */
    public LuceneIndexStorageBuilder withDirectoryFactory( DirectoryFactory directoryFactory )
    {
        this.directoryFactory = directoryFactory;
        return this;
    }

    /**
     * Specify file system abstraction
     *
     * @param fileSystem file system abstraction
     * @return index storage builder
     */
    public LuceneIndexStorageBuilder withFileSystem( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
        return this;
    }

    /**
     * Specify index root folder
     *
     * @param indexRootFolder root folder
     * @return index storage builder
     */
    public LuceneIndexStorageBuilder withIndexFolder( File indexRootFolder )
    {
        this.indexRootFolder = indexRootFolder;
        return this;
    }

    /**
     * Specify partitioned index storage
     *
     * @param indexStorage index storage
     * @return index storage builder
     */
    public LuceneIndexStorageBuilder withIndexStorage( PartitionedIndexStorage indexStorage )
    {
        this.indexStorage = indexStorage;
        return this;
    }
}
