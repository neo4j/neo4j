/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index.builder;

import java.nio.file.Path;
import java.util.Objects;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;

/**
 * Base class for lucene index builders.
 *
 * @param <T> actual index type
 */
public abstract class AbstractLuceneIndexBuilder<T extends AbstractLuceneIndexBuilder<T>> {
    protected LuceneIndexStorageBuilder storageBuilder = LuceneIndexStorageBuilder.create();
    protected final DatabaseReadOnlyChecker readOnlyChecker;
    protected boolean permanentlyReadOnly;

    public AbstractLuceneIndexBuilder(DatabaseReadOnlyChecker readOnlyChecker) {
        this.readOnlyChecker = Objects.requireNonNull(readOnlyChecker);
    }

    /**
     * Specify index storage
     *
     * @param indexStorage index storage
     * @return index builder
     */
    public T withIndexStorage(PartitionedIndexStorage indexStorage) {
        storageBuilder.withIndexStorage(indexStorage);
        return (T) this;
    }

    /**
     * Specify directory factory
     *
     * @param directoryFactory directory factory
     * @return index builder
     */
    public T withDirectoryFactory(DirectoryFactory directoryFactory) {
        storageBuilder.withDirectoryFactory(directoryFactory);
        return (T) this;
    }

    /**
     * Specify file system abstraction
     *
     * @param fileSystem file system abstraction
     * @return index builder
     */
    public T withFileSystem(FileSystemAbstraction fileSystem) {
        storageBuilder.withFileSystem(fileSystem);
        return (T) this;
    }

    /**
     * Specify index root folder
     *
     * @param indexRootFolder root folder
     * @return index builder
     */
    public T withIndexRootFolder(Path indexRootFolder) {
        storageBuilder.withIndexFolder(indexRootFolder);
        return (T) this;
    }

    public T permanentlyReadOnly() {
        this.permanentlyReadOnly = true;
        return (T) this;
    }
}
