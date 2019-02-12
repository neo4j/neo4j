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
package org.neo4j.storageengine.api;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.DependencySatisfier;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

/**
 * A factory suitable for something like service-loading to load {@link StorageEngine} instances.
 * Also migration logic is provided by this factory.
 */
public interface StorageEngineFactory
{
    /**
     * Returns a {@link StoreVersionCheck} which can provide both configured and existing store versions
     * and means of checking upgradability between them.
     * @param dependencyResolver {@link DependencyResolver} for all dependency needs.
     * @return StoreVersionCheck to check store version as well as upgradability to other versions.
     */
    StoreVersionCheck versionCheck( DependencyResolver dependencyResolver );

    StoreVersion versionInformation( String storeVersion );

    /**
     * Returns a {@link StoreMigrationParticipant} which will be able to participate in a store migration.
     * @param dependencyResolver {@link DependencyResolver} for all dependency needs.
     * @return StoreMigrationParticipant for migration.
     */
    StoreMigrationParticipant migrationParticipant( DependencyResolver dependencyResolver );

    /**
     * Instantiates a {@link StorageEngine} where all dependencies can be retrieved from the supplied {@code dependencyResolver}.
     *
     * @param dependencyResolver {@link DependencyResolver} used to get all required dependencies to instantiate the {@link StorageEngine}.
     * @param dependencySatisfier {@link DependencySatisfier} providing ways to let the storage engine provide dependencies
     * back to the instantiator. This is a hack with the goal to be removed completely when graph storage abstraction in kernel is properly in place.
     * @return the instantiated {@link StorageEngine}.
     */
    StorageEngine instantiate( DependencyResolver dependencyResolver, DependencySatisfier dependencySatisfier );

    /**
     * Lists files of a specific storage location.
     * @param fileSystem {@link FileSystemAbstraction} this storage is on.
     * @param databaseLayout {@link DatabaseLayout} pointing out its location.
     * @return a {@link Stream} of {@link File} instances for the storage files.
     * @throws IOException if there was no storage in this location.
     */
    List<File> listStorageFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException;

    boolean storageExists( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout );

    /**
     * Instantiates a read-only {@link TransactionIdStore} to be used outside of a {@link StorageEngine}.
     * @param dependencyResolver resolver for all dependencies required to instantiate the {@link TransactionIdStore}.
     * @return the read-only {@link TransactionIdStore}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    TransactionIdStore readOnlyTransactionIdStore( DependencyResolver dependencyResolver ) throws IOException;

    /**
     * Instantiates a read-only {@link LogVersionRepository} to be used outside of a {@link StorageEngine}.
     * @param dependencyResolver resolver for all dependencies required to instantiate the {@link LogVersionRepository}.
     * @return the read-only {@link LogVersionRepository}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    LogVersionRepository readOnlyLogVersionRepository( DependencyResolver dependencyResolver ) throws IOException;

    /**
     * Instantiates a {@link ReadableStorageEngine} over a storage location without instantiating the full {@link StorageEngine}, just the readable parts.
     * @param dependencyResolver {@link DependencyResolver} for all dependency needs.
     * @return StorageReader for reading the storage at this location. Must be closed after usage.
     */
    ReadableStorageEngine instantiateReadable( DependencyResolver dependencyResolver );

    /**
     * Instantiates a fully functional {@link TransactionMetaDataStore}, which is a union of {@link TransactionIdStore}
     * and {@link LogVersionRepository}.
     * @param dependencyResolver resolver for all dependencies required to instantiate the {@link TransactionMetaDataStore}.
     * @return a fully functional {@link TransactionMetaDataStore}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    TransactionMetaDataStore transactionMetaDataStore( DependencyResolver dependencyResolver ) throws IOException;

    StoreId storeId( DependencyResolver dependencyResolver ) throws IOException;

    /**
     * Selects a {@link StorageEngineFactory} among the candidates. How it's done or which it selects isn't important a.t.m.
     * @param candidates list of {@link StorageEngineFactory} to compare.
     * @return the selected {@link StorageEngineFactory}.
     * @throws IllegalStateException if there were no candidates.
     */
    static StorageEngineFactory selectStorageEngine( Iterable<StorageEngineFactory> candidates )
    {
        return Iterables.single( candidates );
    }
}
