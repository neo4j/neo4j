/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v22.Legacy22Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v23.Legacy23Store;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.recovery.LatestCheckPointFinder;
import org.neo4j.kernel.recovery.LatestCheckPointFinder.LatestCheckPoint;

/**
 * Logic to check whether a database version is upgradable to the current version. It looks at the
 * version information found in the store files themselves.
 */
public class UpgradableDatabase
{
    private final FileSystemAbstraction fs;
    private final StoreVersionCheck storeVersionCheck;
    private final LegacyStoreVersionCheck legacyStoreVersionCheck;

    public UpgradableDatabase( FileSystemAbstraction fs,
            StoreVersionCheck storeVersionCheck, LegacyStoreVersionCheck legacyStoreVersionCheck )
    {
        this.fs = fs;
        this.storeVersionCheck = storeVersionCheck;
        this.legacyStoreVersionCheck = legacyStoreVersionCheck;
    }

    boolean storeFilesUpgradeable( File storeDirectory )
    {
        try
        {
            checkUpgradeable( storeDirectory );
            return true;
        }
        catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            return false;
        }
    }

    public String checkUpgradeable( File storeDirectory )
    {
        Result result = checkUpgradeableFor( storeDirectory, Legacy19Store.LEGACY_VERSION );
        if ( result.outcome.isSuccessful() )
        {
            return Legacy19Store.LEGACY_VERSION;
        }

        result = checkUpgradeableFor( storeDirectory, Legacy20Store.LEGACY_VERSION );
        if ( result.outcome.isSuccessful() )
        {
            return Legacy20Store.LEGACY_VERSION;
        }

        result = checkUpgradeableFor( storeDirectory, Legacy21Store.LEGACY_VERSION );
        if ( result.outcome.isSuccessful() )
        {
            return Legacy21Store.LEGACY_VERSION;
        }

        result = checkUpgradeableFor( storeDirectory, Legacy22Store.LEGACY_VERSION );
        if ( result.outcome.isSuccessful() )
        {
            return Legacy22Store.LEGACY_VERSION;
        }

        result = checkUpgradeableFor( storeDirectory, Legacy23Store.LEGACY_VERSION );
        if ( result.outcome.isSuccessful() )
        {
            return Legacy23Store.LEGACY_VERSION;
        }

        // report error
        switch ( result.outcome )
        {
        case missingStoreFile:
            throw new StoreUpgrader.UpgradeMissingStoreFilesException( getPathToStoreFile( storeDirectory, result ) );
        case storeVersionNotFound:
            throw new StoreUpgrader.UpgradingStoreVersionNotFoundException(
                    getPathToStoreFile( storeDirectory, result ) );
        case unexpectedUpgradingStoreVersion:
            throw new StoreUpgrader.UnexpectedUpgradingStoreVersionException(
                    getPathToStoreFile( storeDirectory, result ), Legacy23Store.LEGACY_VERSION, result.actualVersion );
        case storeNotCleanlyShutDown:
            throw new StoreUpgrader.DatabaseNotCleanlyShutDownException();
        default:
            throw new IllegalArgumentException( "Unexpected outcome: " + result.outcome.name() );
        }
    }

    private String getPathToStoreFile( File storeDirectory, Result result )
    {
        return new File( storeDirectory, result.storeFilename ).getAbsolutePath();
    }

    private Result checkUpgradeableFor( File storeDirectory, String version )
    {
        if ( Legacy23Store.LEGACY_VERSION.equals( version ) )
        {
            // check version
            File neoStore = new File( storeDirectory, MetaDataStore.DEFAULT_NAME );
            Result result = storeVersionCheck.hasVersion( neoStore, version );

            if ( !result.outcome.isSuccessful() )
            {
                return result;
            }

            PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDirectory, fs );
            LogEntryReader<ReadableLogChannel> logEntryReader =
                    new VersionAwareLogEntryReader<>( new RecordStorageCommandReaderFactory() );
            LatestCheckPointFinder latestCheckPointFinder =
                    new LatestCheckPointFinder( logFiles, fs, logEntryReader );
            try
            {
                LatestCheckPoint latestCheckPoint = latestCheckPointFinder.find( logFiles.getHighestLogVersion() );
                if ( !latestCheckPoint.commitsAfterCheckPoint )
                {
                    return new Result( Result.Outcome.ok, null, null );
                }
            }
            catch ( IOException e )
            {
                // ignore exception and return db not cleanly shutdown
            }

            return new Result( Result.Outcome.storeNotCleanlyShutDown, null, null );
        }
        else
        {
            Result result = null;
            for ( StoreFile store : StoreFile.legacyStoreFilesForVersion( version ) )
            {
                String expectedVersion = store.forVersion( version );
                File storeFile = new File( storeDirectory, store.storeFileName() );
                result = legacyStoreVersionCheck.hasVersion( storeFile, expectedVersion, store.isOptional() );
                if ( !result.outcome.isSuccessful() )
                {
                    break;
                }
            }
            return result;
        }
    }

    public boolean hasCurrentVersion( File storeDir )
    {
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        Result result = storeVersionCheck.hasVersion( neoStore, CommonAbstractStore.ALL_STORES_VERSION );
        switch ( result.outcome )
        {
        case ok:
            return true;
        case missingStoreFile:
            // let's assume the db is empty
            return true;
        case storeVersionNotFound:
            return false;
        case unexpectedUpgradingStoreVersion:
            return false;
        default:
            throw new IllegalArgumentException( "Unknown outcome: " + result.outcome.name() );
        }
    }
}
