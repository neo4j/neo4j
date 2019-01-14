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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;

import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.DatabaseNotCleanlyShutDownException;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnexpectedUpgradingStoreFormatException;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnexpectedUpgradingStoreVersionException;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UpgradeMissingStoreFilesException;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UpgradingStoreVersionNotFoundException;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result.Outcome;
import org.neo4j.kernel.recovery.LogTailScanner;

/**
 * Logic to check whether a database version is upgradable to the current version. It looks at the
 * version information found in the store files themselves.
 */
public class UpgradableDatabase
{
    private final StoreVersionCheck storeVersionCheck;
    private final RecordFormats format;
    private final LogTailScanner tailScanner;

    public UpgradableDatabase( StoreVersionCheck storeVersionCheck, RecordFormats format,
            LogTailScanner tailScanner )
    {
        this.storeVersionCheck = storeVersionCheck;
        this.format = format;
        this.tailScanner = tailScanner;
    }

    /**
     * Assumed to only be called if {@link #hasCurrentVersion(File)} returns {@code false}.
     *
     * @param storeDirectory the store to check for upgradability is in.
     * @return the {@link RecordFormats} the current store (which is upgradable) is currently in.
     * @throws UpgradeMissingStoreFilesException if store cannot be upgraded due to some store files are missing.
     * @throws UpgradingStoreVersionNotFoundException if store cannot be upgraded due to store
     * version cannot be determined.
     * @throws UnexpectedUpgradingStoreVersionException if store cannot be upgraded due to an unexpected store
     * version found.
     * @throws UnexpectedUpgradingStoreFormatException if store cannot be upgraded due to an unexpected store
     * format found.
     * @throws DatabaseNotCleanlyShutDownException if store cannot be upgraded due to not being cleanly shut down.
     */
    public RecordFormats checkUpgradeable( File storeDirectory )
    {
        Result result = storeVersionCheck.hasVersion( new File( storeDirectory, MetaDataStore.DEFAULT_NAME ),
                format.storeVersion() );
        if ( result.outcome.isSuccessful() )
        {
            // This store already has the format that we want
            // Although this method should not have been called in this case.
            return format;
        }

        RecordFormats fromFormat;
        try
        {
            fromFormat = RecordFormatSelector.selectForVersion( result.actualVersion );

            // If we are trying to open an enterprise store when configured to use community format, then inform the user
            // of the config setting to change since downgrades aren't possible but the store can still be opened.
            if ( FormatFamily.isLowerFamilyFormat( format, fromFormat ) )
            {
                throw new StoreUpgrader.UnexpectedUpgradingStoreFormatException();
            }

            if ( FormatFamily.isSameFamily( fromFormat, format ) && (fromFormat.generation() > format.generation()) )
            {
                // Tried to downgrade, that isn't supported
                result = new Result( Outcome.attemptedStoreDowngrade, fromFormat.storeVersion(),
                        new File( storeDirectory, MetaDataStore.DEFAULT_NAME ).getAbsolutePath() );
            }
            else
            {
                result = checkCleanShutDownByCheckPoint();
                if ( result.outcome.isSuccessful() )
                {
                    return fromFormat;
                }
            }
        }
        catch ( IllegalArgumentException e )
        {
            result = new Result( Outcome.unexpectedStoreVersion, result.actualVersion, result.storeFilename );
        }

        switch ( result.outcome )
        {
        case missingStoreFile:
            throw new StoreUpgrader.UpgradeMissingStoreFilesException( getPathToStoreFile( storeDirectory, result ) );
        case storeVersionNotFound:
            throw new StoreUpgrader.UpgradingStoreVersionNotFoundException(
                    getPathToStoreFile( storeDirectory, result ) );
        case attemptedStoreDowngrade:
            throw new StoreUpgrader.AttemptedDowngradeException();
        case unexpectedStoreVersion:
            throw new StoreUpgrader.UnexpectedUpgradingStoreVersionException( result.actualVersion, format.storeVersion() );
        case storeNotCleanlyShutDown:
            throw new StoreUpgrader.DatabaseNotCleanlyShutDownException();
        default:
            throw new IllegalArgumentException( "Unexpected outcome: " + result.outcome.name() );
        }
    }

    private Result checkCleanShutDownByCheckPoint()
    {
        // check version
        try
        {
            if ( !tailScanner.getTailInformation().commitsAfterLastCheckpoint() )
            {
                return new Result( Result.Outcome.ok, null, null );
            }
        }
        catch ( Throwable throwable )
        {
            // ignore exception and return db not cleanly shutdown
        }

        return new Result( Result.Outcome.storeNotCleanlyShutDown, null, null );
    }

    private String getPathToStoreFile( File storeDirectory, Result result )
    {
        return new File( storeDirectory, result.storeFilename ).getAbsolutePath();
    }

    boolean hasCurrentVersion( File storeDir )
    {
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        Result result = storeVersionCheck.hasVersion( neoStore, format.storeVersion() );
        switch ( result.outcome )
        {
        case ok:
        case missingStoreFile: // let's assume the db is empty
            return true;
        case storeVersionNotFound:
        case unexpectedStoreVersion:
        case attemptedStoreDowngrade:
            return false;
        default:
            throw new IllegalArgumentException( "Unknown outcome: " + result.outcome.name() );
        }
    }

    public String currentVersion()
    {
        return format.storeVersion();
    }
}
