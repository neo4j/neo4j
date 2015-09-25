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

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v22.Legacy22Store;

/**
 * Logic to check whether a database version is upgradable to the current version. It looks at the
 * version information found in the store files themselves.
 */
public class UpgradableDatabase
{
    private final StoreVersionCheck storeVersionCheck;

    public UpgradableDatabase( StoreVersionCheck storeVersionCheck )
    {
        this.storeVersionCheck = storeVersionCheck;
    }

    public boolean storeFilesUpgradeable( File storeDirectory )
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

        // report error
        String path = new File( storeDirectory, result.storeFilename ).getAbsolutePath();
        switch ( result.outcome )
        {
            case missingStoreFile:
                throw new StoreUpgrader.UpgradeMissingStoreFilesException( path );
            case storeVersionNotFound:
                throw new StoreUpgrader.UpgradingStoreVersionNotFoundException( path );
            case unexpectedUpgradingStoreVersion:
                throw new StoreUpgrader.UnexpectedUpgradingStoreVersionException(
                        path, Legacy21Store.LEGACY_VERSION, result.actualVersion );
            default:
                throw new IllegalArgumentException( result.outcome.name() );
        }
    }

    private Result checkUpgradeableFor( File storeDirectory, String version )
    {
        Result result = null;
        for ( StoreFile store : StoreFile.legacyStoreFilesForVersion( version ) )
        {
            String expectedVersion = store.forVersion( version );
            File storeFile = new File( storeDirectory, store.storeFileName() );
            result = storeVersionCheck.hasVersion( storeFile, expectedVersion );
            if ( !result.outcome.isSuccessful() )
            {
                break;
            }
        }
        return result;
    }

    public boolean hasCurrentVersion( PageCache pageCache, File storeDir ) throws IOException
    {
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        long versionLong = MetaDataStore.getRecord( pageCache, neoStore, MetaDataStore.Position.STORE_VERSION );
        String versionAsString = MetaDataStore.versionLongToString( versionLong );
        return CommonAbstractStore.ALL_STORES_VERSION.equals( versionAsString );
    }
}
