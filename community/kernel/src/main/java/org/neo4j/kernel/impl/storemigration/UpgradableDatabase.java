/**
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

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Outcome;

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

    public boolean storeFilesUpgradeable( File neoStoreFile )
    {
        try
		{
            checkUpgradeable( neoStoreFile );
            return true;
        }
		catch ( StoreUpgrader.UnableToUpgradeException e )
        {
            return false;
        }
    }

    public void checkUpgradeable( File neoStoreFile )
    {
        File storeDirectory = neoStoreFile.getParentFile();
        for ( StoreFile store : StoreFile.legacyStoreFiles() )
        {
            String expectedVersion = store.legacyVersion();
            File storeFile = new File( storeDirectory, store.storeFileName() );
            Pair<Outcome, String> outcome = storeVersionCheck.hasVersion( storeFile, expectedVersion );
            if ( !outcome.first().isSuccessful() )
            {
                switch ( outcome.first() )
                {
                case missingStoreFile:
                    throw new StoreUpgrader.UpgradeMissingStoreFilesException( storeFile.getName() );
                case storeVersionNotFound:
                    throw new StoreUpgrader.UpgradingStoreVersionNotFoundException( storeFile.getName() );
                case unexpectedUpgradingStoreVersion:
                    throw new StoreUpgrader.UnexpectedUpgradingStoreVersionException(
                            storeFile.getName(), expectedVersion, outcome.other() );
                default:
                    throw new IllegalArgumentException( outcome.first().name() );
                }
            }
        }
    }
}
