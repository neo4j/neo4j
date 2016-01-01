/**
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Outcome;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.StoreFile19;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.StoreFile20;

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
        Triplet<Outcome, String, String> outcome = checkUpgradeableFor19( storeDirectory );

        if ( outcome.first().isSuccessful() )
        {
            return Legacy19Store.LEGACY_VERSION;
        }
        else
        {
            outcome = checkUpgradeableFor20( storeDirectory );
            if ( !outcome.first().isSuccessful() )
            {
                String foundVersion = outcome.second();
                String storeFileName = outcome.third();
                switch ( outcome.first() )
                {
                    case missingStoreFile:
                        throw new StoreUpgrader.UpgradeMissingStoreFilesException( storeFileName );
                    case storeVersionNotFound:
                        throw new StoreUpgrader.UpgradingStoreVersionNotFoundException( storeFileName );
                    case unexpectedUpgradingStoreVersion:
                        throw new StoreUpgrader.UnexpectedUpgradingStoreVersionException(
                                storeFileName, Legacy20Store.LEGACY_VERSION, foundVersion );
                    default:
                        throw new IllegalArgumentException( outcome.first().name() );
                }
            }
            else
            {
                return Legacy20Store.LEGACY_VERSION;
            }
        }
    }

    private Triplet<Outcome, String, String> checkUpgradeableFor20( File storeDirectory )
    {
        Triplet<Outcome, String, String> outcome = null;
        for ( StoreFile20 store : StoreFile20.legacyStoreFiles() )
        {
            String expectedVersion = store.legacyVersion();
            File storeFile = new File( storeDirectory, store.storeFileName() );
            Pair<Outcome, String> check = storeVersionCheck.hasVersion( storeFile, expectedVersion );
            outcome = Triplet.of( check.first(), check.other(), storeFile.getName() );
            if ( !check.first().isSuccessful() )
            {
                break;
            }
        }
        return outcome;
    }


    private Triplet<Outcome, String, String> checkUpgradeableFor19( File storeDirectory )
    {
        Triplet<Outcome, String, String> outcome = null;
        for ( StoreFile19 store : StoreFile19.legacyStoreFiles() )
        {
            String expectedVersion = store.legacyVersion();
            File storeFile = new File( storeDirectory, store.storeFileName() );
            Pair<Outcome, String> check = storeVersionCheck.hasVersion( storeFile, expectedVersion );
            outcome = Triplet.of( check.first(), check.other(), storeFile.getName() );
            if ( !check.first().isSuccessful() )
            {
                break;
            }
        }
        return outcome;
    }
}
