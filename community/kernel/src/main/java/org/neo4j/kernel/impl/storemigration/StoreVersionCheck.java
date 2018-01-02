/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.kernel.impl.store.MetaDataStore;

import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result.Outcome;

public class StoreVersionCheck
{
    private final PageCache pageCache;

    public StoreVersionCheck( PageCache pageCache )
    {
        this.pageCache = pageCache;
    }

    public Result hasVersion( File neostoreFile, String expectedVersion )
    {
        long record;

        try
        {
            record = MetaDataStore.getRecord( pageCache, neostoreFile, STORE_VERSION );
        }
        catch ( IOException ex )
        {
            // since we cannot read let's assume the file is not there
            return new Result( Outcome.missingStoreFile, null, neostoreFile.getName() );
        }

        if ( record == MetaDataStore.FIELD_NOT_PRESENT )
        {
            return new Result( Outcome.storeVersionNotFound, null, neostoreFile.getName() );
        }

        String storeVersion = MetaDataStore.versionLongToString( record );
        if ( !expectedVersion.equals( storeVersion ) )
        {
            return new Result( Outcome.unexpectedUpgradingStoreVersion, storeVersion, expectedVersion );
        }

        return new Result( Outcome.ok, null, neostoreFile.getName() );
    }

    public static class Result
    {
        public final Outcome outcome;
        public final String actualVersion;
        public final String storeFilename;

        public Result( Outcome outcome, String actualVersion, String storeFilename )
        {
            this.outcome = outcome;
            this.actualVersion = actualVersion;
            this.storeFilename = storeFilename;
        }

        public enum Outcome
        {
            ok( true ),
            missingStoreFile( false ),
            storeVersionNotFound( false ),
            unexpectedUpgradingStoreVersion( false );

            private final boolean success;

            Outcome( boolean success )
            {
                this.success = success;
            }

            public boolean isSuccessful()
            {
                return this.success;
            }
        }
    }
}
