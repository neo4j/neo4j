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
import java.io.IOException;
import java.util.Optional;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;

import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.storemigration.StoreVersionCheck.Result.Outcome;

public class StoreVersionCheck
{
    private final PageCache pageCache;

    public StoreVersionCheck( PageCache pageCache )
    {
        this.pageCache = pageCache;
    }

    public Optional<String> getVersion( File neostoreFile ) throws IOException
    {
        long record = MetaDataStore.getRecord( pageCache, neostoreFile, STORE_VERSION );
        if ( record == MetaDataRecordFormat.FIELD_NOT_PRESENT )
        {
            return Optional.empty();
        }
        return Optional.of( MetaDataStore.versionLongToString( record ) );
    }

    public Result hasVersion( File neostoreFile, String expectedVersion )
    {
        Optional<String> storeVersion;

        try
        {
            storeVersion = getVersion( neostoreFile );
        }
        catch ( IOException e )
        {
            // since we cannot read let's assume the file is not there
            return new Result( Outcome.missingStoreFile, null, neostoreFile.getName() );
        }

        return storeVersion
                .map( v ->
                        expectedVersion.equals( v ) ?
                        new Result( Outcome.ok, null, neostoreFile.getName() ) :
                        new Result( Outcome.unexpectedStoreVersion, v, neostoreFile.getName() ) )
                .orElseGet( () -> new Result( Outcome.storeVersionNotFound, null, neostoreFile.getName() ) );
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
            unexpectedStoreVersion( false ),
            attemptedStoreDowngrade( false ),
            storeNotCleanlyShutDown( false );

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
