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

import java.util.Optional;

public interface StoreVersionCheck
{
    /**
     * Store version of an existing store (this instance knows which store it's about).
     * @return store version of the existing store.
     */
    Optional<String> storeVersion();

    /**
     * Store version as dictated by configuration.
     * @return store version that configuration says is the version to use.
     */
    String configuredVersion();

    StoreVersion versionInformation( String storeVersion );

    /**
     * Should only be called if {@link #storeVersion()} and {@link #configuredVersion()} aren't equal.
     * Checks whether or not the store version can be upgraded to the desired (i.e. from configured) version.
     * @param desiredVersion store version to check whether or not the existing store is upgradable to.
     * @return Result of the check.
     */
    Result checkUpgrade( String desiredVersion );

    class Result
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

        @Override
        public String toString()
        {
            return "Result{" + "outcome=" + outcome + ", actualVersion='" + actualVersion + '\'' + ", storeFilename='" + storeFilename + '\'' + '}';
        }
    }

    enum Outcome
    {
        ok( true ),
        missingStoreFile( false ),
        storeVersionNotFound( false ),
        unexpectedStoreVersion( false ),
        attemptedStoreDowngrade( false ),
        storeNotCleanlyShutDown( false ),
        unexpectedUpgradingVersion( false );

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
