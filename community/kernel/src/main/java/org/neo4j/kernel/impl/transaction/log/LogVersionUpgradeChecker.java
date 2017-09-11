/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;

/**
 * Here we check the latest entry in the transaction log and make sure it matches the current version, if this check
 * fails, it means that we will write entries with a version not compatible with the previous version responsible for
 * creating the transaction logs.
 * <p>
 * This can be considered an upgrade since the user is not able to revert back to the previous version of neo4j. This
 * will effectively guard the users from accidental upgrades.
 */
public class LogVersionUpgradeChecker
{
    private LogVersionUpgradeChecker()
    {
        throw new AssertionError( "No instances allowed" );
    }

    public static void check( LogTailScanner tailScanner, Config config ) throws UpgradeNotAllowedByConfigurationException
    {
        if ( !config.get( GraphDatabaseSettings.allow_upgrade ) )
        {
            // The user doesn't want us to upgrade the store.
            LogEntryVersion latestLogEntryVersion = tailScanner.getTailInformation().latestLogEntryVersion;
            if ( latestLogEntryVersion != null && LogEntryVersion.moreRecentVersionExists( latestLogEntryVersion ) )
            {
                String message = "The version you're upgrading to is using a new transaction log format. This is a " +
                        "non-reversible upgrade and you wont be able to downgrade after starting";

                throw new UpgradeNotAllowedByConfigurationException( message );
            }
        }
    }
}
