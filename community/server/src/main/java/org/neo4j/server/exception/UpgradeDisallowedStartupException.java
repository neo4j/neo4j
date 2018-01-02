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
package org.neo4j.server.exception;

import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByDatabaseModeException;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedException;
import org.neo4j.logging.Log;
import org.neo4j.server.ServerStartupException;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.allow_store_upgrade;

public class UpgradeDisallowedStartupException extends ServerStartupException
{
    public UpgradeDisallowedStartupException(UpgradeNotAllowedException cause)
    {
        super(cause.getMessage(), cause);
    }

    @Override
    public void describeTo( Log log )
    {
        if( getCause() instanceof UpgradeNotAllowedByDatabaseModeException )
        {
            log.error( "Neo4j cannot be started, because the database files require upgrading and upgrading is not " +
                       "supported in this database mode. Please start the database in stand-alone mode to allow a " +
                       "safe upgrade of the database files." );
        }
        else if( getCause() instanceof UpgradeNotAllowedByConfigurationException )
        {
            log.error( "Neo4j cannot be started, because the database files require upgrading and upgrades are " +
                       "disabled in configuration. Please set '%s' to 'true' in your configuration file and try again.",
                    allow_store_upgrade.name());
        }
        else
        {
            super.describeTo( log );
        }
    }
}
