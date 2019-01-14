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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;

public class UpgradeNotAllowedByConfigurationException extends UpgradeNotAllowedException
{
    public UpgradeNotAllowedByConfigurationException( String msg )
    {
        super( String.format( "%s Detailed description: %s", baseMessage() , msg)  );
    }

    public UpgradeNotAllowedByConfigurationException()
    {
        super( baseMessage() );
    }

    private static String baseMessage()
    {
        return String.format(
                "Neo4j cannot be started because the database files require upgrading and upgrades are disabled " +
                "in the configuration. Please set '%s' to 'true' in your configuration file and try again.",
                GraphDatabaseSettings.allow_upgrade.name() );
    }
}
