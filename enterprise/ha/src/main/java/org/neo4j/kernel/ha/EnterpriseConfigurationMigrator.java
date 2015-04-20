/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import org.neo4j.kernel.configuration.GraphDatabaseConfigurationMigrator;

// TODO: This shouldn't extend GraphDatabaseConfigurationMigrator,
// the migrations there will be applied when we load config from GraphDatabaseSettings.
// We need to move the migration of online backup settings out before this can be done.
public class EnterpriseConfigurationMigrator extends GraphDatabaseConfigurationMigrator
{
    {
        add(propertyRenamed(
        		"ha.machine_id", 
        		"ha.server_id", 
        		"ha.machine_id has been replaced with ha.server_id"));
    }
}
