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
package org.neo4j.kernel.impl.factory;

public class DatabaseInfo
{
    public static final DatabaseInfo UNKNOWN = new DatabaseInfo( Edition.unknown, OperationalMode.unknown );
    public static final DatabaseInfo COMMUNITY = new DatabaseInfo( Edition.community, OperationalMode.single );
    public static final DatabaseInfo ENTERPRISE = new DatabaseInfo( Edition.enterprise, OperationalMode.single );
    public static final DatabaseInfo HA = new DatabaseInfo( Edition.enterprise, OperationalMode.ha );
    public static final DatabaseInfo CORE = new DatabaseInfo( Edition.enterprise, OperationalMode.core );
    public static final DatabaseInfo READ_REPLICA = new DatabaseInfo( Edition.enterprise, OperationalMode.read_replica );

    public final Edition edition;
    public final OperationalMode operationalMode;

    private DatabaseInfo( Edition edition, OperationalMode operationalMode )
    {
        this.edition = edition;
        this.operationalMode = operationalMode;
    }

    @Override
    public String toString()
    {
        return edition + " " + operationalMode;
    }

}
