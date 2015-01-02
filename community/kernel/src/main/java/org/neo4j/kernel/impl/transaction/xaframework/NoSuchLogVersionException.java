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
package org.neo4j.kernel.impl.transaction.xaframework;

public class NoSuchLogVersionException extends MissingLogDataException
{
    private long version;

    public NoSuchLogVersionException( long version )
    {
        super( "No such log version: '" + version + "'. This means we encountered a log file that we expected " +
                "to find was missing. If you are unable to start the database due to this problem, please make " +
                "sure that the correct logical log files are in the database directory." );
        this.version = version;
    }

    public long getVersion()
    {
        return version;
    }
}
