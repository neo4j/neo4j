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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

public class IllegalLogFormatException extends IOException
{
    private final long expected;
    private final long was;

    public IllegalLogFormatException( long expected, long was )
    {
        super( "Invalid log format version found, expected " + expected + " but was " + was +
                ". To be able to upgrade from an older log format version there must have " +
                "been a clean shutdown of the database" );
        this.expected = expected;
        this.was = was;
    }

    /** Check if the log we read was from a newer version of Neo4j. */
    public boolean wasNewerLogVersion()
    {
        return expected < was;
    }
}
