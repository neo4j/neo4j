/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.kernel.KernelVersion;

public class LogEntryParserSets
{
    public static LogEntryParserSet parserSet( KernelVersion version )
    {
        switch ( version )
        {
        case V2_3:
            return LogEntryParserSetV2_3.V2_3;
        case V4_0:
            return LogEntryParserSetV4_0.V4_0;
        case V4_2:
            return LogEntryParserSetV4_2.V4_2;
        default:
            throw new IllegalArgumentException( "No log entries version matching " + version );
        }
    }

    public static LogEntryParserSet checkpointParserSet( KernelVersion version )
    {
        switch ( version )
        {
        case V4_2:
        default:
            throw new IllegalArgumentException( "No checkpoint log entries version matching " + version );
        }
    }
}
