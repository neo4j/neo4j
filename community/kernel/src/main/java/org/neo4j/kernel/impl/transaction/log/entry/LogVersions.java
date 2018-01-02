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
package org.neo4j.kernel.impl.transaction.log.entry;

/**
 * @deprecated since from 2.2.4 and onwards there's only one version in town, namely {@link LogEntryVersion}.
 * The way this log version was used was actually to set a hard command format version inside a particular log file
 * so even if it may have looked like multiple versions of log entries were supported it wasn't. Also the
 * log entry versions implied specific command versions so it was a bit of a mess. This class is here as
 * long as we decide to keep the log format header, which may be good to keep for backwards compatibility of
 * reading log headers.
 */
@Deprecated
public class LogVersions
{
    private LogVersions()
    {
        // no instances are allowed
    }

    // This version will probably be the end of the line of log header format versions.
    // Please don't add more since they aren't really used anyway.
    public static final byte CURRENT_LOG_VERSION = 6;

    // on disk current format version
    static final short CURRENT_FORMAT_VERSION = CURRENT_LOG_VERSION & 0xFF;
}
