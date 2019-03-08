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
package org.neo4j.kernel.impl.transaction.log.files;

import java.io.File;

public class RangeLogVersionVisitor implements LogVersionVisitor
{
    public static final long UNKNOWN = -1;

    private long lowestVersion = UNKNOWN;
    private File lowestFile;
    private long highestVersion = UNKNOWN;
    private File highestFile;

    @Override
    public void visit( File file, long logVersion )
    {
        if ( logVersion > highestVersion )
        {
            highestVersion = logVersion;
            highestFile = file;
        }
        if ( lowestVersion == UNKNOWN || logVersion < lowestVersion )
        {
            lowestVersion = logVersion;
            lowestFile = file;
        }
    }

    public long getLowestVersion()
    {
        return lowestVersion;
    }

    public File getLowestFile()
    {
        return lowestFile;
    }

    public long getHighestVersion()
    {
        return highestVersion;
    }

    public File getHighestFile()
    {
        return highestFile;
    }
}
