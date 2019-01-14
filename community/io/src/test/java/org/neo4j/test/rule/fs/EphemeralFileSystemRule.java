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
package org.neo4j.test.rule.fs;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;

public class EphemeralFileSystemRule extends FileSystemRule<EphemeralFileSystemAbstraction>
{
    public EphemeralFileSystemRule()
    {
        super( new EphemeralFileSystemAbstraction() );
    }

    public EphemeralFileSystemAbstraction snapshot( Runnable action ) throws Exception
    {
        EphemeralFileSystemAbstraction snapshot = fs.snapshot();
        try
        {
            action.run();
        }
        finally
        {
            fs.close();
            fs = snapshot;
        }
        return fs;
    }

    public void clear() throws Exception
    {
        fs.close();
        fs = new EphemeralFileSystemAbstraction();
    }

    public void crash()
    {
        fs.crash();
    }

    public EphemeralFileSystemAbstraction snapshot()
    {
        return fs.snapshot();
    }

    public long checksum()
    {
        return fs.checksum();
    }
}
