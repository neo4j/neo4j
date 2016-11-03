/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.graphdb.mockfs;

import java.io.IOException;
import java.util.zip.ZipOutputStream;

import org.neo4j.io.fs.FileSystemAbstraction;

public class CloseTrackingFileSystem implements FileSystemAbstraction.ThirdPartyFileSystem
{
    private boolean closed = false;

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public void dumpToZip( ZipOutputStream zip, byte[] scratchPad ) throws IOException
    {
    }

    public boolean isClosed()
    {
        return closed;
    }
}
