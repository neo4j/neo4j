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
package org.neo4j.coreedge.catchup.storecopy.edge;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.coreedge.catchup.storecopy.edge.StoreFileStreams;
import org.neo4j.io.fs.FileSystemAbstraction;

public class StreamToDisk implements StoreFileStreams
{
    private final File storeDir;
    private final FileSystemAbstraction fs;

    public StreamToDisk( File storeDir, FileSystemAbstraction fs ) throws IOException
    {
        this.storeDir = storeDir;
        this.fs = fs;
        fs.mkdirs( storeDir );
    }

    @Override
    public OutputStream createStream( String destination ) throws IOException
    {
        return fs.openAsOutputStream( new File( storeDir, destination ), true );
    }
}
