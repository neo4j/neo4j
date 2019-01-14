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
package org.neo4j.test.impl;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

public class ChannelOutputStreamTest
{
    @Rule
    public FileSystemRule fs = new EphemeralFileSystemRule();

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void shouldStoreAByteAtBoundary() throws Exception
    {
        File workFile = tmpDir.newFile();
        fs.mkdirs( tmpDir.getRoot() );
        OutputStream out = fs.openAsOutputStream( workFile, false );

        // When I write a byte[] that is larger than the internal buffer in
        // ChannelOutputStream..
        byte[] b = new byte[8097];
        b[b.length - 1] = 7;
        out.write( b );
        out.flush();

        // Then it should get cleanly written and be readable
        InputStream in = fs.openAsInputStream( workFile );
        in.skip( 8096 );
        assert in.read() == 7;
    }
}
