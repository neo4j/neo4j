/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnzipUtil
{
    /**
     * Unzip a zip file located in the resource directly of provided class.
     * The zip file is expected to contain a single file with the same name as target.
     * The content is unpacked into target location.
     */
    public static void unzipResource( Class<?> klass, String zipName, File target ) throws IOException
    {
        URL resource = klass.getResource( zipName );
        if ( resource == null )
        {
            throw new FileNotFoundException();
        }

        try ( ZipFile zipFile = new ZipFile( resource.getFile() ) )
        {
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            assertTrue( entries.hasMoreElements() );
            ZipEntry entry = entries.nextElement();
            assertEquals( target.getName(), entry.getName() );
            Files.copy( zipFile.getInputStream( entry ), target.toPath() );
        }
    }
}
