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
package org.neo4j.test.jar;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import static java.util.Objects.requireNonNull;

/**
 * Utility to create jar files containing classes from the current classpath.
 */
public class JarBuilder
{
    public URL createJarFor( File f, Class<?>... classesToInclude ) throws IOException
    {
        try ( JarOutputStream jarOut = new JarOutputStream( Files.newOutputStream( f.toPath() ) ) )
        {
            for ( Class<?> target : classesToInclude )
            {
                String fileName = target.getName().replace( '.', '/' ) + ".class";
                jarOut.putNextEntry( new ZipEntry( fileName ) );
                jarOut.write( classCompiledBytes( fileName ) );
                jarOut.closeEntry();
            }
        }
        return f.toURI().toURL();
    }

    private byte[] classCompiledBytes( String fileName ) throws IOException
    {
        try ( InputStream in = getClass().getClassLoader().getResourceAsStream( fileName ) )
        {
            requireNonNull( in );
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            while ( in.available() > 0 )
            {
                out.write( in.read() );
            }

            return out.toByteArray();
        }
    }
}
