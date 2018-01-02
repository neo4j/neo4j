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
package org.neo4j.codegen.source;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;

import org.neo4j.codegen.ByteCodes;

class FileManager extends ForwardingJavaFileManager<StandardJavaFileManager>
{
    private final Map<String/*className*/, ClassFile> classes = new HashMap<>();

    FileManager( StandardJavaFileManager fileManager )
    {
        super( fileManager );
    }

    @Override
    public JavaFileObject getJavaFileForOutput( Location location, String className,
                                                JavaFileObject.Kind kind, FileObject sibling ) throws IOException
    {
        ClassFile file = new ClassFile( className );
        classes.put( className, file );
        return file;
    }

    public Iterable<? extends ByteCodes> bytecodes()
    {
        return classes.values();
    }

    private static class ClassFile extends SimpleJavaFileObject implements ByteCodes
    {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final String className;

        protected ClassFile( String className )
        {
            super( URI.create( "classes:/" + className.replace( '.', '/' ) + Kind.CLASS.extension ), Kind.CLASS );
            this.className = className;
        }

        @Override
        public OutputStream openOutputStream() throws IOException
        {
            return bytes;
        }

        @Override
        public String name()
        {
            return className;
        }

        @Override
        public ByteBuffer bytes()
        {
            return ByteBuffer.wrap( bytes.toByteArray() );
        }
    }
}
