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
package org.neo4j.test.docs;

import java.io.File;
import java.io.PrintWriter;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.io.fs.FileUtils.path;

public class DocsIncludeFile implements TestRule
{
    public static DocsIncludeFile inSection( String section )
    {
        return new DocsIncludeFile( section );
    }

    public void printf( String format, Object... parameters )
    {
        writer.printf( format, parameters );
    }

    public void println( String line )
    {
        writer.println( line );
    }

    public void println()
    {
        writer.println();
    }

    private final String section;
    private PrintWriter writer;

    DocsIncludeFile( String section )
    {
        this.section = section;
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        String methodName = description.getMethodName();
        assertNotNull( DocsIncludeFile.class.getName() + " must be a non-static @Rule", methodName );
        File dir = path( "target", "docs", section, "includes" );
        assertTrue( dir + " must be a directory", dir.isDirectory() || dir.mkdirs() );
        final File file = path( dir, methodName + ".asciidoc" );
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                if ( file.exists() )
                {
                    assertTrue( file + " should not exist", file.isFile() && file.delete() );
                }
                writer = new PrintWriter( file );
                try
                {
                    base.evaluate();
                }
                finally
                {
                    try
                    {
                        writer.close();
                    }
                    finally
                    {
                        writer = null;
                    }
                }
            }
        };
    }
}
