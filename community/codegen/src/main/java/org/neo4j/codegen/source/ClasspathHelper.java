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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

final class ClasspathHelper
{
    private ClasspathHelper()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    static String javaClasspathString()
    {
        return System.getProperty( "java.class.path" );
    }

    static Set<String> javaClasspath()
    {
        String[] classpathElements = javaClasspathString().split( File.pathSeparator );
        Set<String> result = new LinkedHashSet<>();

        for ( String element : classpathElements )
        {
            result.add( canonicalPath( element ) );
        }

        return result;
    }

    static String fullClasspathStringFor( ClassLoader classLoader )
    {
        Set<String> classpathElements = fullClasspathFor( classLoader );
        return formClasspathString( classpathElements );
    }

    static Set<String> fullClasspathFor( ClassLoader classLoader )
    {
        Set<String> result = new LinkedHashSet<>();

        result.addAll( javaClasspath() );

        ClassLoader loader = classLoader;
        while ( loader != null )
        {
            if ( loader instanceof URLClassLoader )
            {
                for ( URL url : ((URLClassLoader) loader).getURLs() )
                {
                    result.add( canonicalPath( url ) );
                }
            }
            loader = loader.getParent();
        }

        return result;
    }

    private static String canonicalPath( URL url )
    {
        return canonicalPath( url.getPath() );
    }

    private static String canonicalPath( String path )
    {
        try
        {
            File file = new File( path );
            return file.getCanonicalPath();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to get canonical path for: '" + path + "'", e );
        }
    }

    private static String formClasspathString( Set<String> classPathElements )
    {
        StringBuilder classpath = new StringBuilder();

        Iterator<String> classPathElementsIterator = classPathElements.iterator();
        while ( classPathElementsIterator.hasNext() )
        {
            classpath.append( classPathElementsIterator.next() );
            if ( classPathElementsIterator.hasNext() )
            {
                classpath.append( File.pathSeparator );
            }
        }

        return classpath.toString();
    }
}
