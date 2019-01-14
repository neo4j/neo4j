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
package org.neo4j.helpers;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ServiceTest
{

    @Test
    public void shouldLoadServiceInDefaultEnvironment()
    {
        FooService fooService = Service.load( FooService.class, "foo" );
        assertTrue( fooService instanceof BarService );
    }

    @Test
    public void whenContextCallsLoaderBlocksServicesFolderShouldLoadClassFromKernelClassloader()
    {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try
        {
            Thread.currentThread().setContextClassLoader( new ServiceBlockClassLoader( contextClassLoader ) );
            FooService fooService = Service.load( FooService.class, "foo" );
            assertTrue( fooService instanceof BarService );
        }
        finally
        {
            Thread.currentThread().setContextClassLoader( contextClassLoader );
        }
    }

    @Test
    public void whenContextClassLoaderOverridesServiceShouldLoadThatClass()
    {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try
        {
            Thread.currentThread().setContextClassLoader( new ServiceRedirectClassLoader( contextClassLoader ) );
            FooService fooService = Service.load( FooService.class, "foo" );
            assertTrue( fooService instanceof BazService );
        }
        finally
        {
            Thread.currentThread().setContextClassLoader( contextClassLoader );
        }
    }

    @Test
    public void whenContextClassLoaderDuplicatesServiceShouldLoadItOnce()
    {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try
        {
            Thread.currentThread().setContextClassLoader( Service.class.getClassLoader() );
            Iterable<FooService> services = Service.load( FooService.class );
            int size = 0;
            for ( FooService fooService : services )
            {
                size++;
            }
            assertEquals( 1, size );
        }
        finally
        {
            Thread.currentThread().setContextClassLoader( contextClassLoader );
        }
    }

    private static final class ServiceBlockClassLoader extends ClassLoader
    {

        ServiceBlockClassLoader( ClassLoader parent )
        {
            super( parent );
        }

        @Override
        public URL getResource( String name )
        {
            return name.startsWith( "META-INF/services" ) ? null : super.getResource( name );
        }

        @Override
        public Enumeration<URL> getResources( String name ) throws IOException
        {
            return name.startsWith( "META-INF/services" ) ? Collections.enumeration( Collections.<URL>emptySet() )
                                                          : super.getResources( name );
        }

        @Override
        public InputStream getResourceAsStream( String name )
        {
            return name.startsWith( "META-INF/services" ) ? null : super.getResourceAsStream( name );
        }
    }

    private static final class ServiceRedirectClassLoader extends ClassLoader
    {

        ServiceRedirectClassLoader( ClassLoader parent )
        {
            super( parent );
        }

        @Override
        public URL getResource( String name )
        {
            return name.startsWith( "META-INF/services" ) ? super.getResource( "test/" + name )
                                                          : super.getResource( name );
        }

        @Override
        public Enumeration<URL> getResources( String name ) throws IOException
        {
            return name.startsWith( "META-INF/services" ) ? super.getResources( "test/" + name )
                                                          : super.getResources( name );
        }

        @Override
        public InputStream getResourceAsStream( String name )
        {
            return name.startsWith( "META-INF/services" ) ? super.getResourceAsStream( "test/" + name )
                                                          : super.getResourceAsStream( name );
        }
    }
}
