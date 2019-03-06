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

package org.neo4j.service;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.neo4j.service.test.BarService;
import org.neo4j.service.test.BazService;
import org.neo4j.service.test.FooService;
import org.neo4j.service.test.NotNamedService;
import org.neo4j.service.test.ServiceWithDuplicateName;
import org.neo4j.service.test.SomeService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ServicesTest
{

    @Test
    void loadAll()
    {
        final Collection<SomeService> services = Services.loadAll( SomeService.class );
        assertThat( services, allOf(
                hasSize( 2 ),
                containsInAnyOrder(
                        instanceOf( FooService.class ),
                        instanceOf( BarService.class )
                )
        ) );
    }

    @Test
    void loadByName()
    {
        final Optional<SomeService> foo = Services.load( SomeService.class, "foo" );
        assertTrue( foo.isPresent() );
        assertThat( foo.get(), instanceOf( FooService.class ) );

        final Optional<SomeService> bar = Services.load( SomeService.class, "bar" );
        assertTrue( bar.isPresent() );
        assertThat( bar.get(), instanceOf( BarService.class ) );
    }

    @Test
    void loadByKey()
    {
        final Optional<NotNamedService> impl1 = Services.load( NotNamedService.class, 1L, NotNamedService::id );
        assertTrue( impl1.isPresent() );
        assertThat( impl1.get(), instanceOf( NotNamedService.ServiceImpl1.class ) );

        final Optional<NotNamedService> impl2 = Services.load( NotNamedService.class, 2L, NotNamedService::id );
        assertTrue( impl2.isPresent() );
        assertThat( impl2.get(), instanceOf( NotNamedService.ServiceImpl2.class ) );
    }

    @Test
    void loadOrFail_load()
    {
        final SomeService service = Services.loadOrFail( SomeService.class, "foo" );
        assertThat( service, instanceOf( FooService.class ) );
    }

    @Test
    void loadOrFail_fail()
    {
        assertThrows( NoSuchElementException.class, () -> Services.loadOrFail( SomeService.class, "nonexisting-key" ) );
    }

    @Test
    void loadByNameNoMatch()
    {
        final Optional<SomeService> provider = Services.load( SomeService.class, "nonexisting-key" );
        assertFalse( provider.isPresent() );
    }

    @Test
    void failOnDuplicateKeyWhenLoadingByName()
    {
        final RuntimeException error = assertThrows( RuntimeException.class, () -> Services.load( ServiceWithDuplicateName.class, "duplicate-name" ) );
        assertThat( error.getMessage(), containsString( "Found multiple service providers" ) );
    }

    @Test
    void loadAllFromCurrentAndContextClassLoadersNoDuplicates()
    {
        withContextClassLoader( new TestClassLoader(), () ->
        {
            final Collection<SomeService> services = Services.loadAll( SomeService.class );
            assertThat( services, allOf(
                    hasSize( 3 ),
                    containsInAnyOrder(
                            instanceOf( FooService.class ),
                            instanceOf( BarService.class ),
                            instanceOf( BazService.class )
                    )
            ) );
        } );
    }

    @Test
    void loadByNameFromCurrentAndContextClassLoaders()
    {
        withContextClassLoader( new TestClassLoader(), () ->
        {
            final Optional<SomeService> foo = Services.load( SomeService.class, "foo" );
            assertTrue( foo.isPresent() );
            assertThat( foo.get(), instanceOf( FooService.class ) );

            final Optional<SomeService> baz = Services.load( SomeService.class, "baz" );
            assertTrue( baz.isPresent() );
            assertThat( baz.get(), instanceOf( BazService.class ) );
        } );
    }

    private static void withContextClassLoader( ClassLoader contextClassLoader, Runnable action )
    {
        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try
        {
            Thread.currentThread().setContextClassLoader( contextClassLoader );
            action.run();
        }
        finally
        {
            Thread.currentThread().setContextClassLoader( originalClassLoader );
        }
    }

    private static final class TestClassLoader extends ClassLoader
    {
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
