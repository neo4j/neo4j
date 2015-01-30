/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.extension;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

public class KernelExtensionsTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldHandleExtensionsThatDependOnEachOther() throws Throwable
    {
        // Given
        LifeSupport life = new LifeSupport();
        Dependencies dependencies = new Dependencies();
        KernelExtensions extensions = life.add( new KernelExtensions(
                asList(
                    new DependsOnExtensionA.Factory(),
                    new ExtensionA.Factory() ),
                new Config(), dependencies, UnsatisfiedDependencyStrategies.fail() ));

        // When
        life.start();

        // Then no exception should've been thrown
        // And when
        ExtensionA extA = extensions.resolveDependency( ExtensionA.class );
        DependsOnExtensionA depsOnExtA = extensions.resolveDependency( DependsOnExtensionA.class );

        // Then
        assertNotNull( extA );
        assertNotNull( depsOnExtA );

        assertThat( depsOnExtA.extension, equalTo(extA) );
    }

    @Test
    public void shouldDescribeMissingDependenciesWell() throws Throwable
    {
        // Given
        LifeSupport life = new LifeSupport();
        Dependencies dependencies = new Dependencies();
        KernelExtensions extensions = life.add( new KernelExtensions(
                asList(
                    new DependsOnExtensionA.Factory(),
                    new ExtensionB.Factory() ),
                new Config(), dependencies, UnsatisfiedDependencyStrategies.fail() ));

        // Expect
        exception.expectCause( hasMessage( equalTo("Unable to instantiate Factory, " +
                                           "unable to satisfy the following dependencies: [ExtensionA].") ) );

        // When
        life.start();
    }

    public static class ExtensionA extends LifecycleAdapter
    {
        public static class Factory extends KernelExtensionFactory<Factory.Dependencies>
        {
            public interface Dependencies
            {

            }

            protected Factory()
            {
                super( "extension-a" );
            }

            @Override
            public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
            {
                return new ExtensionA();
            }

        }
    }

    public static class ExtensionB extends LifecycleAdapter
    {
        public static class Factory extends KernelExtensionFactory<Factory.Dependencies>
        {
            public interface Dependencies
            {

            }

            protected Factory()
            {
                super( "extension-b" );
            }

            @Override
            public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
            {
                return new ExtensionB();
            }

        }
    }

    public static class DependsOnExtensionA extends LifecycleAdapter
    {
        public static class Factory extends KernelExtensionFactory<Factory.Dependencies>
        {
            public interface Dependencies
            {
                ExtensionA extension();
            }

            protected Factory()
            {
                super( "extension-b" );
            }

            @Override
            public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
            {
                return new DependsOnExtensionA(dependencies.extension());
            }

        }

        private final ExtensionA extension;

        public DependsOnExtensionA( ExtensionA extension )
        {
            this.extension = extension;
        }
    }
}