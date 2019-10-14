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
package org.neo4j.kernel.extension;

import org.junit.jupiter.api.Test;

import org.neo4j.collection.Dependencies;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.extension.context.GlobalExtensionContext;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.internal.helpers.collection.Iterables.iterable;

@Neo4jLayoutExtension
class ExtensionContextTest
{
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private Neo4jLayout neo4jLayout;

    @Test
    void shouldConsultUnsatisfiedDependencyHandlerOnMissingDependencies()
    {
        GlobalExtensionContext context = mock( GlobalExtensionContext.class );
        ExtensionFailureStrategy handler = mock( ExtensionFailureStrategy.class );
        Dependencies dependencies = new Dependencies(); // that hasn't got anything.
        TestingExtensionFactory extensionFactory = new TestingExtensionFactory();
        GlobalExtensions extensions = new GlobalExtensions( context, iterable( extensionFactory ), dependencies, handler );

        try ( Lifespan ignored = new Lifespan( extensions ) )
        {
            verify( handler ).handle( eq( extensionFactory ), any( UnsatisfiedDependencyException.class ) );
        }
    }

    @Test
    void shouldFindDependenciesFromHierarchyBottomUp()
    {
        GlobalExtensionContext context = mock( GlobalExtensionContext.class );
        ExtensionFailureStrategy handler = mock( ExtensionFailureStrategy.class );
        Dependencies dependencies = new Dependencies();
        JobScheduler jobScheduler = mock( JobScheduler.class );
        dependencies.satisfyDependencies( jobScheduler );
        SubTestingExtensionFactory extensionFactory = new SubTestingExtensionFactory();
        GlobalExtensions extensions = new GlobalExtensions( context, iterable( extensionFactory ), dependencies, handler );

        try ( Lifespan ignored = new Lifespan( extensions ) )
        {
            assertNotNull( dependencies.resolveDependency( TestingExtension.class ) );
        }
    }

    @Test
    void shouldConsultUnsatisfiedDependencyHandlerOnFailingDependencyClasses()
    {
        GlobalExtensionContext context = mock( GlobalExtensionContext.class );
        ExtensionFailureStrategy handler = mock( ExtensionFailureStrategy.class );
        Dependencies dependencies = new Dependencies(); // that hasn't got anything.
        UninitializableExtensionFactory extensionFactory = new UninitializableExtensionFactory();
        GlobalExtensions extensions = new GlobalExtensions( context, iterable( extensionFactory ), dependencies, handler );

        try ( Lifespan ignored = new Lifespan( extensions ) )
        {
            verify( handler ).handle( eq( extensionFactory ), any( IllegalArgumentException.class ) );
        }
    }

    @Test
    void globalContextRootDirectoryEqualToStoreDirectory()
    {
        GlobalExtensionContext context = new GlobalExtensionContext( neo4jLayout, DatabaseInfo.TOOL, new Dependencies() );
        assertSame( neo4jLayout.databasesDirectory(), context.directory() );
    }

    @Test
    void databaseContextRootDirectoryEqualToDatabaseDirectory()
    {
        DatabaseExtensionContext context = new DatabaseExtensionContext( databaseLayout, DatabaseInfo.TOOL, new Dependencies() );
        assertSame( databaseLayout.databaseDirectory(), context.directory() );
    }

    private interface TestingDependencies
    {
        // Just some dependency
        JobScheduler jobScheduler();
    }

    private static class TestingExtensionFactory extends ExtensionFactory<TestingDependencies>
    {
        TestingExtensionFactory()
        {
            super( "testing" );
        }

        @Override
        public Lifecycle newInstance( ExtensionContext context, TestingDependencies dependencies )
        {
            dependencies.jobScheduler();
            return new TestingExtension();
        }
    }

    private static class SubTestingExtensionFactory extends TestingExtensionFactory
    {
        // Nothing to override.
    }

    private static class TestingExtension extends LifecycleAdapter
    {
        TestingExtension()
        {
            // We don't need it right now
        }
    }
}
