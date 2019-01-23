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
package org.neo4j.test.extension;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.platform.commons.JUnitException;

import java.io.File;
import java.io.PrintStream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.resources.Profiler;
import org.neo4j.test.rule.TestDirectory;

/**
 * A sampling profiler extension for JUnit 5. This extension profiles a given set of threads that run in a unit test, and if the test fails, prints a profile
 * of where the time was spent. This is particularly useful for tests that has a tendency to fail with a timeout, and this extension can be used to diagnose
 * such flaky tests.
 * <p>
 * The profile output is printed to a {@code profiler-output.txt} file in the test directory by default.
 * <p>
 * Here is an example of how to use it:
 *
 * <pre><code>
 *     {@literal @}ExtendWith( {TestDirectoryExtension.class, PorfilerExtension.class} )
 *     public class MyTest
 *     {
 *         {@literal @}Inject
 *         public Profiler profiler;
 *
 *         {@literal @}Test
 *         void testSomeStuff()
 *         {
 *             profiler.profile();
 *             // ... do some stuff in this thread.
 *         }
 *     }
 * </code></pre>
 *
 * @see Profiler The Profiler interface, for more information on how to use the injected profiler instance.
 */
public class ProfilerExtension extends StatefullFieldExtension<Profiler> implements BeforeEachCallback, AfterEachCallback
{
    static final String PROFILER_KEY = "profiler";
    static final Namespace PROFILER_NAMESPACE = Namespace.create( PROFILER_KEY );

    @Override
    protected String getFieldKey()
    {
        return PROFILER_KEY;
    }

    @Override
    protected Class<Profiler> getFieldType()
    {
        return Profiler.class;
    }

    @Override
    protected Profiler createField( ExtensionContext extensionContext )
    {
        return Profiler.profiler();
    }

    @Override
    protected Namespace getNameSpace()
    {
        return PROFILER_NAMESPACE;
    }

    @Override
    public void beforeEach( ExtensionContext context )
    {
        getStoredValue( context ).reset();
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        Profiler profiler = getStoredValue( context );
        try
        {
            profiler.finish();
            if ( context.getExecutionException().isPresent() )
            {
                ExtensionContext.Store testDirStore = getStore( context, TestDirectoryExtension.TEST_DIRECTORY_NAMESPACE );
                TestDirectory testDir = (TestDirectory) testDirStore.get( TestDirectoryExtension.TEST_DIRECTORY );
                File profileOutputFile = testDir.createFile( "profiler-output.txt" );
                FileSystemAbstraction fs = testDir.getFileSystem();

                try ( PrintStream out = new PrintStream( fs.openAsOutputStream( profileOutputFile, false ) ) )
                {
                    String displayName = context.getTestClass().map( Class::getSimpleName ).orElse( "class" ) + "." + context.getDisplayName();
                    profiler.printProfile( out, displayName );
                }
            }
        }
        catch ( Exception e )
        {
            throw new JUnitException( "Failed to finish profiling and/or produce profiling output.", e );
        }
    }
}
