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
package org.neo4j.test.extension;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.platform.commons.JUnitException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.neo4j.test.rule.TestDirectory.testDirectory;

public class TestDirectoryExtension extends StatefullFieldExtension<TestDirectory> implements BeforeEachCallback, AfterEachCallback, AfterAllCallback
{
    static final String TEST_DIRECTORY = "testDirectory";
    static final Namespace TEST_DIRECTORY_NAMESPACE = Namespace.create( TEST_DIRECTORY );

    @Override
    public void beforeEach( ExtensionContext context ) throws Exception
    {
        TestDirectory testDirectory = getStoredValue( context );
        testDirectory.prepareDirectory( context.getRequiredTestClass(), context.getRequiredTestMethod().getName() );
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        TestDirectory testDirectory = getStoredValue( context );
        try
        {
            testDirectory.complete( !context.getExecutionException().isPresent() );
        }
        catch ( Exception e )
        {
            throw new JUnitException( format( "Fail to cleanup test directory for %s test.", context.getDisplayName() ), e );
        }
    }

    @Override
    protected String getFieldKey()
    {
        return TEST_DIRECTORY;
    }

    @Override
    protected Class<TestDirectory> getFieldType()
    {
        return TestDirectory.class;
    }

    @Override
    protected TestDirectory createField( ExtensionContext extensionContext )
    {
        ExtensionContext.Store fileSystemStore = getStore( extensionContext, FileSystemExtension.FILE_SYSTEM_NAMESPACE );
        FileSystemAbstraction fileSystemAbstraction = fileSystemStore.get( FileSystemExtension.FILE_SYSTEM, FileSystemAbstraction.class );
        return fileSystemAbstraction != null ? testDirectory(fileSystemAbstraction) : testDirectory();
    }

    @Override
    protected Namespace getNameSpace()
    {
        return TEST_DIRECTORY_NAMESPACE;
    }
}
