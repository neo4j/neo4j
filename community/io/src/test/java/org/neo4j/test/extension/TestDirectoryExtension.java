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
package org.neo4j.test.extension;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.neo4j.test.rule.TestDirectory;

public class TestDirectoryExtension extends StatefullFieldExtension<TestDirectory>
        implements BeforeEachCallback, AfterEachCallback, AfterAllCallback
{
    private static final String TEST_DIRECTORY_STORE_KEY = "testDirectory";

    @Override
    public void beforeEach( ExtensionContext context ) throws Exception
    {
        TestDirectory testDirectory = getStoredValue( context );
        testDirectory.prepareDirectory( context.getRequiredTestClass(), context.getRequiredTestMethod().getName() );
    }

    @Override
    public void afterEach( ExtensionContext context ) throws Exception
    {
        TestDirectory testDirectory = getStoredValue( context );
        testDirectory.complete( true ); //TODO: for now lets assume its always fine
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        removeStoredValue( context );
    }

    @Override
    protected String getFieldKey()
    {
        return TEST_DIRECTORY_STORE_KEY;
    }

    @Override
    protected Class<TestDirectory> getFieldType()
    {
        return TestDirectory.class;
    }

    @Override
    protected TestDirectory createField()
    {
        return TestDirectory.testDirectory();
    }
}
