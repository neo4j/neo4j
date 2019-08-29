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

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.service.Services;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * Base class for testing a {@link ExtensionFactory}. The base test cases in this
 * class verifies that a extension upholds the {@link ExtensionFactory} contract.
 */
@TestDirectoryExtension
public abstract class ExtensionFactoryContractTest
{
    @Inject
    private TestDirectory target;

    private final Class<? extends ExtensionFactory<?>> extClass;
    private final String key;

    protected DatabaseManagementService managementService;

    protected ExtensionFactoryContractTest( String key, Class<? extends ExtensionFactory<?>> extClass )
    {
        this.extClass = extClass;
        this.key = key;
    }

    protected GraphDatabaseAPI graphDb( int instance )
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    @Test
    void extensionShouldHavePublicNoArgConstructor()
    {
        ExtensionFactory<?> instance = newInstance();
        assertNotNull( instance );
    }

    @Test
    void shouldBeAbleToLoadExtensionAsAServiceProvider()
    {
        ExtensionFactory<?> instance = loadInstance();
        assertNotNull( instance, "Could not load the kernel extension with the provided key" );
        assertSame( instance.getClass(), extClass, "Class of the loaded instance is a subclass of the extension class" );
    }

    @Test
    void differentInstancesShouldHaveEqualHashCodesAndBeEqual()
    {
        ExtensionFactory<?> one = newInstance();
        ExtensionFactory<?> two = newInstance();
        assertEquals( one.hashCode(), two.hashCode(), "new instances have different hash codes" );
        assertEquals( one, two, "new instances are not equals" );

        one = loadInstance();
        two = loadInstance();
        assertEquals( one.hashCode(), two.hashCode(), "loaded instances have different hash codes" );
        assertEquals( one, two, "loaded instances are not equals" );

        one = loadInstance();
        two = newInstance();
        assertEquals( one.hashCode(), two.hashCode(), "loaded instance and new instance have different hash codes" );
        assertEquals( one, two, "loaded instance and new instance are not equals" );
    }

    private ExtensionFactory<?> newInstance()
    {
        try
        {
            return extClass.newInstance();
        }
        catch ( Exception cause )
        {
            throw new IllegalArgumentException( "Could not instantiate extension class", cause );
        }
    }

    private ExtensionFactory<?> loadInstance()
    {
        return extClass.cast( Services.loadOrFail( ExtensionFactory.class, key ) );
    }
}
