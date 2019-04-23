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

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.service.Services;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * Base class for testing a {@link ExtensionFactory}. The base test cases in this
 * class verifies that a extension upholds the {@link ExtensionFactory} contract.
 */
public abstract class ExtensionFactoryContractTest
{
    private final Class<? extends ExtensionFactory<?>> extClass;
    private final String key;

    @Rule
    public final TestDirectory target = TestDirectory.testDirectory();
    protected DatabaseManagementService managementService;

    public ExtensionFactoryContractTest( String key, Class<? extends ExtensionFactory<?>> extClass )
    {
        this.extClass = extClass;
        this.key = key;
    }

    protected GraphDatabaseAPI graphDb( int instance )
    {
        Map<String, String> config = configuration( instance );
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().setConfigRaw( config ).build();
        return (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    /**
     * Override to create default configuration for the {@link ExtensionFactory}
     * under test.
     *
     * @param instance   used for differentiating multiple instances that will run
     *                   simultaneously.
     */
    protected Map<String, String> configuration( int instance )
    {
        return MapUtil.stringMap();
    }

    @Test
    public void extensionShouldHavePublicNoArgConstructor()
    {
        ExtensionFactory<?> instance = null;
        try
        {
            instance = newInstance();
        }
        catch ( IllegalArgumentException failure )
        {
            failure.printStackTrace();
            fail( "Contract violation: extension class must have public no-arg constructor (Exception in stderr)" );
        }
        assertNotNull( instance );
    }

    @Test
    public void shouldBeAbleToLoadExtensionAsAServiceProvider()
    {
        ExtensionFactory<?> instance = null;
        try
        {
            instance = loadInstance();
        }
        catch ( ClassCastException failure )
        {
            failure.printStackTrace();
            fail( "Loaded instance does not match the extension class (Exception in stderr)" );
        }

        assertNotNull( "Could not load the kernel extension with the provided key", instance );
        assertSame( "Class of the loaded instance is a subclass of the extension class", instance.getClass(), extClass );
    }

    @Test
    public void differentInstancesShouldHaveEqualHashCodesAndBeEqual()
    {
        ExtensionFactory<?> one = newInstance();
        ExtensionFactory<?> two = newInstance();
        assertEquals( "new instances have different hash codes", one.hashCode(), two.hashCode() );
        assertEquals( "new instances are not equals", one, two );

        one = loadInstance();
        two = loadInstance();
        assertEquals( "loaded instances have different hash codes", one.hashCode(), two.hashCode() );
        assertEquals( "loaded instances are not equals", one, two );

        one = loadInstance();
        two = newInstance();
        assertEquals( "loaded instance and new instance have different hash codes", one.hashCode(), two.hashCode() );
        assertEquals( "loaded instance and new instance are not equals", one, two );
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
