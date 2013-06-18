/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.io.IOException;
import java.lang.reflect.Field;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.impl.StaticLoggerBinder;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * checking the creation of {@link org.neo4j.kernel.InternalAbstractGraphDatabase}'s logging facility
 * since this depends on static methods, we need to mock this using PowerMock.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(StaticLoggerBinder.class)
public class LoggerFactoryIT
{

    private TemporaryFolder graphDbFolder = new TemporaryFolder();

    @Before
    public void setupTempFolder() throws IOException
    {
        graphDbFolder.create();
    }

    @After
    public void cleanupTempFolder()
    {
        graphDbFolder.delete();
    }


    @Test
    public void shouldFallbackToClassicLoggingServiceIfCustomStaticLoggerBinder() throws Exception
    {
        StaticLoggerBinder mockedInstance = PowerMockito.spy( StaticLoggerBinder.getSingleton() );
        PowerMockito.when( mockedInstance.getLoggerFactory() ).thenReturn( new DummyLoggerFactory() );

        PowerMockito.mockStatic( StaticLoggerBinder.class );
        PowerMockito.when( StaticLoggerBinder.getSingleton() ).thenReturn( mockedInstance );

        GraphDatabaseService graphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabase( graphDbFolder
                .getRoot().getAbsolutePath() );
        assertGraphDatabaseLoggingMatches( "org.neo4j.kernel.logging.ClassicLoggingService", graphDatabaseService );

    }

    @Test
    public void shouldUseLogbackServiceWithStandardStaticLoggerBinder() throws Exception
    {
        GraphDatabaseService graphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabase( graphDbFolder
                .getRoot().getAbsolutePath() );
        assertGraphDatabaseLoggingMatches( "org.neo4j.kernel.logging.LogbackService", graphDatabaseService );
    }

    /**
     * helper class to be return upon {@link org.slf4j.impl.StaticLoggerBinder#getLoggerFactory()} in a mocked
     * environment
     */
    static class DummyLoggerFactory implements ILoggerFactory
    {

        @Override
        public Logger getLogger( String name )
        {
            return null;
        }
    }

    private void assertGraphDatabaseLoggingMatches( String expectedLoggingClassname, Object graphDatabaseService )
    {
        assertThat( graphDatabaseService, notNullValue() );
        Object logging = getFieldValueByReflection( graphDatabaseService, "logging" );

        assertThat( logging, notNullValue() );
        assertThat( "gds.logging is not a " + expectedLoggingClassname + " instance", logging.getClass().getName(),
                is( expectedLoggingClassname ) );
    }

    private Object getFieldValueByReflection( Object instance, String fieldName )
    {
        Field field = findFieldRecursively( instance.getClass(), fieldName );
        if ( field == null )
        {
            throw new IllegalArgumentException( "found no field '" + fieldName + "' in class " + instance.getClass()
                    + " or its superclasses." );
        }
        else
        {
            try
            {
                field.setAccessible( true );
                return field.get( instance );
            }
            catch ( IllegalAccessException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private Field findFieldRecursively( Class<? extends Object> clazz, String fieldName )
    {
        try
        {
            return clazz.getDeclaredField( fieldName );
        }
        catch ( NoSuchFieldException e )
        {
            return findFieldRecursively( clazz.getSuperclass(), fieldName );
        }
    }

}
