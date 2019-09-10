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

import org.apache.commons.lang3.ClassUtils;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.TestInstances;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Arrays.stream;
import static java.util.Collections.addAll;
import static org.junit.platform.commons.support.AnnotationSupport.isAnnotated;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class DbmsSupportExtension implements AfterEachCallback, BeforeEachCallback
{
    protected static final String DBMS = "service";
    private static final Namespace DBMS_NAMESPACE = Namespace.create( "org", "neo4j", "dbms" );

    @Override
    public void beforeEach( ExtensionContext context )
    {
        TestInstances testInstances = context.getRequiredTestInstances();
        TestDirectory testDir = getTestDirectory( context );

        // Find closest configuration
        TestConfiguration configuration = getConfigurationFromAnnotations( context );

        // Make service
        TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder( testDir.homeDir() ).setFileSystem( testDir.getFileSystem() );
        for ( Object testInstance : testInstances.getAllInstances() )
        {
            maybeInvokeCallback( testInstance, builder, configuration.configurationCallback );
        }
        DatabaseManagementService dbms = builder.build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( configuration.injectableDatabase );

        // Save in context
        Store store = getStore( context );
        store.put( DBMS, dbms );

        // Inject
        for ( Object testInstance : testInstances.getAllInstances() )
        {
            injectInstance( testInstance, dbms, DatabaseManagementService.class );
            injectInstance( testInstance, db, GraphDatabaseService.class );
            injectInstance( testInstance, db, GraphDatabaseAPI.class );
        }
    }

    @Override
    public void afterEach( ExtensionContext context )
    {
        DatabaseManagementService dbms = getStore( context ).remove( DBMS, DatabaseManagementService.class );
        dbms.shutdown();
    }

    protected static <T> void injectInstance( Object testInstance, T instance, Class<T> clazz )
    {
        Class<?> testClass = testInstance.getClass();
        do
        {
            stream( testClass.getDeclaredFields() )
                    .filter( field -> isAnnotated( field, Inject.class ) )
                    .filter( field -> field.getType() == clazz )
                    .forEach( field -> setField( testInstance, field, instance ) );
            testClass = testClass.getSuperclass();
        }
        while ( testClass != null );
    }

    private static void setField( Object testInstance, Field field, Object db )
    {
        field.setAccessible( true );
        try
        {
            field.set( testInstance, db );
        }
        catch ( IllegalAccessException e )
        {
            throw new RuntimeException( e );
        }
    }

    protected TestDirectory getTestDirectory( ExtensionContext context )
    {
        TestDirectory testDir = context.getStore( TestDirectorySupportExtension.TEST_DIRECTORY_NAMESPACE )
                .get( TestDirectorySupportExtension.TEST_DIRECTORY, TestDirectory.class );
        if ( testDir == null )
        {
            throw new IllegalStateException(
                    TestDirectorySupportExtension.class.getSimpleName() + " not in scope, make sure to add it before the " + getClass().getSimpleName() );
        }
        return testDir;
    }

    protected static void maybeInvokeCallback( Object testInstance, TestDatabaseManagementServiceBuilder builder, String callback )
    {
        if ( callback == null || callback.isEmpty() )
        {
            return; // Callback disabled
        }

        for ( Method declaredMethod : getAllMethods( testInstance.getClass() ) )
        {
            if ( declaredMethod.getName().equals( callback ) )
            {
                // Make sure it returns void
                if ( declaredMethod.getReturnType() != Void.TYPE )
                {
                    throw new IllegalArgumentException( "The method '" + callback + "', must return void." );
                }

                // Make sure we have compatible parameters
                Class<?>[] parameterTypes = declaredMethod.getParameterTypes();
                if ( parameterTypes.length != 1 || !parameterTypes[0].isAssignableFrom( TestDatabaseManagementServiceBuilder.class ) )
                {
                    throw new IllegalArgumentException( "The method '" + callback + "', must take one parameter that is assignable from " +
                            TestDatabaseManagementServiceBuilder.class.getSimpleName() + "." );
                }

                // Make sure we have the required annotation
                if ( declaredMethod.getAnnotation( ExtensionCallback.class ) == null )
                {
                    throw new IllegalArgumentException(
                            "The method '" + callback + "', must be annotated with " + ExtensionCallback.class.getSimpleName() + "." );
                }

                // All match, try calling it
                declaredMethod.setAccessible( true );
                try
                {
                    declaredMethod.invoke( testInstance, builder );
                }
                catch ( IllegalAccessException e )
                {
                    throw new IllegalArgumentException( "The method '" + callback + "' is not accessible.", e );
                }
                catch ( InvocationTargetException e )
                {
                    throw new RuntimeException( "The method '" + callback + "' threw an exception.", e );
                }

                // All done
                return;
            }
        }

        // No method matching the provided name
        throw new IllegalArgumentException( "The method with name '" + callback + "' can not be found." );
    }

    private static Iterable<? extends Method> getAllMethods( Class clazz )
    {
        List<Method> methods = new ArrayList<>();
        addAll( methods, clazz.getDeclaredMethods() );
        var classes = ClassUtils.getAllSuperclasses( clazz );
        for ( var aClass : classes )
        {
            addAll( methods, aClass.getDeclaredMethods() );
        }
        return methods;
    }

    protected static Store getStore( ExtensionContext context )
    {
        return context.getStore( DBMS_NAMESPACE );
    }

    /**
     * Since annotations can't be extended we use this internal class to represent the annotated values
     */
    private static class TestConfiguration
    {
        private final String injectableDatabase;
        private final String configurationCallback;

        private TestConfiguration( String injectableDatabase, String configurationCallback )
        {
            this.injectableDatabase = injectableDatabase;
            this.configurationCallback = configurationCallback;
        }
    }

    /**
     * We should check for the annotation in terms of locality. If the method is annotated, that configuration should take
     * president over the annotation on class level. This way you can add a global value to the test class, and override
     * configuration values etc. on method level.
     */
    private static TestConfiguration getConfigurationFromAnnotations( ExtensionContext context )
    {
        // Try test method
        List<DbmsExtension> dbmsExtensions = new ArrayList<>();
        List<ImpermanentDbmsExtension> impermanentDbmsExtensions = new ArrayList<>();
        Method requiredTestMethod = context.getRequiredTestMethod();
        DbmsExtension dbmsExtension = requiredTestMethod.getAnnotation( DbmsExtension.class );
        ImpermanentDbmsExtension impermanentDbmsExtension = requiredTestMethod.getAnnotation( ImpermanentDbmsExtension.class );
        if ( dbmsExtension != null )
        {
            dbmsExtensions.add( dbmsExtension );
        }
        if ( impermanentDbmsExtension != null )
        {
            impermanentDbmsExtensions.add( impermanentDbmsExtension );
        }

        // Try test class
        Class<?> testClass = context.getRequiredTestClass();
        dbmsExtension = testClass.getAnnotation( DbmsExtension.class );
        if ( dbmsExtension != null )
        {
            dbmsExtensions.add( dbmsExtension );
        }
        impermanentDbmsExtension = testClass.getAnnotation( ImpermanentDbmsExtension.class );
        if ( impermanentDbmsExtension != null )
        {
            impermanentDbmsExtensions.add( impermanentDbmsExtension );
        }

        // Make sure we don't mix annotations
        if ( !dbmsExtensions.isEmpty() && !impermanentDbmsExtensions.isEmpty() )
        {
            throw new IllegalArgumentException( String.format( "Mix of %s and %s found, this is not supported", DbmsExtension.class.getSimpleName(),
                    ImpermanentDbmsExtension.class.getSimpleName() ) );
        }

        // Get first one, they are added in order of locality
        if ( !dbmsExtensions.isEmpty() )
        {
            dbmsExtension = dbmsExtensions.get( 0 );
            return new TestConfiguration( dbmsExtension.injectableDatabase(), dbmsExtension.configurationCallback() );
        }
        if ( !impermanentDbmsExtensions.isEmpty() )
        {
            impermanentDbmsExtension = impermanentDbmsExtensions.get( 0 );
            return new TestConfiguration( impermanentDbmsExtension.injectableDatabase(), impermanentDbmsExtension.configurationCallback() );
        }

        // Nothing found, default values
        return new TestConfiguration( DEFAULT_DATABASE_NAME, null );
    }

}
