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

import org.apache.commons.lang3.ClassUtils;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstances;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.addAll;
import static org.apache.commons.lang3.reflect.FieldUtils.getFieldsListWithAnnotation;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension.TEST_DIRECTORY;
import static org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension.TEST_DIRECTORY_NAMESPACE;

public class DbmsSupportController
{
    private static final String DBMS_KEY = "service";
    private static final String CONTROLLER_KEY = "controller";
    private static final ExtensionContext.Namespace DBMS_NAMESPACE = ExtensionContext.Namespace.create( "org", "neo4j", "dbms" );

    private final ExtensionContext context;
    private final TestInstances testInstances;
    private DatabaseManagementService dbms;
    private String databaseName;

    public DbmsSupportController( ExtensionContext context )
    {
        this.context = context;
        this.testInstances = context.getRequiredTestInstances();
        getStore( context ).put( CONTROLLER_KEY, this );
    }

    public static DbmsSupportController get( ExtensionContext context )
    {
        return getStore( context ).get( CONTROLLER_KEY, DbmsSupportController.class );
    }

    public static DbmsSupportController remove( ExtensionContext context )
    {
        return getStore( context ).remove( CONTROLLER_KEY, DbmsSupportController.class );
    }

    public final void startDbms()
    {
        startDbms( UnaryOperator.identity() );
    }

    public void startDbms( UnaryOperator<TestDatabaseManagementServiceBuilder> callback )
    {
        // Find closest configuration
        TestConfiguration configuration = getConfigurationFromAnnotations(
                getTestAnnotation( DbmsExtension.class ),
                getTestAnnotation( ImpermanentDbmsExtension.class ) );

        // Make service
        buildDbms( configuration.configurationCallback, callback );
        startDatabase( configuration.injectableDatabase );
    }

    public void startDatabase( String databaseName )
    {
        this.databaseName = databaseName;
        if ( !dbms.listDatabases().contains( databaseName ) )
        {
            dbms.createDatabase( databaseName );
        }
        var db = (GraphDatabaseAPI) dbms.database( databaseName );
        var dependencyResolver = db.getDependencyResolver();
        injectDependencies( dependencyResolver );

        // Also inject DbmsController into the test.
        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( asDbmsController() );
        injectDependencies( deps );
    }

    public void stopDatabase()
    {
        dbms.shutdownDatabase( databaseName );
    }

    public void restartDatabase()
    {
        stopDatabase();
        startDatabase( databaseName );
    }

    public TestDirectory getTestDirectory()
    {
        TestDirectory testDir = context.getStore( TEST_DIRECTORY_NAMESPACE ).get( TEST_DIRECTORY, TestDirectory.class );
        if ( testDir == null )
        {
            String tdClassName = TestDirectorySupportExtension.class.getSimpleName();
            String dbClassName = DbmsSupportExtension.class.getSimpleName();
            throw new IllegalStateException( tdClassName + " not in scope, make sure to add it before the relevant " + dbClassName );
        }
        return testDir;
    }

    public  <T extends Annotation> Optional<T> getTestAnnotation( Class<T> annotationType )
    {
        return context.getTestMethod().map( m -> m.getAnnotation( annotationType ) )
                .or( () -> context.getTestClass().map( cls -> cls.getAnnotation( annotationType ) ) );
    }

    public DatabaseManagementService buildDbms( String configurationCallback, UnaryOperator<TestDatabaseManagementServiceBuilder> callback )
    {
        var testDir = getTestDirectory();
        var builder = createBuilder( testDir.homeDir(), testDir.getFileSystem() );
        for ( Object testInstance : testInstances.getAllInstances() )
        {
            maybeInvokeCallback( testInstance, builder, configurationCallback );
        }
        builder = callback.apply( builder );
        dbms = builder.build();
        ExtensionContext.Store store = getStore( context );
        store.put( DBMS_KEY, dbms );
        return dbms;
    }

    public TestDatabaseManagementServiceBuilder createBuilder( File homeDirectory, FileSystemAbstraction fileSystem )
    {
        return new TestDatabaseManagementServiceBuilder( homeDirectory ).setFileSystem( fileSystem );
    }

    public void injectDependencies( DependencyResolver dependencyResolver )
    {
        for ( Object testInstance : testInstances.getAllInstances() )
        {
            var injectableFields = lookupInjectableFields( testInstance );
            injectInstance( testInstance, injectableFields, dependencyResolver );
        }
    }

    public void shutdown()
    {
        var databaseManagementService = getStore( context ).remove( DBMS_KEY, DatabaseManagementService.class );
        databaseManagementService.shutdown();
    }

    public DbmsController asDbmsController()
    {
        return new DbmsController()
        {
            @Override
            public void restartDbms( UnaryOperator<TestDatabaseManagementServiceBuilder> callback )
            {
                shutdown();
                try
                {
                    startDbms( callback );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void restartDatabase()
            {
                restartDbms();
            }
        };
    }

    private static List<Field> lookupInjectableFields( Object testInstance )
    {
        return getFieldsListWithAnnotation( testInstance.getClass(), Inject.class );
    }

    private static void injectInstance( Object testInstance, List<Field> injectables, DependencyResolver dependencies )
    {
        for ( Field injectable : injectables )
        {
            var fieldType = injectable.getType();
            if ( dependencies.resolveTypeDependencies( fieldType ).iterator().hasNext() )
            {
                setField( testInstance, injectable, dependencies.resolveDependency( fieldType ) );
            }
        }
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

    private static void maybeInvokeCallback( Object testInstance, TestDatabaseManagementServiceBuilder builder, String callback )
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
                            TestDatabaseManagementServiceBuilder.class.getSimpleName() + '.' );
                }

                // Make sure we have the required annotation
                if ( declaredMethod.getAnnotation( ExtensionCallback.class ) == null )
                {
                    throw new IllegalArgumentException(
                            "The method '" + callback + "', must be annotated with " + ExtensionCallback.class.getSimpleName() + '.' );
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
        throw new IllegalArgumentException( "The method with name '" + callback + "' cannot be found." );
    }

    private static Iterable<? extends Method> getAllMethods( Class<?> clazz )
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

    private static ExtensionContext.Store getStore( ExtensionContext context )
    {
        return context.getStore( DBMS_NAMESPACE );
    }

    /**
     * We should check for the annotation in terms of locality. If the method is annotated, that configuration should take
     * president over the annotation on class level. This way you can add a global value to the test class, and override
     * configuration values etc. on method level.
     */
    @SafeVarargs
    private static TestConfiguration getConfigurationFromAnnotations( Optional<? extends Annotation>... options )
    {
        Annotation[] annotations = Arrays.stream( options ).flatMap( Optional::stream ).toArray( Annotation[]::new );

        if ( annotations.length > 1 )
        {
            throw new IllegalArgumentException( "Multiple DBMS annotations found for the configuration: " + Arrays.toString( annotations ) + '.' );
        }

        if ( annotations.length == 1 )
        {
            if ( annotations[0] instanceof DbmsExtension )
            {
                DbmsExtension annotation = (DbmsExtension) annotations[0];
                return new TestConfiguration( annotation.injectableDatabase(), annotation.configurationCallback() );
            }
            if ( annotations[0] instanceof ImpermanentDbmsExtension )
            {
                ImpermanentDbmsExtension annotation = (ImpermanentDbmsExtension) annotations[0];
                return new TestConfiguration( annotation.injectableDatabase(), annotation.configurationCallback() );
            }
        }

        // Either we don't recognise the annotation type, or no special configuration was requested.
        // In any case, go with the defaults.
        return new TestConfiguration( DEFAULT_DATABASE_NAME, null );
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
}
