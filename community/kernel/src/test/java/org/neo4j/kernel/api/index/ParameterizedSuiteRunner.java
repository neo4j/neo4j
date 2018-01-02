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
package org.neo4j.kernel.api.index;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.Runner;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class ParameterizedSuiteRunner extends Suite
{
    @SuppressWarnings("unused"/*used by jUnit through reflection*/)
    public ParameterizedSuiteRunner( Class<?> testClass ) throws InitializationError
    {
        this( testClass, new ParameterBuilder( testClass ) );
    }

    ParameterizedSuiteRunner( Class<?> testClass, ParameterBuilder builder ) throws InitializationError
    {
        super( builder, testClass, builder.suiteClasses() );
    }

    private static class ParameterBuilder extends RunnerBuilder
    {
        private final Map<Class<?>, Parameterization> parameterizations = new HashMap<Class<?>, Parameterization>();
        private final Class<?> suiteClass;

        ParameterBuilder( Class<?> suiteClass ) throws InitializationError
        {
            this.suiteClass = suiteClass;
            boolean ok = false;
            for ( Constructor<?> suiteConstructor : suiteClass.getConstructors() )
            {
                if ( suiteConstructor.getParameterTypes().length == 0 )
                {
                    if ( Modifier.isPublic( suiteConstructor.getModifiers() ) )
                    {
                        ok = true;
                    }
                    break;
                }
            }
            List<Throwable> errors = new ArrayList<Throwable>();
            if ( !ok )
            {
                errors.add( new IllegalArgumentException( "Suite class (" + suiteClass.getName() +
                        ") does not have a public zero-arg constructor." ) );
            }
            if ( Modifier.isAbstract( suiteClass.getModifiers() ) )
            {
                errors.add(
                        new IllegalArgumentException( "Suite class (" + suiteClass.getName() + ") is abstract." ) );
            }
            buildParameterizations( parameterizations, suiteClass, errors );
            if ( !errors.isEmpty() )
            {
                throw new InitializationError( errors );
            }
        }

        @Override
        public Runner runnerForClass( Class<?> testClass ) throws Throwable
        {
            if ( testClass == this.suiteClass )
            {
                return new BlockJUnit4ClassRunner( testClass );
            }
            return parameterizations.get( testClass );
        }

        Class<?>[] suiteClasses()
        {
            ArrayList<Class<?>> classes = new ArrayList<Class<?>>( parameterizations.keySet() );
            for ( Method method : suiteClass.getMethods() )
            {
                if ( method.getAnnotation( Test.class ) != null )
                {
                    classes.add( suiteClass );
                }
            }
            return classes.toArray( new Class[classes.size()] );
        }

        private void buildParameterizations( Map<Class<?>, Parameterization> result, Class<?> type,
                                             List<Throwable> errors )
        {
            if ( type == Object.class )
            {
                return;
            }
            buildParameterizations( result, type.getSuperclass(), errors );
            SuiteClasses annotation = type.getAnnotation( SuiteClasses.class );
            if ( annotation != null )
            {
                for ( Class<?> test : annotation.value() )
                {
                    if ( !result.containsKey( test ) )
                    {
                        try
                        {
                            result.put( test, new Parameterization( this, test.getConstructor( type ) ) );
                        }
                        catch ( InitializationError failure )
                        {
                            errors.addAll( failure.getCauses() );
                        }
                        catch ( NoSuchMethodException e )
                        {
                            errors.add( e );
                        }
                    }
                }
            }
        }

        Object newSuiteInstance() throws Exception
        {
            return suiteClass.newInstance();
        }
    }

    private static class Parameterization extends BlockJUnit4ClassRunner
    {
        private final ParameterBuilder builder;
        private final Constructor<?> constructor;

        Parameterization( ParameterBuilder builder, Constructor<?> constructor ) throws InitializationError
        {
            super( constructor.getDeclaringClass() );
            this.builder = builder;
            this.constructor = constructor;
        }

        @Override
        protected void validateConstructor( List<Throwable> errors )
        {
            // constructor is already verified
        }

        @Override
        protected Object createTest() throws Exception
        {
            return constructor.newInstance( builder.newSuiteInstance() );
        }
    }
}
