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
package org.neo4j.test.extension.actors;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Optional;

import org.neo4j.test.extension.Inject;

import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.test.ReflectionUtil.getAllFields;

public class ActorsSupportExtension implements TestInstancePostProcessor, AfterEachCallback, AfterAllCallback
{
    private static final Namespace NAMESPACE = Namespace.create( "neo4j", "actors" );
    private static final String ACTOR_MANAGER = "ACTOR_MANAGER";

    @Override
    public void afterAll( ExtensionContext context ) throws Exception
    {
        if ( getLifecycle( context ) == PER_CLASS )
        {
            tearDownActors( context );
        }
    }

    @Override
    public void afterEach( ExtensionContext context ) throws Exception
    {
        if ( getLifecycle( context ) == PER_METHOD )
        {
            tearDownActors( context );
        }
    }

    private TestInstance.Lifecycle getLifecycle( ExtensionContext context )
    {
        return context.getTestInstanceLifecycle().orElse( PER_METHOD );
    }

    private void tearDownActors( ExtensionContext context ) throws Exception
    {
        Optional<ExtensionContext> current = Optional.of( context );
        while ( current.isPresent() )
        {
            ExtensionContext ctx = current.get();
            ActorsManager manager = (ActorsManager) getStore( ctx ).remove( ACTOR_MANAGER );
            if ( manager != null )
            {
                manager.stopAllActors();
            }
            current = ctx.getParent();
        }
    }

    @Override
    public void postProcessTestInstance( Object testInstance, ExtensionContext extensionContext ) throws Exception
    {
        ActorsManager manager = (ActorsManager) getStore( extensionContext ).getOrComputeIfAbsent( ACTOR_MANAGER,
                k -> new ActorsManager( extensionContext.getDisplayName() ) );
        Class<?> clazz = testInstance.getClass();
        List<Field> declaredFields = getAllFields( clazz );
        for ( Field declaredField : declaredFields )
        {
            if ( declaredField.getType() == Actor.class && declaredField.isAnnotationPresent( Inject.class ) )
            {
                if ( Modifier.isStatic( declaredField.getModifiers() ) )
                {
                    throw new ExtensionConfigurationException(
                            format( "Actors cannot be inject into static field: %s.%s.", clazz.getName(), declaredField.getName() ) );
                }
                declaredField.setAccessible( true );
                if ( declaredField.get( testInstance ) != null )
                {
                    throw new ExtensionConfigurationException(
                            format( "Field %s that is marked for injection in class %s is managed by extension container " +
                                    "and should not have any manually assigned value.", declaredField.getName(), clazz.getName() ) );
                }
                declaredField.set( testInstance, manager.createActor( declaredField.getName() ) );
            }
        }
    }

    private ExtensionContext.Store getStore( ExtensionContext extensionContext )
    {
        return extensionContext.getStore( NAMESPACE );
    }
}
