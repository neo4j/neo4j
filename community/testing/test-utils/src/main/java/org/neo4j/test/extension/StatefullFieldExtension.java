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

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;

public abstract class StatefullFieldExtension<T> implements TestInstancePostProcessor, AfterAllCallback
{
    protected abstract String getFieldKey();

    protected abstract Class<T> getFieldType();

    protected abstract T createField( ExtensionContext extensionContext );

    protected abstract Namespace getNameSpace();

    @Override
    public void afterAll( ExtensionContext context ) throws Exception
    {
        removeStoredValue( context );
    }

    @Override
    public void postProcessTestInstance( Object testInstance, ExtensionContext context ) throws Exception
    {
        Class<?> clazz = testInstance.getClass();
        Object instance = createInstance( context );
        List<Field> declaredFields = getAllFields( clazz );
        for ( Field declaredField : declaredFields )
        {
            if ( declaredField.isAnnotationPresent( Inject.class ) && declaredField.getType().isAssignableFrom( getFieldType() ) )
            {
                declaredField.setAccessible( true );
                if ( declaredField.get( testInstance ) != null )
                {
                    throw new ExtensionConfigurationException(
                            format( "Field %s that is marked for injection in class %s is managed by extension container " +
                                            "and should not have any manually assigned value.", declaredField.getName(), clazz.getName() ) );
                }
                declaredField.set( testInstance, instance );
            }
        }
    }

    protected T getStoredValue( ExtensionContext context )
    {
        return getLocalStore( context ).get( getFieldKey(), getFieldType() );
    }

    protected T removeStoredValue( ExtensionContext context )
    {
        return getLocalStore( context ).remove( getFieldKey(), getFieldType() );
    }

    protected T deepRemoveStoredValue( ExtensionContext context )
    {
        T removedValue = null;
        ExtensionContext valueContext = context;
        while ( removedValue == null && valueContext != null )
        {
            removedValue = removeStoredValue( valueContext );
            valueContext = valueContext.getParent().orElse( null );
        }
        return removedValue;
    }

    protected static Store getStore( ExtensionContext extensionContext, Namespace namespace )
    {
        return extensionContext.getStore( namespace );
    }

    private Store getLocalStore( ExtensionContext extensionContext )
    {
        return getStore( extensionContext, getNameSpace() );
    }

    private Object createInstance( ExtensionContext extensionContext )
    {
        Object value = getStoredValue( extensionContext );
        if ( value == null )
        {
            value = createField( extensionContext );
            getLocalStore( extensionContext ).put( getFieldKey(), value );
        }
        return value;
    }

    private static List<Field> getAllFields( Class<?> baseClazz )
    {
        ArrayList<Field> fields = new ArrayList<>();
        Class<?> clazz = baseClazz;
        do
        {
            Collections.addAll( fields, clazz.getDeclaredFields() );
            clazz = clazz.getSuperclass();
        }
        while ( clazz != null );
        return fields;
    }
}
