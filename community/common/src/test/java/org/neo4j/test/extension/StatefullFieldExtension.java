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
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public abstract class StatefullFieldExtension<T> implements TestInstancePostProcessor, AfterAllCallback
{
    protected abstract String getFieldKey();

    protected abstract Class<T> getFieldType();

    protected abstract T createField( ExtensionContext extensionContext );

    protected abstract Namespace getNameSpace();

    @Override
    public void afterAll( ExtensionContext context )
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
            if ( declaredField.isAnnotationPresent( Inject.class ) &&
                    getFieldType().equals( declaredField.getType() ) )
            {
                declaredField.setAccessible( true );
                declaredField.set( testInstance, instance );
            }
        }
    }

    protected T getStoredValue( ExtensionContext context )
    {
        return getLocalStore( context ).get( getFieldKey(), getFieldType() );
    }

    void removeStoredValue( ExtensionContext context )
    {
        getLocalStore( context ).remove( getFieldKey(), getFieldType() );
    }

    static Store getStore( ExtensionContext extensionContext, Namespace namespace )
    {
        return extensionContext.getRoot().getStore( namespace );
    }

    private Store getLocalStore( ExtensionContext extensionContext )
    {
        return getStore( extensionContext, getNameSpace() );
    }

    private Object createInstance( ExtensionContext extensionContext )
    {
        Store store = getLocalStore( extensionContext );
        return store.getOrComputeIfAbsent( getFieldKey(), (Function<String,Object>) s -> createField( extensionContext ) );
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
