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

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.junit.platform.commons.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Resource;

abstract class StatefullFieldExtension<T> implements TestInstancePostProcessor
{
    protected abstract String getFieldKey();

    protected abstract Class<T> getFieldType();

    protected abstract T createField();

    @Override
    public void postProcessTestInstance( Object testInstance, ExtensionContext context ) throws Exception
    {
        Class<?> clazz = testInstance.getClass();
        List<Field> fields =
                ReflectionUtils.findFields( clazz, a -> true, ReflectionUtils.HierarchyTraversalMode.BOTTOM_UP );
        for ( Field declaredField : fields )
        {
            if ( declaredField.isAnnotationPresent( Resource.class ) &&
                    getFieldType().equals( declaredField.getType() ) )
            {
                declaredField.setAccessible( true );
                declaredField.set( testInstance, createFieldInstance( context ) );
            }
        }

    }

    T getStoredValue( ExtensionContext context )
    {
        return getStore( context ).get( getFieldKey(), getFieldType() );
    }

    void removeStoredValue( ExtensionContext context )
    {
        getStore( context ).remove( getFieldKey(), getFieldType() );
    }

    private Object createFieldInstance( ExtensionContext extensionContext )
    {
        ExtensionContext.Store store = getStore( extensionContext );
        return store.getOrComputeIfAbsent( getFieldKey(),
                (Function<String,Object>) s -> createField() );
    }

    ExtensionContext.Store getStore( ExtensionContext extensionContext )
    {
        return extensionContext.getStore( ExtensionContext.Namespace.create( getClass() ) );
    }
}
