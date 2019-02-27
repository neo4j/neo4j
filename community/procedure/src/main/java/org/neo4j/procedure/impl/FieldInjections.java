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
package org.neo4j.procedure.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.ComponentInjectionException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.procedure.Context;

import static java.lang.reflect.Modifier.isPublic;

/**
 * Injects annotated fields with appropriate values.
 */
class FieldInjections
{
    private final ComponentRegistry components;

    FieldInjections( ComponentRegistry components )
    {
        this.components = components;
    }

    /**
     * For each annotated field in the provided class, creates a `FieldSetter`.
     * @param cls The class where injection should happen.
     * @return A list of `FieldSetters`
     * @throws ProcedureException if the type of the injected field does not match what has been registered.
     */
    List<FieldSetter> setters( Class<?> cls ) throws ProcedureException
    {
        List<FieldSetter> setters = new LinkedList<>();
        Class<?> currentClass = cls;

        do
        {
            for ( Field field : currentClass.getDeclaredFields() )
            {
                //ignore synthetic fields
                if ( field.isSynthetic() )
                {
                    continue;
                }
                if ( Modifier.isStatic( field.getModifiers() ) )
                {
                    if ( field.isAnnotationPresent( Context.class ) )
                    {
                        throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                                 "The field `%s` in the class named `%s` is annotated as a @Context field,%n" +
                                "but it is static. @Context fields must be public, non-final and non-static,%n" +
                                "because they are reset each time a procedure is invoked.",
                                field.getName(), cls.getSimpleName() );
                    }
                    continue;
                }

                assertValidForInjection( cls, field );
                setters.add( createInjector( cls, field ) );
            }
        }
        while ( (currentClass = currentClass.getSuperclass()) != null );

        return setters;
    }

    private FieldSetter createInjector( Class<?> cls, Field field ) throws ProcedureException
    {

        ThrowingFunction<org.neo4j.kernel.api.procedure.Context,?,ProcedureException> provider = components.providerFor( field.getType() );
        if ( provider == null )
        {
            throw new ComponentInjectionException( Status.Procedure.ProcedureRegistrationFailed,
                    "Unable to set up injection for procedure `%s`, the field `%s` " +
                    "has type `%s` which is not a known injectable component.",
                    cls.getSimpleName(), field.getName(), field.getType() );
        }
        if ( !isPublic( field.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Unable to set up injection for `%s`, failed to access field `%s", cls.getSimpleName(),
                    field.getName() );
        }

        return new FieldSetter( field, provider );
    }

    private void assertValidForInjection( Class<?> cls, Field field ) throws ProcedureException
    {
        if ( !field.isAnnotationPresent( Context.class ) )
        {
            throw new ProcedureException(  Status.Procedure.ProcedureRegistrationFailed,
                    "Field `%s` on `%s` is not annotated as a @" + Context.class.getSimpleName() +
                            " and is not static. If you want to store state along with your procedure," +
                            " please use a static field.",
                    field.getName(), cls.getSimpleName() );
        }

        if ( !isPublic( field.getModifiers() ) || Modifier.isFinal( field.getModifiers() ) )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed,
                    "Field `%s` on `%s` must be non-final and public.", field.getName(), cls.getSimpleName() );

        }
    }
}
