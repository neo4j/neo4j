/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.proc;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.ProcedureSignature.FieldSignature;

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Takes user-defined record classes, and does two things: Describe the class as a {@link ProcedureSignature}, and provide a mechanism to convert
 * an instance of the class to Neo4j-typed Object[].
 */
public class OutputMappers
{
    public OutputMappers( TypeMappers typeMappers )
    {
        this.typeMappers = typeMappers;
    }

    /**
     * A compiled mapper, takes an instance of a java class, and converts it to an Object[] matching the specified {@link #signature()}.
     */
    public static class OutputMapper
    {
        private final List<FieldSignature> signature;
        private final FieldMapper[] fieldMappers;

        public OutputMapper( FieldSignature[] signature, FieldMapper[] fieldMappers )
        {
            this.signature = asList( signature );
            this.fieldMappers = fieldMappers;
        }

        public List<FieldSignature> signature()
        {
            return signature;
        }

        public Object[] apply( Object record ) throws ProcedureException
        {
            Object[] output = new Object[fieldMappers.length];
            for ( int i = 0; i < fieldMappers.length; i++ )
            {
                output[i] = fieldMappers[i].apply( record );
            }
            return output;
        }
    }

    /**
     * Extracts field value from an instance and converts it to a Neo4j typed value.
     */
    private static class FieldMapper
    {
        private final MethodHandle getter;
        private final TypeMappers.NeoValueConverter mapper;

        public FieldMapper( MethodHandle getter, TypeMappers.NeoValueConverter mapper )
        {
            this.getter = getter;
            this.mapper = mapper;
        }

        Object apply( Object record ) throws ProcedureException
        {
            Object invoke = getValue( record );
            return mapper.toNeoValue( invoke );
        }

        private Object getValue( Object record ) throws ProcedureException
        {
            try
            {
                return getter.invoke( record );
            }
            catch ( Throwable throwable )
            {
                throw new ProcedureException( Status.Procedure.CallFailed, throwable,
                        "Unable to read value from record `%s`: %s", record, throwable.getMessage() );
            }
        }
    }

    private final Lookup lookup = MethodHandles.lookup();
    private final TypeMappers typeMappers;

    /**
     * Build an output mapper for the return type of a given method.
     *
     * @param method the procedure method
     * @return an outputmapper for the return type of the method.
     * @throws ProcedureException
     */
    public OutputMapper mapper( Method method ) throws ProcedureException
    {
        Class<?> cls = method.getReturnType();
        if ( cls != Stream.class )
        {
            throw new ProcedureException( Status.Procedure.FailedRegistration,
                    "A procedure must return a `java.util.stream.Stream`, `%s.%s` returns `%s`.",
                    method.getDeclaringClass().getSimpleName(), method.getName(), cls.getSimpleName() );
        }

        ParameterizedType genType = (ParameterizedType) method.getGenericReturnType();
        Type recordType = genType.getActualTypeArguments()[0];

        return mapper( (Class<?>) recordType );
    }

    public OutputMapper mapper( Class<?> userClass ) throws ProcedureException
    {
        List<Field> fields = instanceFields( userClass );
        FieldSignature[] signature = new FieldSignature[fields.size()];
        FieldMapper[] fieldMappers = new FieldMapper[fields.size()];

        for ( int i = 0; i < fields.size(); i++ )
        {
            Field field = fields.get( i );
            if ( !isPublic( field.getModifiers() ) )
            {
                throw new ProcedureException( Status.Procedure.TypeError,
                        "Field `%s` in record `%s` cannot be accessed. Please ensure the field is marked as `public`.", field.getName(),
                        userClass.getSimpleName() );
            }

            try
            {
                TypeMappers.NeoValueConverter mapper = typeMappers.converterFor( field.getGenericType() );
                MethodHandle getter = lookup.unreflectGetter( field );
                FieldMapper fieldMapper = new FieldMapper( getter, mapper );

                fieldMappers[i] = fieldMapper;
                signature[i] = new FieldSignature( field.getName(), mapper.type() );
            }
            catch ( ProcedureException e )
            {
                throw new ProcedureException( e.status(), e, "Field `%s` in record `%s` cannot be converted to a Neo4j type: %s", field.getName(),
                        userClass.getSimpleName(), e.getMessage() );
            }
            catch ( IllegalAccessException e )
            {
                throw new ProcedureException( Status.Procedure.TypeError, e, "Field `%s` in record `%s` cannot be accessed: %s", field.getName(),
                        userClass.getSimpleName(), e.getMessage() );
            }
        }

        return new OutputMapper( signature, fieldMappers );
    }

    private List<Field> instanceFields( Class<?> userClass )
    {
        return asList( userClass.getDeclaredFields() ).stream().filter( ( f ) -> !isStatic( f.getModifiers() ) ).collect( toList() );
    }
}
