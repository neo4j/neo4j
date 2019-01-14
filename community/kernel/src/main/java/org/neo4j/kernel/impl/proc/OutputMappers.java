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
package org.neo4j.kernel.impl.proc;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Takes user-defined record classes, and does two things: Describe the class as a {@link ProcedureSignature},
 * and provide a mechanism to convert an instance of the class to Neo4j-typed Object[].
 */
public class OutputMappers
{
    public OutputMappers( TypeMappers typeMappers )
    {
        this.typeMappers = typeMappers;
    }

    /**
     * A compiled mapper, takes an instance of a java class, and converts it to an Object[] matching
     * the specified {@link #signature()}.
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

    private static final OutputMapper VOID_MAPPER = new OutputMapper( new FieldSignature[0], new FieldMapper[0] )
    {
        @Override
        public List<FieldSignature> signature()
        {
            return ProcedureSignature.VOID;
        }
    };

    /**
     * Extracts field value from an instance and converts it to a Neo4j typed value.
     */
    private static class FieldMapper
    {
        private final MethodHandle getter;
        private final TypeMappers.TypeChecker checker;

        FieldMapper( MethodHandle getter, TypeMappers.TypeChecker checker )
        {
            this.getter = getter;
            this.checker = checker;
        }

        Object apply( Object record ) throws ProcedureException
        {
            Object invoke = getValue( record );
            return checker.typeCheck( invoke );
        }

        private Object getValue( Object record ) throws ProcedureException
        {
            try
            {
                return getter.invoke( record );
            }
            catch ( Throwable throwable )
            {
                throw new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
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
     * @return an output mapper for the return type of the method.
     * @throws ProcedureException
     */
    public OutputMapper mapper( Method method ) throws ProcedureException
    {
        Class<?> cls = method.getReturnType();
        if ( cls == Void.class || cls == void.class )
        {
            return OutputMappers.VOID_MAPPER;
        }

        if ( cls != Stream.class )
        {
            throw invalidReturnType( cls );
        }

        Type genericReturnType = method.getGenericReturnType();
        if ( !(genericReturnType instanceof ParameterizedType) )
        {
            throw new ProcedureException( Status.Procedure.TypeError,
                    "Procedures must return a Stream of records, where a record is a concrete class%n" +
                            "that you define and not a raw Stream." );
        }

        ParameterizedType genType = (ParameterizedType) genericReturnType;
        Type recordType = genType.getActualTypeArguments()[0];
        if ( recordType instanceof WildcardType )
        {
            throw new ProcedureException( Status.Procedure.TypeError,
                    "Procedures must return a Stream of records, where a record is a concrete class%n" +
                    "that you define and not a Stream<?>." );
        }
        if ( recordType instanceof ParameterizedType )
        {
            ParameterizedType type = (ParameterizedType) recordType;
            throw new ProcedureException( Status.Procedure.TypeError,
                    "Procedures must return a Stream of records, where a record is a concrete class%n" +
                            "that you define and not a parameterized type such as %s.", type );
        }

        return mapper( (Class<?>) recordType );
    }

    public OutputMapper mapper( Class<?> userClass ) throws ProcedureException
    {
        assertIsValidRecordClass( userClass );

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
                TypeMappers.TypeChecker checker = typeMappers.checkerFor( field.getGenericType() );
                MethodHandle getter = lookup.unreflectGetter( field );
                FieldMapper fieldMapper = new FieldMapper( getter, checker );

                fieldMappers[i] = fieldMapper;
                signature[i] = FieldSignature.outputField( field.getName(), checker.type(), field.isAnnotationPresent( Deprecated.class ) );
            }
            catch ( ProcedureException e )
            {
                throw new ProcedureException( e.status(), e,
                        "Field `%s` in record `%s` cannot be converted to a Neo4j type: %s", field.getName(),
                        userClass.getSimpleName(), e.getMessage() );
            }
            catch ( IllegalAccessException e )
            {
                throw new ProcedureException( Status.Procedure.TypeError, e,
                        "Field `%s` in record `%s` cannot be accessed: %s", field.getName(), userClass.getSimpleName(),
                        e.getMessage() );
            }
        }

        return new OutputMapper( signature, fieldMappers );
    }

    private void assertIsValidRecordClass( Class<?> userClass ) throws ProcedureException
    {
        if ( userClass.isPrimitive() || userClass.isArray() ||
                userClass.getPackage() != null && userClass.getPackage().getName().startsWith( "java." ) )
        {
            throw invalidReturnType( userClass );
        }
    }

    private ProcedureException invalidReturnType( Class<?> userClass )
    {
        return new ProcedureException( Status.Procedure.TypeError,
                "Procedures must return a Stream of records, where a record is a concrete class%n" +
                "that you define, with public non-final fields defining the fields in the record.%n" +
                "If you''d like your procedure to return `%s`, you could define a record class like:%n" +
                "public class Output '{'%n" +
                "    public %s out;%n" +
                "'}'%n" +
                "%n" +
                "And then define your procedure as returning `Stream<Output>`.", userClass.getSimpleName(),
                userClass.getSimpleName() );
    }

    private List<Field> instanceFields( Class<?> userClass )
    {
        return Arrays.stream( userClass.getDeclaredFields() )
                .filter( f -> !isStatic( f.getModifiers() ) &&
                                  !f.isSynthetic( ) )
                .collect( toList() );
    }
}
