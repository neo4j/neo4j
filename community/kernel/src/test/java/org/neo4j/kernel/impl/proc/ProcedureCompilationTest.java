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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.api.proc.Key;
import org.neo4j.values.AnyValue;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature;
import static org.neo4j.kernel.impl.proc.ProcedureCompilation.compileFunction;
import static org.neo4j.values.storable.Values.longValue;

public class ProcedureCompilationTest
{
    private static final Key<Long> KEY_1 = Key.key( "long1", Long.class );
    private static final Key<Long> KEY_2 = Key.key( "long2", Long.class );
    private static final AnyValue[] EMPTY = new AnyValue[0];
    private Context ctx;

    @BeforeEach
    void setUp() throws ProcedureException
    {
        ctx = mock(Context.class);
        when(ctx.get( KEY_1 )).thenReturn( 42L );
        when(ctx.get( KEY_2 )).thenReturn( 1337L );
    }

    @Test
    void shouldCallSimpleMethod() throws ProcedureException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" ).out( Neo4jTypes.NTInteger ).build();
        // When
        CallableUserFunction longMethod = compileFunction( signature, emptyList(), method( "longMethod" ) );

        // Then
        assertEquals( longMethod.apply( ctx, EMPTY ), longValue(1337L));
    }

    @Test
    void shouldAccessContext() throws ProcedureException, NoSuchFieldException, IllegalAccessException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" ).out( Neo4jTypes.NTInteger ).build();
        FieldSetter setter1 = createSetter(  InnerClass.class, "field1", KEY_1 );
        FieldSetter setter2 = createSetter(  InnerClass.class, "field2", KEY_2);
        Method longMethod = method( InnerClass.class, "longMethod" );

        // Then
        assertEquals( longValue(0L), compileFunction( signature, emptyList(), longMethod ).apply( ctx, EMPTY ));
        assertEquals( longValue(42L), compileFunction( signature, singletonList( setter1 ), longMethod ).apply( ctx, EMPTY ));
        assertEquals( longValue(1337L), compileFunction( signature, singletonList( setter2 ), longMethod ).apply( ctx, EMPTY ));
        assertEquals( longValue(1379L), compileFunction( signature, asList(setter1, setter2), longMethod ).apply( ctx, EMPTY ));
    }

    @Test
    void shouldHandleThrowingUDF() throws ProcedureException, NoSuchFieldException, IllegalAccessException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" ).out( Neo4jTypes.NTInteger ).build();

        // When
        CallableUserFunction longMethod = compileFunction( signature, emptyList(), method( "throwingLongMethod" ) );

        // Then
        assertThrows( ProcedureException.class, () -> longMethod.apply( ctx, EMPTY ));
    }

    private FieldSetter createSetter(Class<?> owner, String field, Key<Long> key) throws NoSuchFieldException, IllegalAccessException
    {
        Field declaredField = owner.getDeclaredField( field );
        MethodHandle setter = MethodHandles.lookup().unreflectSetter( declaredField );
        return new FieldSetter( declaredField, setter,
                (ComponentRegistry.Provider<Long>) context -> context.get( key) );
    }

    private Method method( String name, Class<?>...types )
    {
       return method( this.getClass(), name, types );
    }

    private Method method( Class<?> owner, String name, Class<?>...types )
    {
        try
        {
            return owner.getMethod( name, types );
        }
        catch ( NoSuchMethodException e )
        {
            throw new AssertionError( e );
        }
    }


    public long longMethod()
    {
        return 1337L;
    }

    public long throwingLongMethod()
    {
        throw new RuntimeException( "wut!" );
    }

    public static class InnerClass
    {
        public long field1;
        public long field2;
        public InnerClass()
        {
            System.out.println("init");
        }

        public long longMethod()
        {
            return field1 + field2;
        }

    }
}