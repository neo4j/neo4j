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
import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTAny;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTBoolean;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTByteArray;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTFloat;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNumber;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature;
import static org.neo4j.kernel.impl.proc.ProcedureCompilation.compileFunction;
import static org.neo4j.kernel.impl.proc.ProcedureCompilation.compileProcedure;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.PI;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.booleanValue;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.byteValue;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.list;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class ProcedureCompilationTest
{
    private static final AnyValue[] EMPTY = new AnyValue[0];
    private static final DefaultValueMapper VALUE_MAPPER = new DefaultValueMapper( mock( EmbeddedProxySPI.class ) );
    private static final KernelTransaction TRANSACTION = mock( KernelTransaction.class );
    public static final ResourceTracker RESOURCE_TRACKER = mock( ResourceTracker.class );

    private Context ctx;

    @BeforeEach
    void setUp() throws ProcedureException
    {
        ctx = mock( Context.class );
        when( ctx.thread() ).thenReturn( Thread.currentThread() );
        when( ctx.kernelTransaction()).thenReturn( TRANSACTION );
        when( ctx.valueMapper() ).thenReturn( VALUE_MAPPER );
        when( TRANSACTION.toString() ).thenReturn( "I'm transaction" );
    }

    @Test
    void shouldCallSimpleMethod() throws ProcedureException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" ).out( Neo4jTypes.NTInteger ).build();
        // When
        CallableUserFunction longMethod =
                compileFunction( signature, emptyList(), method( "longMethod" ) );

        // Then
        assertEquals( longMethod.apply( ctx, EMPTY ), longValue( 1337L ) );
    }

    @Test
    void shouldExposeUserFunctionSignature() throws ProcedureException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" ).out( Neo4jTypes.NTInteger ).build();
        // When
        CallableUserFunction function = compileFunction( signature, emptyList(), method( "longMethod" ) );

        // Then
        assertEquals( function.signature(), signature );
    }

    @Test
    void functionShouldAccessContext() throws ProcedureException, NoSuchFieldException, IllegalAccessException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" ).out( Neo4jTypes.NTInteger ).build();
        FieldSetter setter1 = createSetter( InnerClass.class, "transaction", Context::kernelTransaction );
        FieldSetter setter2 = createSetter( InnerClass.class, "thread", Context::thread );
        Method longMethod = method( InnerClass.class, "stringMethod" );

        // Then
        String threadName = Thread.currentThread().getName();
        assertEquals( stringValue( "NULL AND NULL" ),
                compileFunction( signature, emptyList(), longMethod ).apply( ctx, EMPTY ) );
        assertEquals( stringValue( "I'm transaction AND NULL" ),
                compileFunction( signature, singletonList( setter1 ), longMethod ).apply( ctx, EMPTY ) );
        assertEquals( stringValue( "NULL AND " + threadName ),
                compileFunction( signature, singletonList( setter2 ), longMethod ).apply( ctx, EMPTY ) );
        assertEquals( stringValue( "I'm transaction AND " + threadName ),
                compileFunction( signature, asList( setter1, setter2 ), longMethod ).apply( ctx, EMPTY ) );
    }

    @Test
    void shouldHandleThrowingUDF() throws ProcedureException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" ).out( Neo4jTypes.NTInteger ).build();

        // When
        CallableUserFunction longMethod = compileFunction( signature, emptyList(), method( "throwingLongMethod" ) );

        // Then
        assertThrows( ProcedureException.class, () -> longMethod.apply( ctx, EMPTY ) );
    }

    @Test
    void shouldCallMethodWithParameters() throws ProcedureException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" )
                .in( "l", Neo4jTypes.NTInteger )
                .in( "d", NTFloat )
                .in( "b", NTBoolean )
                .out( NTString ).build();

        // When
        CallableUserFunction concatMethod = compileFunction( signature, emptyList(),
                method( "concat", long.class, Double.class, boolean.class ) );

        // Then
        assertEquals(
                stringValue( "421.1true" ),
                concatMethod.apply( ctx, new AnyValue[]{longValue( 42 ), doubleValue( 1.1 ), booleanValue( true )} ) );
    }

    @Test
    void shouldCallMethodWithCompositeParameters() throws ProcedureException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" )
                .in( "l", NTList( NTAny ) )
                .out( NTString ).build();

        // When
        CallableUserFunction concatMethod = compileFunction( signature, emptyList(),
                method( "concat", List.class ) );

        // Then
        assertEquals( stringValue( "421.1true" ),
                concatMethod
                        .apply( ctx,
                                new AnyValue[]{list( longValue( 42 ), doubleValue( 1.1 ), TRUE )} ) );
    }

    @Test
    void shouldHandleNulls() throws ProcedureException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" )
                .in( "b", NTBoolean )
                .out( NTFloat ).build();

        // When
        CallableUserFunction nullyMethod = compileFunction( signature, emptyList(),
                method( "nullyMethod", Boolean.class ) );

        // Then
        assertEquals( Values.NO_VALUE,
                nullyMethod.apply( ctx, new AnyValue[]{TRUE} ) );
        assertEquals( PI,
                nullyMethod.apply( ctx, new AnyValue[]{Values.NO_VALUE} ) );
    }

    @Test
    void shouldHandleNumberOutput() throws ProcedureException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" )
                .in( "numbers", NTList( NTNumber ) )
                .out( NTNumber ).build();

        // When
        CallableUserFunction sumMethod = compileFunction( signature, emptyList(),
                method( "sum", List.class ) );

        // Then
        assertEquals( longValue( 3 ),
                sumMethod.apply( ctx, new AnyValue[]{list( longValue( 1 ), longValue( 2 ) )} ) );
    }

    @Test
    void shouldHandleByteArrays() throws ProcedureException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" )
                .in( "bytes", NTByteArray )
                .out( NTByteArray ).build();

        // When
        CallableUserFunction bytesMethod =
                compileFunction( signature, emptyList(), method( "testMethod", byte[].class ) );

        // Then
        assertEquals( byteArray( new byte[]{1, 2, 3} ),
                bytesMethod.apply( ctx, new AnyValue[]{byteArray( new byte[]{1, 2, 3} )} ) );
        assertEquals( byteArray( new byte[]{1, 2, 3} ),
                bytesMethod.apply( ctx, new AnyValue[]{list( byteValue( (byte) 1 ), byteValue( (byte) 2 ), byteValue(
                        (byte) 3 ) )} ) );
        assertEquals( NO_VALUE,
                bytesMethod.apply( ctx, new AnyValue[]{NO_VALUE} ) );
    }

    @Test
    void shouldHandleStrings() throws ProcedureException
    {
        // Given
        UserFunctionSignature signature = functionSignature( "test", "foo" )
                .in( "string", NTString )
                .out( NTString ).build();

        // When
        CallableUserFunction stringMethod =
                compileFunction( signature, emptyList(), method( "testMethod", String.class ) );

        // Then
        assertEquals( stringValue( "good" ),
                stringMethod.apply( ctx, new AnyValue[]{stringValue( "good" )} ) );
        assertEquals(NO_VALUE,
                stringMethod.apply( ctx, new AnyValue[]{NO_VALUE} ) );
    }

    @Test
    void shouldHandleAllTypes() throws ProcedureException
    {
        Map<Class<?>,Method> allTypes = typeMaps();
        UserFunctionSignature signature = functionSignature( "test", "foo" ).in( "in", NTAny  ).out( NTAny  ).build();

        for ( Entry<Class<?>,Method> entry : allTypes.entrySet() )
        {

            CallableUserFunction function = compileFunction( signature, emptyList(), entry.getValue() );
            Class<?> type = entry.getKey();

            if ( type.equals( long.class ) )
            {
                assertEquals( longValue( 1337L ), function.apply( ctx, new AnyValue[]{longValue( 1337L )} ) );
            }
            else if ( type.equals( double.class ) )
            {
                assertEquals( PI, function.apply( ctx, new AnyValue[]{PI} ) );
            }
            else if ( type.equals( boolean.class ) )
            {
                assertEquals( TRUE, function.apply( ctx, new AnyValue[]{TRUE} ) );
            }
            else
            {
                assertEquals( NO_VALUE, function.apply( ctx, new AnyValue[]{NO_VALUE} ) );
            }
        }
    }

    @Test
    void shouldCallSimpleProcedure() throws ProcedureException
    {
        // Given
        ProcedureSignature signature = ProcedureSignature.procedureSignature(  "test", "foo" )
                .in( "in", NTInteger )
                .out( singletonList( inputField( "name", NTInteger ) ) ).build();
        // When
        CallableProcedure longStream =
                compileProcedure( signature, emptyList(), method( "longStream", long.class ) );

        // Then
        RawIterator<AnyValue[],ProcedureException> iterator =
                longStream.apply( ctx, new AnyValue[]{longValue( 1337L )}, RESOURCE_TRACKER );
        assertArrayEquals( new AnyValue[]{longValue( 1337L )}, iterator.next() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldExposeProcedureSignature() throws ProcedureException
    {
        // Given
        ProcedureSignature signature = ProcedureSignature.procedureSignature(  "test", "foo" )
                .in( "in", NTInteger )
                .out( singletonList( inputField( "name", NTInteger ) ) ).build();
        // When
        CallableProcedure longStream =
                compileProcedure( signature, emptyList(), method( "longStream", long.class ) );

        // Then
       assertEquals( signature, longStream.signature() );
    }

    @Test
    void procedureShouldAccessContext() throws ProcedureException, NoSuchFieldException, IllegalAccessException
    {
        // Given
        ProcedureSignature signature = ProcedureSignature.procedureSignature(  "test", "foo" )
                .in( "in", NTString )
                .out( singletonList( inputField( "name", NTString ) ) ).build();
        FieldSetter setter1 = createSetter( InnerClass.class, "transaction", Context::kernelTransaction );
        FieldSetter setter2 = createSetter( InnerClass.class, "thread", Context::thread );
        Method stringStream = method( InnerClass.class, "stringStream" );

        // Then
        String threadName = Thread.currentThread().getName();
        assertEquals( stringValue( "NULL AND NULL" ),
                compileProcedure( signature, emptyList(), stringStream ).apply( ctx, EMPTY, RESOURCE_TRACKER ).next()[0] );
        assertEquals( stringValue( "I'm transaction AND NULL" ),
                compileProcedure( signature, singletonList( setter1 ), stringStream ).apply( ctx, EMPTY, RESOURCE_TRACKER ).next()[0] );
        assertEquals( stringValue( "NULL AND " + threadName ),
                compileProcedure( signature, singletonList( setter2 ), stringStream ).apply( ctx, EMPTY, RESOURCE_TRACKER ).next()[0] );
        assertEquals( stringValue( "I'm transaction AND " + threadName ),
                compileProcedure( signature, asList( setter1, setter2 ), stringStream ).apply( ctx, EMPTY, RESOURCE_TRACKER ).next()[0] );
    }

    @Test
    void shouldHandleThrowingProcedure() throws ProcedureException
    {
        // Given
        ResourceTracker tracker = mock( ResourceTracker.class );
        ProcedureSignature signature = ProcedureSignature.procedureSignature(  "test", "foo" )
                .in( "in", NTString )
                .out( singletonList( inputField( "name", NTString ) ) ).build();

        // When
        CallableProcedure longMethod = compileProcedure( signature, emptyList(), method( "throwingLongStreamMethod" ) );

        // Then
        assertThrows( ProcedureException.class, () -> longMethod.apply( ctx, EMPTY, tracker ).next() );
        verify( tracker ).registerCloseableResource( any( Stream.class ) );
        verify( tracker ).unregisterCloseableResource( any( Stream.class ) );
    }

    @Test
    void shouldCallVoidProcedure() throws ProcedureException, NoSuchFieldException, IllegalAccessException
    {
        // Given
        ProcedureSignature signature = ProcedureSignature.procedureSignature(  "test", "foo" ).build();
        // When
        FieldSetter setter = createSetter( InnerClass.class, "transaction", Context::kernelTransaction );
        CallableProcedure voidMethod =
                compileProcedure( signature, singletonList( setter ), method( InnerClass.class, "voidMethod" ) );

        // Then
        RawIterator<AnyValue[],ProcedureException> iterator =
                voidMethod.apply( ctx, EMPTY, RESOURCE_TRACKER );
        assertFalse( iterator.hasNext() );
        verify( TRANSACTION ).startTime();
    }

    private <T> FieldSetter createSetter( Class<?> owner, String field, ComponentRegistry.Provider<T> provider )
            throws NoSuchFieldException, IllegalAccessException
    {
        Field declaredField = owner.getDeclaredField( field );
        MethodHandle setter = MethodHandles.lookup().unreflectSetter( declaredField );
        return new FieldSetter( declaredField, setter, provider );
    }

    private Method method( String name, Class<?>... types )
    {
        return method( this.getClass(), name, types );
    }

    private Method method( Class<?> owner, String name, Class<?>... types )
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
        public KernelTransaction transaction;
        public Thread thread;

        public String stringMethod()
        {
            String first = transaction != null ? transaction.toString() : "NULL";
            String second = thread != null ? thread.getName() : "NULL";
            return first + " AND " + second;
        }

        public Stream<StringOut> stringStream()
        {
            String first = transaction != null ? transaction.toString() : "NULL";
            String second = thread != null ? thread.getName() : "NULL";
            return Stream.of( new StringOut( first + " AND " + second ) );
        }

        public void voidMethod()
        {
            transaction.startTime();
        }
    }

    public String concat( long l, Double d, boolean b )
    {
        return l + d.toString() + b;
    }

    public String concat( List<Object> list )
    {
        StringBuilder builder = new StringBuilder();
        for ( Object o : list )
        {
            builder.append( o.toString() );
        }
        return builder.toString();
    }

    public Double nullyMethod( Boolean b )
    {
        if ( b != null )
        {
            return null;
        }
        else
        {
            return Math.PI;
        }
    }

    public Number sum( List<Number> numbers )
    {
        return numbers.stream().mapToDouble( Number::doubleValue ).sum();
    }

    //Exhaustive implementation of supported types
    public String testMethod( String in )
    {
        return in;
    }

    public long testMethod( long in )
    {
        return in;
    }

    public Long testMethod( Long in )
    {
        return in;
    }

    public double testMethod( double in )
    {
        return in;
    }

    public Double testMethod( Double in )
    {
        return in;
    }

    public Number testMethod( Number in )
    {
        return in;
    }

    public boolean testMethod( boolean in )
    {
        return in;
    }

    public Boolean testMethod( Boolean in )
    {
        return in;
    }

    public List<Object> testMethod( List<Object> in )
    {
        return in;
    }

    public Map<String,Object> testMethod( Map<String,Object> in )
    {
        return in;
    }

    public byte[] testMethod( byte[] bytes )
    {
        return bytes;
    }

    public Object testMethod( Object in )
    {
        return in;
    }

    public ZonedDateTime testMethod( ZonedDateTime in )
    {
        return in;
    }

    public LocalDateTime testMethod( LocalDateTime in )
    {
        return in;
    }

    public LocalDate testMethod( LocalDate in )
    {
        return in;
    }

    public OffsetTime testMethod( OffsetTime in )
    {
        return in;
    }

    public LocalTime testMethod( LocalTime in )
    {
        return in;
    }

    public TemporalAmount testMethod( TemporalAmount in )
    {
        return in;
    }

    private Map<Class<?>,Method> typeMaps()
    {
        HashMap<Class<?>,Method> methodHashMap = new HashMap<>();
        methodHashMap.put( String.class, method( "testMethod", String.class ) );
        methodHashMap.put( long.class, method( "testMethod", long.class ) );
        methodHashMap.put( Long.class, method( "testMethod", Long.class ) );
        methodHashMap.put( double.class, method( "testMethod", double.class ) );
        methodHashMap.put( Double.class, method( "testMethod", Double.class ) );
        methodHashMap.put( Number.class, method( "testMethod", Number.class ) );
        methodHashMap.put( boolean.class, method( "testMethod", boolean.class ) );
        methodHashMap.put( Boolean.class, method( "testMethod", Boolean.class ) );
        methodHashMap.put( byte[].class, method( "testMethod", byte[].class ) );
        methodHashMap.put( List.class, method( "testMethod", List.class ) );
        methodHashMap.put( Map.class, method( "testMethod", Map.class ) );
        methodHashMap.put( Object.class, method( "testMethod", Object.class ) );
        methodHashMap.put( ZonedDateTime.class, method( "testMethod", ZonedDateTime.class ) );
        methodHashMap.put( LocalDateTime.class, method( "testMethod", LocalDateTime.class ) );
        methodHashMap.put( LocalDate.class, method( "testMethod", LocalDate.class ) );
        methodHashMap.put( OffsetTime.class, method( "testMethod", OffsetTime.class ) );
        methodHashMap.put( LocalTime.class, method( "testMethod", LocalTime.class ) );
        methodHashMap.put( TemporalAmount.class, method( "testMethod", TemporalAmount.class ) );

        //safety check, make sure we are testing all types
        Set<Type> types = new TypeMappers().allTypes();
        for ( Type type : types )
        {
            assertTrue( methodHashMap.containsKey( type ), type + " is not being tested!" );
        }
        return methodHashMap;
    }

    public Stream<LongOut> longStream( long in )
    {
        return Stream.of( new LongOut( in ) );
    }

    public Stream<LongOut> throwingLongStreamMethod()
    {
        return Stream.generate( () -> {
            throw new RuntimeException( "wut!" );
        } );
    }


    public static class LongOut
    {
        public long field;

        public LongOut( long field )
        {
            this.field = field;
        }
    }

    public static class StringOut
    {
        public String field;

        public StringOut( String field )
        {
            this.field = field;
        }
    }
}
