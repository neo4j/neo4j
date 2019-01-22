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

import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.codegen.ClassGenerator;
import org.neo4j.codegen.ClassHandle;
import org.neo4j.codegen.CodeBlock;
import org.neo4j.codegen.CodeGenerationNotSupportedException;
import org.neo4j.codegen.CodeGenerator;
import org.neo4j.codegen.Expression;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.TypeReference;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableUserFunction;
import org.neo4j.kernel.api.proc.Context;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;

import static java.lang.String.format;
import static org.neo4j.codegen.CodeGenerator.generateCode;
import static org.neo4j.codegen.Expression.arrayLoad;
import static org.neo4j.codegen.Expression.box;
import static org.neo4j.codegen.Expression.cast;
import static org.neo4j.codegen.Expression.constant;
import static org.neo4j.codegen.Expression.getStatic;
import static org.neo4j.codegen.Expression.invoke;
import static org.neo4j.codegen.Expression.unbox;
import static org.neo4j.codegen.FieldReference.fieldReference;
import static org.neo4j.codegen.MethodDeclaration.method;
import static org.neo4j.codegen.MethodReference.methodReference;
import static org.neo4j.codegen.Parameter.param;
import static org.neo4j.codegen.TypeReference.OBJECT;
import static org.neo4j.codegen.TypeReference.parameterizedType;
import static org.neo4j.codegen.TypeReference.toBoxedType;
import static org.neo4j.codegen.TypeReference.typeReference;
import static org.neo4j.codegen.bytecode.ByteCode.BYTECODE;
import static org.neo4j.codegen.source.SourceCode.PRINT_SOURCE;
import static org.neo4j.codegen.source.SourceCode.SOURCECODE;

public class ProcedureCompilation
{
    private static final String LONG = long.class.getCanonicalName();
    private static final String BOXED_LONG = Long.class.getCanonicalName();
    private static final String DOUBLE = double.class.getCanonicalName();
    private static final String BOXED_DOUBLE = Double.class.getCanonicalName();
    private static final String BOOLEAN = boolean.class.getCanonicalName();
    private static final String BOXED_BOOLEAN = Boolean.class.getCanonicalName();
    private static final String NUMBER = Number.class.getCanonicalName();
    private static final String STRING = String.class.getCanonicalName();
    private static final String NODE = Node.class.getCanonicalName();
    private static final String RELATIONSHIP = Relationship.class.getCanonicalName();
    private static final String PATH = Path.class.getCanonicalName();
    private static final String POINT = org.neo4j.graphdb.spatial.Point.class.getCanonicalName();
    private static final String LIST = List.class.getCanonicalName();
    private static final String MAP = Map.class.getCanonicalName();
    private static final String BYTE_ARRAY = byte[].class.getCanonicalName();
    private static final String ZONED_DATE_TIME = ZonedDateTime.class.getCanonicalName();
    private static final String LOCAL_DATE_TIME = LocalDateTime.class.getCanonicalName();
    private static final String LOCAL_DATE = LocalDate.class.getCanonicalName();
    private static final String OFFSET_TIME = OffsetTime.class.getCanonicalName();
    private static final String LOCAL_TIME = LocalTime.class.getCanonicalName();
    private static final String TEMPORAL_AMOUNT = TemporalAmount.class.getCanonicalName();
    private static final String PACKAGE = "org.neo4j.kernel.impl.proc";
    private static final boolean DEBUG = false;

    static CallableUserFunction compileFunction(
            UserFunctionSignature signature, List<FieldSetter> fieldSetters,
            Method methodToCall, ValueMapper<Object> typeMappers )
    {

        ClassHandle handle;
        try
        {
            CodeGenerator codeGenerator = codeGenerator();
            String className = format( "Generated_%s%d", signature.name().name(), System.nanoTime() );
            try ( ClassGenerator generator = codeGenerator.generateClass( PACKAGE, className, CallableUserFunction.class ) )
            {

                //static fields
                FieldReference signatureField =
                        generator.publicStaticField( typeReference( UserFunctionSignature.class ), "SIGNATURE" );
                FieldReference udfField = generator.publicStaticField( typeReference( methodToCall.getDeclaringClass() ), "UDF" );
                FieldReference mapperField =
                        generator.publicStaticField( parameterizedType( ValueMapper.class, Object.class ), "MAPPER" );
                List<FieldReference> fieldsToSet = new ArrayList<>( fieldSetters.size() );
                for ( int i = 0; i < fieldSetters.size(); i++ )
                {
                    fieldsToSet.add( generator.publicStaticField( typeReference( FieldSetter.class ), "SETTER_" + i ) );
                }

                //CallableUserFunction::apply
                try ( CodeBlock method = generator.generate( method( AnyValue.class, "apply",
                        param( Context.class, "ctx" ),
                        param( AnyValue[].class, "input" ) )
                        .throwsException( typeReference( ProcedureException.class ) ) ) )
                {
                    method.tryCatch(
                            body -> functionBody( body, fieldSetters, fieldsToSet, udfField, methodToCall,
                                    mapperField ),
                            onError ->
                                    onError.throwException(
                                            invoke( methodReference( ProcedureCompilation.class,
                                                    ProcedureException.class,
                                                    "rethrowProcedureException", Throwable.class, String.class ),
                                                    onError.load( "T" ),
                                                    constant( format( "function '%s'", signature.name() ) ) ) ),
                            param( Throwable.class, "T" )
                    );
                }

                //CallableUserFunction::signature
                try ( CodeBlock method = generator.generateMethod( UserFunctionSignature.class, "signature" ) )
                {
                    method.returns( getStatic( signatureField ) );
                }

                handle = generator.handle();
            }
            Class<?> clazz = handle.loadClass();
            clazz.getDeclaredField( "SIGNATURE" ).set( null, signature );
            clazz.getDeclaredField( "UDF" ).set( null, methodToCall.getDeclaringClass().newInstance() );
            clazz.getDeclaredField( "MAPPER" ).set( null, typeMappers );
            for ( int i = 0; i < fieldSetters.size(); i++ )
            {
                clazz.getDeclaredField( "SETTER_" + i ).set(null, fieldSetters.get( i ));
            }
            return (CallableUserFunction) clazz.newInstance();
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }

    private static void functionBody( CodeBlock block,
            List<FieldSetter> fieldSetters, List<FieldReference> fieldsToSet, FieldReference udfField,
            Method methodToCall, FieldReference mapperField )
    {
        for ( int i = 0; i < fieldSetters.size(); i++ )
        {
            FieldSetter setter = fieldSetters.get( i );
            Field field = setter.field();
            Class<?> fieldType = field.getType();
            block.put( getStatic( udfField ), fieldReference( field ),
                    unboxIfNecessary( fieldType,
                            invoke(
                                    getStatic( fieldsToSet.get( i ) ),
                                    methodReference( typeReference( FieldSetter.class ), OBJECT, "get",
                                            typeReference( Context.class ) ),
                                    block.load( "ctx" ) ) ) );
        }
        Class<?>[] parameterTypes = methodToCall.getParameterTypes();
        Expression[] parameters = new Expression[parameterTypes.length];
        for ( int i = 0; i < parameterTypes.length; i++ )
        {
            parameters[i] = fromAnyValue(
                    typeReference(
                            parameterTypes[i] ), arrayLoad( block.load( "input" ), constant( i ) ), mapperField );
        }

        block.returns(
                toAnyValue( invoke( getStatic( udfField ), methodReference( methodToCall ), parameters ) ) );
    }

    private static Expression unboxIfNecessary( Class<?> fieldType, Expression invoke )
    {
        if ( fieldType.isPrimitive() )
        {
            return unbox( cast( toBoxedType( typeReference( fieldType ) ), invoke ) );
        }
        else
        {
            return cast( fieldType, invoke );
        }
    }

    private static CodeGenerator codeGenerator() throws CodeGenerationNotSupportedException
    {
        if ( DEBUG )
        {
            return generateCode( CallableUserFunction.class.getClassLoader(), SOURCECODE, PRINT_SOURCE );
        }
        else
        {
            return generateCode( CallableUserFunction.class.getClassLoader(), BYTECODE );
        }
    }

    public static ProcedureException rethrowProcedureException( Throwable throwable, String typeAndName )
    {
        if ( throwable instanceof Status.HasStatus )
        {
            return new ProcedureException( ((Status.HasStatus) throwable).status(), throwable,
                    throwable.getMessage(), throwable );
        }
        else
        {
            Throwable cause = ExceptionUtils.getRootCause( throwable );
            return new ProcedureException( Status.Procedure.ProcedureCallFailed, throwable,
                    "Failed to invoke %s : %s", typeAndName,
                    "Caused by: " + (cause != null ? cause : throwable) );
        }
    }

    /**
     * Takes an expression evaluating to one of the supported java values and turns
     * it into the corresponding AnyValue
     *
     * @param expression the expression to evaluate
     * @return an expression propery mapped to AnyValue
     */
    private static Expression toAnyValue( Expression expression )
    {
        String type = expression.type().fullName();
        if ( type.equals( LONG ) )
        {
            return invoke( methodReference( Values.class, LongValue.class, "longValue", long.class ), expression );
        }
        else if ( type.equals( BOXED_LONG ) )
        {
            return invoke( methodReference( Values.class, LongValue.class, "longValue", long.class ),
                    unbox( expression ) );
        }
        else if ( type.equals( DOUBLE ) )
        {
            return invoke( methodReference( Values.class, DoubleValue.class, "doubleValue", double.class ),
                    expression );
        }
        else if ( type.equals( NUMBER ) )
        {
            return invoke( methodReference( Values.class, Number.class, "numberValue", Number.class ), expression );
        }
        else if ( type.equals( BOXED_DOUBLE ) )
        {
            return invoke( methodReference( Values.class, DoubleValue.class, "doubleValue", double.class ),
                    unbox( expression ) );
        }
        else if ( type.equals( BOOLEAN ) )
        {
            return invoke( methodReference( Values.class, BooleanValue.class, "booleanValue", boolean.class ),
                    expression );
        }
        else if ( type.equals( BOXED_BOOLEAN ) )
        {
            return invoke( methodReference( Values.class, BooleanValue.class, "booleanValue", boolean.class ),
                    unbox( expression ) );
        }
        else if ( type.equals( STRING ) )
        {
            return invoke( methodReference( Values.class, TextValue.class, "stringValue", String.class ), expression );
        }
        else if ( type.equals( BYTE_ARRAY ) )
        {
            return invoke( methodReference( Values.class, ByteArray.class, "byteArray", byte[].class ), expression );
        }
        else if ( type.equals( LIST ) )
        {
            return invoke( methodReference( ValueUtils.class, ListValue.class, "asListValue", Iterable.class ),
                    expression );
        }
        else if ( type.equals( MAP ) )
        {
            return invoke( methodReference( ValueUtils.class, MapValue.class, "asMapValue", Map.class ),
                    expression );
        }
        else if ( type.equals( ZONED_DATE_TIME ) )
        {
            return invoke( methodReference( DateTimeValue.class, DateTimeValue.class, "datetime", ZonedDateTime.class ),
                    expression );
        }
        else if ( type.equals( OFFSET_TIME ) )
        {
            return invoke( methodReference( TimeValue.class, TimeValue.class, "time", OffsetTime.class ),
                    expression );
        }
        else if ( type.equals( LOCAL_DATE ) )
        {
            return invoke( methodReference( DateValue.class, DateValue.class, "date", LocalDate.class ),
                    expression );
        }
        else if ( type.equals( LOCAL_TIME ) )
        {
            return invoke( methodReference( LocalTimeValue.class, LocalTimeValue.class, "localTime", LocalTime.class ),
                    expression );
        }
        else if ( type.equals( LOCAL_DATE_TIME ) )
        {
            return invoke( methodReference( LocalDateTimeValue.class, LocalDateTimeValue.class, "localDateTime",
                    LocalDateTime.class ), expression );
        }
        else if ( type.equals( TEMPORAL_AMOUNT ) )
        {
            return invoke( methodReference( Values.class, DurationValue.class, "durationValue",
                    TemporalAmount.class ), expression );
        }
        else if ( type.equals( NODE ) )
        {
            return invoke( methodReference( ValueUtils.class, NodeValue.class, "fromNodeProxy", Node.class ),
                    expression );
        }
        else if ( type.equals( RELATIONSHIP ) )
        {
            return invoke( methodReference( ValueUtils.class, RelationshipValue.class, "fromRelationshipProxy",
                    Relationship.class ), expression );
        }
        else if ( type.equals( PATH ) )
        {
            return invoke( methodReference( ValueUtils.class, PathValue.class, "fromPath", Path.class ), expression );
        }
        else if ( type.equals( POINT ) )
        {
            return invoke( methodReference( ValueUtils.class, PointValue.class, "asPointValue", Point.class ),
                    expression );
        }
        else
        {
            return invoke( methodReference( ValueUtils.class, AnyValue.class, "of", Object.class ), expression );
        }
    }

    /**
     * Converts from an `AnyValue` to the type specified by the procedure or function.
     * <p>
     * In a lot of cases we can figure out the conversion statically, for example `LongValue` will
     * be turned into ((LongValue) value).longValue() etc. For composite types as Lists and Maps and also
     * for Graph types such as Nodes, Relationships and Paths we will us a ValueMapper.
     *
     * @param expectedType the java type expected by the procedure or function
     * @param expression an expression that will evaluate to an AnyValue
     * @param mapper The field holding the ValueMapper.
     * @return an expression properly typed to be consumed by function or procedure
     */
    private static Expression fromAnyValue( TypeReference expectedType, Expression expression, FieldReference mapper )
    {
        String type = expectedType.fullName();
        if ( type.equals( LONG ) )
        {
            return invoke( cast( NumberValue.class, expression ),
                    methodReference( NumberValue.class, long.class, "longValue" ) );

        }
        else if ( type.equals( BOXED_LONG ) )
        {
            return box( invoke( cast( NumberValue.class, expression ),
                    methodReference( NumberValue.class, long.class, "longValue" ) ) );
        }
        else if ( type.equals( DOUBLE ) )
        {
            return invoke( cast( NumberValue.class, expression ),
                    methodReference( NumberValue.class, double.class, "doubleValue" ) );
        }
        else if ( type.equals( BOXED_DOUBLE ) )
        {
            return box( invoke( cast( NumberValue.class, expression ),
                    methodReference( NumberValue.class, double.class, "doubleValue" ) ) );
        }
        else if ( type.equals( NUMBER ) )
        {
            return invoke( cast( NumberValue.class, expression ),
                    methodReference( NumberValue.class, Number.class, "asObjectCopy" ) );
        }
        else if ( type.equals( BOOLEAN ) )
        {
            return invoke( cast( BooleanValue.class, expression ),
                    methodReference( BooleanValue.class, boolean.class, "booleanValue" ) );
        }
        else if ( type.equals( BOXED_BOOLEAN ) )
        {
            return box( invoke( cast( BooleanValue.class, expression ),
                    methodReference( BooleanValue.class, boolean.class, "booleanValue" ) ) );
        }
        else if ( type.equals( STRING ) )
        {
            return invoke( cast( TextValue.class, expression ),
                    methodReference( TextValue.class, String.class, "stringValue" ) );
        }
        else if ( type.equals( BYTE_ARRAY ) )
        {
            return invoke( cast( ByteArray.class, expression ),
                    methodReference( ByteArray.class, byte[].class, "asObjectCopy" ) );
        }
        else if ( type.equals( ZONED_DATE_TIME ) )
        {
            return invoke( cast( DateTimeValue.class, expression ),
                    methodReference( DateTimeValue.class, ZonedDateTime.class, "temporal" ) );
        }
        else if ( type.equals( OFFSET_TIME ) )
        {
            return invoke( cast( TimeValue.class, expression ),
                    methodReference( TimeValue.class, OffsetTime.class, "temporal" ) );
        }
        else if ( type.equals( LOCAL_DATE ) )
        {
            return invoke( cast( TimeValue.class, expression ),
                    methodReference( TimeValue.class, OffsetTime.class, "temporal" ) );
        }
        else if ( type.equals( LOCAL_TIME ) )
        {
            return invoke( cast( LocalTimeValue.class, expression ),
                    methodReference( LocalTimeValue.class, LocalTime.class, "temporal" ) );
        }
        else if ( type.equals( LOCAL_DATE_TIME ) )
        {
            return invoke( cast( LocalDateTimeValue.class, expression ),
                    methodReference( LocalTimeValue.class, LocalDateTime.class, "temporal" ) );
        }
        else if ( type.equals( TEMPORAL_AMOUNT ) )
        {
            return invoke( cast( DurationValue.class, expression ),
                    methodReference( DurationValue.class, TemporalAmount.class, "asObjectCopy" ) );
        }
        else if ( type.equals( PATH ) )
        {
            return invoke( methodReference( ValueUtils.class, PathValue.class, "fromPath", Path.class ), expression );
        }
        else if ( type.equals( POINT ) )
        {
            return invoke( cast( PointValue.class, expression ),
                    methodReference( PointValue.class, Point.class, "asObjectCopy" ) );
        }
        else
        {
            return
                    cast( expectedType, invoke(
                            expression,
                            methodReference( AnyValue.class, Object.class, "map", ValueMapper.class ),
                            getStatic( mapper ) ) );

        }
    }
}
