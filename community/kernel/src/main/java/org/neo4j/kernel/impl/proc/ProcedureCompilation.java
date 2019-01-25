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
import org.neo4j.codegen.MethodDeclaration;
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
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.ByteValue;
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
import org.neo4j.values.storable.Value;
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
import static org.neo4j.codegen.Expression.equal;
import static org.neo4j.codegen.Expression.getStatic;
import static org.neo4j.codegen.Expression.invoke;
import static org.neo4j.codegen.Expression.ternary;
import static org.neo4j.codegen.Expression.unbox;
import static org.neo4j.codegen.FieldReference.field;
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
import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;

/**
 * Class responsible for generating code for calling user-defined procedures and functions.
 */
@SuppressWarnings( {"WeakerAccess", "unused"} )
public final class ProcedureCompilation
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
    private static final String SIGNATURE_NAME = "SIGNATURE";
    private static final String USER_CLASS = "USER_CLASS";
    private static final String VALUE_MAPPER_NAME = "MAPPER";
    private static final MethodDeclaration.Builder USER_FUNCTION = method( AnyValue.class, "apply",
            param( Context.class, "ctx" ),
            param( AnyValue[].class, "input" ) )
            .throwsException( typeReference( ProcedureException.class ) );

    private ProcedureCompilation()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    /**
     * Generates code for a user-defined function.
     * <p>
     * Given a user-defined function defined by
     *
     * <pre>
     *     class MyClass {
     *         //Using [AT] because the javadocs chokes on the real symbol
     *        [AT]Context
     *        public Log log;
     *        //Using [AT] because the javadocs chokes on the real symbol
     *        [AT]serFunction
     *        public double addPi(long value) {
     *            return value + Math.PI;
     *        }
     *     }
     * </pre>
     * <p>
     * we will generate something like
     *
     * <pre>
     *     class GeneratedAddPi implements CallableUserFunction {
     *         public static UserFunctionSignature SIGNATURE;
     *         public static MyClass USER_CLASS;
     *         public static FieldSetter SETTER_0;
     *
     *         public AnyValue apply(Context ctx, AnyValue[] input) {
     *              try {
     *                  UDF.log = (Log) SETTER_0.get(ctx);
     *                  return Values.doubleValue(UDF.addPi( ((NumberValue) input[0]).longValue() );
     *              } catch (Throwable T) {
     *                  throw new ProcedureException([appropriate error msg], T);
     *              }
     *         }
     *
     *         public UserFunctionSignature signature() {return SIGNATURE;}
     *     }
     * </pre>
     * <p>
     * where the static fields are set once during loading via reflection.
     *
     * @param signature the signature of the user-defined function
     * @param fieldSetters the fields to set before each call.
     * @param methodToCall the method to call
     * @return a CallableUserFunction delegating to the underlying user-defined function.
     * @throws ProcedureException if something went wrong when compiling the user-defined function.
     */
    static CallableUserFunction compileFunction(
            UserFunctionSignature signature, List<FieldSetter> fieldSetters,
            Method methodToCall ) throws ProcedureException
    {

        ClassHandle handle;
        try
        {
            CodeGenerator codeGenerator = codeGenerator();
            try ( ClassGenerator generator = codeGenerator.generateClass( PACKAGE, className( signature ), CallableUserFunction.class ) )
            {
                //static fields
                FieldReference signatureField = generator.publicStaticField( typeReference( UserFunctionSignature.class ), SIGNATURE_NAME );
                FieldReference userClass = generator.publicStaticField( typeReference( methodToCall.getDeclaringClass() ), USER_CLASS );
                FieldReference mapperField = generator.publicStaticField( parameterizedType( ValueMapper.class, Object.class ),
                                VALUE_MAPPER_NAME );
                List<FieldReference> fieldsToSet = new ArrayList<>( fieldSetters.size() );
                for ( int i = 0; i < fieldSetters.size(); i++ )
                {
                    fieldsToSet.add( generator.publicStaticField( typeReference( FieldSetter.class ), "SETTER_" + i ) );
                }

                //CallableUserFunction::apply
                try ( CodeBlock method = generator.generate( USER_FUNCTION ) )
                {
                    method.tryCatch(
                            body -> functionBody( body, fieldSetters, fieldsToSet, userClass, methodToCall,
                                    mapperField ),
                            onError ->
                                    onError( onError, format( "function `%s`", signature.name() ) ),
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

            //set all static fields
            clazz.getDeclaredField( SIGNATURE_NAME ).set( null, signature );
            clazz.getDeclaredField( USER_CLASS ).set( null, methodToCall.getDeclaringClass().newInstance() );
            for ( int i = 0; i < fieldSetters.size(); i++ )
            {
                clazz.getDeclaredField( "SETTER_" + i ).set(null, fieldSetters.get( i ));
            }

            return (CallableUserFunction) clazz.newInstance();
        }
        catch ( Throwable e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureRegistrationFailed, e,
                    "Failed to compile function defined in `%s`: %s", methodToCall.getDeclaringClass().getSimpleName(),
                    e.getMessage() );
        }
    }

    private static String className( UserFunctionSignature signature )
    {
        return format( "Generated_%s%d", signature.name().name(), System.nanoTime() );
    }

    /**
     * Generates the actual body of the function. Generated the code will look something like:
     * <p>
     * USER_CLASS.field1 = (type1) SETTER_1.get(context)
     * USER_CLASS.field2 = (type2) SETTER_2.get(context)
     * ...
     * return [CONVERT TO AnyVALUE](USER_CLASS.call( [CONVERT_TO_JAVA] input[0], ... );
     */
    private static void functionBody( CodeBlock block,
            List<FieldSetter> fieldSetters, List<FieldReference> fieldsToSet, FieldReference udfField,
            Method methodToCall, FieldReference mapperField )
    {
        for ( int i = 0; i < fieldSetters.size(); i++ )
        {
            FieldSetter setter = fieldSetters.get( i );
            Field field = setter.field();
            Class<?> fieldType = field.getType();
            block.put( getStatic( udfField ), field( field ),
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
        block.assign( methodToCall.getReturnType(), "fromFunction",
                invoke( getStatic( udfField ), methodReference( methodToCall ), parameters ) );
        block.returns(
                toAnyValue( block.load( "fromFunction" ) ) );
    }

    /**
     * Handle errors by redirecting to {@link #rethrowProcedureException(Throwable, String)}  }
     */
    private static void onError( CodeBlock block, String typeAndName )
    {

        block.throwException(
                invoke( methodReference( ProcedureCompilation.class, ProcedureException.class,
                        "rethrowProcedureException", Throwable.class, String.class ),
                        block.load( "T" ),
                        constant( typeAndName ) ) );
    }

    /**
     * Used for properly setting a primitive field
     *
     * For example say that the class has a field of type long. Then we will generate the RHS of.
     * <pre>
     *     USER_CLASS.longField = ((Long) invoke).longValue()
     * </pre>
     *
     * @param fieldType the type of the field
     * @param invoke the expression used to set the field
     * @return an expression where casting and boxing/unboxing has been taken care of.
     */
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

    /**
     * Used by generated code. Needs to be public.
     * @param throwable The thrown exception
     * @param typeAndName the type and name of the caller, e.g "function `my.udf`"
     * @return an exception with an appropriate message.
     */
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
                    "Failed to invoke %s: %s", typeAndName,
                    "Caused by: " + (cause != null ? cause : throwable) );
        }
    }

    /**
     * Byte arrays needs special treatment since it is not a proper Cypher type
     * @param input either a ByteArray or ListValue of bytes
     * @return input value converted to a byte[]
     */
    public static byte[] toByteArray( AnyValue input )
    {
        if ( input instanceof ByteArray )
        {
            return ((ByteArray) input).asObjectCopy();
        }
        if ( input instanceof SequenceValue )
        {
            SequenceValue list = (SequenceValue) input;
            if ( list.iterationPreference() == RANDOM_ACCESS )
            {
                byte[] bytes = new byte[list.length()];
                for ( int a = 0; a < bytes.length; a++ )
                {
                    bytes[a] = asByte( list.value( a ) );
                }
                return  bytes;
            }
            else
            {
                //list.length may have linear complexity, still worth doing it upfront
                byte[] bytes = new byte[list.length()];
                int i = 0;
                for ( AnyValue anyValue : list )
                {
                    bytes[i++] = asByte( anyValue );
                }

                return bytes;
            }
        }
        else
        {
            throw new IllegalArgumentException(
                    "Cannot convert " + input.getClass().getSimpleName() + " to byte[] for input to procedure" );
        }
    }

    private static byte asByte( AnyValue value )
    {
        if ( value instanceof ByteValue )
        {
            return ((ByteValue) value).value();
        }
        else
        {
            throw new IllegalArgumentException(
                    "Cannot convert " + value.map( new DefaultValueMapper( null ) ) + " to byte for input to procedure" );
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
            return nullCheck( expression, invoke( methodReference( Values.class, LongValue.class, "longValue", long.class ),
                    unbox( expression ) ));
        }
        else if ( type.equals( DOUBLE ) )
        {
            return invoke( methodReference( Values.class, DoubleValue.class, "doubleValue", double.class ),
                    expression );
        }
        else if ( type.equals( BOXED_DOUBLE ) )
        {
            return nullCheck( expression, invoke( methodReference( Values.class, DoubleValue.class, "doubleValue", double.class ),
                    unbox( expression )));
        }
        else if ( type.equals( NUMBER ) )
        {
            return nullCheck( expression, invoke( methodReference( Values.class, NumberValue.class, "numberValue", Number.class ), expression ));
        }
        else if ( type.equals( BOOLEAN ) )
        {
            return invoke( methodReference( Values.class, BooleanValue.class, "booleanValue", boolean.class ),
                    expression );
        }
        else if ( type.equals( BOXED_BOOLEAN ) )
        {
            return nullCheck( expression, invoke( methodReference( Values.class, BooleanValue.class, "booleanValue", boolean.class ),
                    unbox( expression ) ));
        }
        else if ( type.equals( STRING ) )
        {
            return invoke( methodReference( Values.class, Value.class, "stringOrNoValue", String.class ), expression );
        }
        else if ( type.equals( BYTE_ARRAY ) )
        {
            return nullCheck( expression, invoke( methodReference( Values.class, ByteArray.class, "byteArray", byte[].class ), expression ));
        }
        else if ( type.equals( LIST ) )
        {
            return nullCheck( expression, invoke( methodReference( ValueUtils.class, ListValue.class, "asListValue", Iterable.class ),
                    expression ));
        }
        else if ( type.equals( MAP ) )
        {
            return nullCheck( expression, invoke( methodReference( ValueUtils.class, MapValue.class, "asMapValue", Map.class ),
                    expression ));
        }
        else if ( type.equals( ZONED_DATE_TIME ) )
        {
            return nullCheck( expression, invoke( methodReference( DateTimeValue.class, DateTimeValue.class, "datetime", ZonedDateTime.class ),
                    expression ));
        }
        else if ( type.equals( OFFSET_TIME ) )
        {
            return nullCheck( expression, invoke( methodReference( TimeValue.class, TimeValue.class, "time", OffsetTime.class ),
                    expression ));
        }
        else if ( type.equals( LOCAL_DATE ) )
        {
            return nullCheck( expression, invoke( methodReference( DateValue.class, DateValue.class, "date", LocalDate.class ),
                    expression ));
        }
        else if ( type.equals( LOCAL_TIME ) )
        {
            return nullCheck( expression, invoke( methodReference( LocalTimeValue.class, LocalTimeValue.class, "localTime", LocalTime.class ),
                    expression ));
        }
        else if ( type.equals( LOCAL_DATE_TIME ) )
        {
            return nullCheck( expression, invoke( methodReference( LocalDateTimeValue.class, LocalDateTimeValue.class, "localDateTime",
                    LocalDateTime.class ), expression ));
        }
        else if ( type.equals( TEMPORAL_AMOUNT ) )
        {
            return nullCheck( expression, invoke( methodReference( Values.class, DurationValue.class, "durationValue",
                    TemporalAmount.class ), expression ));
        }
        else if ( type.equals( NODE ) )
        {
            return nullCheck( expression, invoke( methodReference( ValueUtils.class, NodeValue.class, "fromNodeProxy", Node.class ),
                    expression ));
        }
        else if ( type.equals( RELATIONSHIP ) )
        {
            return nullCheck( expression, invoke( methodReference( ValueUtils.class, RelationshipValue.class, "fromRelationshipProxy",
                    Relationship.class ), expression ));
        }
        else if ( type.equals( PATH ) )
        {
            return nullCheck( expression, invoke( methodReference( ValueUtils.class, PathValue.class, "fromPath", Path.class ), expression ));
        }
        else if ( type.equals( POINT ) )
        {
            return nullCheck( expression, invoke( methodReference( ValueUtils.class, PointValue.class, "asPointValue", Point.class ),
                    expression ));
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
            return noValueCheck( expression, box( invoke( cast( NumberValue.class, expression ),
                    methodReference( NumberValue.class, long.class, "longValue" ) ) ));
        }
        else if ( type.equals( DOUBLE ) )
        {
            return invoke( cast( NumberValue.class, expression ),
                    methodReference( NumberValue.class, double.class, "doubleValue" ) );
        }
        else if ( type.equals( BOXED_DOUBLE ) )
        {
            return noValueCheck( expression,box( invoke( cast( NumberValue.class, expression ),
                    methodReference( NumberValue.class, double.class, "doubleValue" ) ) ));
        }
        else if ( type.equals( NUMBER ) )
        {
            return noValueCheck( expression, invoke( cast( NumberValue.class, expression ),
                    methodReference( NumberValue.class, Number.class, "asObjectCopy" ) ));
        }
        else if ( type.equals( BOOLEAN ) )
        {
            return invoke( cast( BooleanValue.class, expression ),
                    methodReference( BooleanValue.class, boolean.class, "booleanValue" ) );
        }
        else if ( type.equals( BOXED_BOOLEAN ) )
        {
            return noValueCheck(expression, box( invoke( cast( BooleanValue.class, expression ),
                    methodReference( BooleanValue.class, boolean.class, "booleanValue" ) ) ));
        }
        else if ( type.equals( STRING ) )
        {
            return noValueCheck( expression, invoke( cast( TextValue.class, expression ),
                    methodReference( TextValue.class, String.class, "stringValue" ) ));
        }
        else if ( type.equals( BYTE_ARRAY ) )
        {
            return noValueCheck( expression, invoke(
                    methodReference( ProcedureCompilation.class, byte[].class, "toByteArray", AnyValue.class ),
                    expression ));
        }
        else if ( type.equals( ZONED_DATE_TIME ) )
        {
            return noValueCheck( expression,invoke( cast( DateTimeValue.class, expression ),
                    methodReference( DateTimeValue.class, ZonedDateTime.class, "temporal" ) ));
        }
        else if ( type.equals( OFFSET_TIME ) )
        {
            return noValueCheck( expression,invoke( cast( TimeValue.class, expression ),
                    methodReference( TimeValue.class, OffsetTime.class, "temporal" ) ));
        }
        else if ( type.equals( LOCAL_DATE ) )
        {
            return noValueCheck( expression,invoke( cast( TimeValue.class, expression ),
                    methodReference( TimeValue.class, OffsetTime.class, "temporal" ) ));
        }
        else if ( type.equals( LOCAL_TIME ) )
        {
            return noValueCheck( expression,invoke( cast( LocalTimeValue.class, expression ),
                    methodReference( LocalTimeValue.class, LocalTime.class, "temporal" ) ));
        }
        else if ( type.equals( LOCAL_DATE_TIME ) )
        {
            return noValueCheck( expression, invoke( cast( LocalDateTimeValue.class, expression ),
                    methodReference( LocalTimeValue.class, LocalDateTime.class, "temporal" ) ));
        }
        else if ( type.equals( TEMPORAL_AMOUNT ) )
        {
            return noValueCheck( expression, invoke( cast( DurationValue.class, expression ),
                    methodReference( DurationValue.class, TemporalAmount.class, "asObjectCopy" ) ));
        }
        else if ( type.equals( PATH ) )
        {
            return noValueCheck( expression, invoke( methodReference( ValueUtils.class, PathValue.class, "fromPath", Path.class ), expression ));
        }
        else if ( type.equals( POINT ) )
        {
            return noValueCheck( expression, invoke( cast( PointValue.class, expression ),
                    methodReference( PointValue.class, Point.class, "asObjectCopy" ) ));
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

    /**
     * toCheck == null ? Values.NO_VALUE : onNotNull;
     */
    private static Expression nullCheck( Expression toCheck, Expression onNotNull )
    {
        return ternary( equal( toCheck, constant( null ) ), noValue(), onNotNull );
    }

    /**
     * toCheck == NO_VALUE ? null : onNotNoValue;
     */
    private static Expression noValueCheck( Expression toCheck, Expression onNotNoValue )
    {
        return ternary( equal( toCheck, noValue() ), constant( null ), onNotNoValue );
    }

    /**
     * @return Values.NO_VALUE;
     */
    private static Expression noValue()
    {
        return getStatic( field( typeReference( Values.class ), typeReference( Value.class ), "NO_VALUE" ) );
    }
}
