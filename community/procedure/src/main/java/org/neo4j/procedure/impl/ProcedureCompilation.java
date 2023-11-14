/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.impl;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.neo4j.codegen.CodeGenerator.generateCode;
import static org.neo4j.codegen.Expression.arrayLoad;
import static org.neo4j.codegen.Expression.box;
import static org.neo4j.codegen.Expression.cast;
import static org.neo4j.codegen.Expression.constant;
import static org.neo4j.codegen.Expression.equal;
import static org.neo4j.codegen.Expression.get;
import static org.neo4j.codegen.Expression.getStatic;
import static org.neo4j.codegen.Expression.invoke;
import static org.neo4j.codegen.Expression.invokeSuper;
import static org.neo4j.codegen.Expression.newInstance;
import static org.neo4j.codegen.Expression.ternary;
import static org.neo4j.codegen.Expression.unbox;
import static org.neo4j.codegen.FieldReference.field;
import static org.neo4j.codegen.MethodDeclaration.method;
import static org.neo4j.codegen.MethodReference.constructorReference;
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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.neo4j.codegen.ClassGenerator;
import org.neo4j.codegen.ClassHandle;
import org.neo4j.codegen.CodeBlock;
import org.neo4j.codegen.CodeGenerationNotSupportedException;
import org.neo4j.codegen.CodeGenerator;
import org.neo4j.codegen.CompilationFailureException;
import org.neo4j.codegen.Expression;
import org.neo4j.codegen.FieldReference;
import org.neo4j.codegen.MethodDeclaration;
import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.UserAggregator;
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.CallableUserFunction;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
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
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

/**
 * Class responsible for generating code for calling user-defined procedures and functions.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class ProcedureCompilation {
    public static final RawIterator<AnyValue[], ProcedureException> VOID_ITERATOR = new RawIterator<>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public AnyValue[] next() {
            return EMPTY_ARRAY;
        }
    };

    private static final boolean DEBUG = false;
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
    private static final String SIGNATURE_NAME = "SIGNATURE";
    private static final String USER_CLASS = "userClass";
    private static final String VALUE_MAPPER_NAME = "MAPPER";
    private static final AnyValue[] EMPTY_ARRAY = new AnyValue[0];

    private static final MethodDeclaration.Builder USER_FUNCTION = method(
                    AnyValue.class, "apply", param(Context.class, "ctx"), param(AnyValue[].class, "input"))
            .throwsException(typeReference(ProcedureException.class));
    private static final MethodDeclaration.Builder USER_PROCEDURE = method(
                    parameterizedType(
                            RawIterator.class,
                            typeReference(AnyValue[].class),
                            typeReference(ProcedureException.class)),
                    "apply",
                    param(Context.class, "ctx"),
                    param(AnyValue[].class, "input"),
                    param(ResourceMonitor.class, "monitor"))
            .throwsException(typeReference(ProcedureException.class));
    private static final MethodDeclaration.Builder AGGREGATION_CREATE = method(
                    UserAggregator.class, "create", param(Context.class, "ctx"))
            .throwsException(typeReference(ProcedureException.class));
    private static final MethodDeclaration.Builder AGGREGATION_UPDATE = method(
                    void.class, "update", param(AnyValue[].class, "input"))
            .throwsException(typeReference(ProcedureException.class));
    private static final MethodDeclaration.Builder AGGREGATION_RESULT =
            method(AnyValue.class, "result").throwsException(typeReference(ProcedureException.class));

    private ProcedureCompilation() {
        throw new UnsupportedOperationException("Do not instantiate");
    }

    /**
     * Generates code for a user-defined function.
     * <p>
     * Given a user-defined function defined by
     *
     * <pre>
     *     class MyClass {
     *       {@literal @}Context
     *        public Log log;
     *
     *       {@literal @}UserFunction
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
     *         public static FieldSetter SETTER_0;
     *
     *         public AnyValue apply(Context ctx, AnyValue[] input) {
     *              try {
     *                  MyClass userClass = new MyClass();
     *                  userClass.log = (Log) SETTER_0.get(ctx);
     *                  return Values.doubleValue(userClass.addPi( ((NumberValue) input[0]).longValue() );
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
     * @param parentClassLoader the classloader to resolve with.
     * @return a CallableUserFunction delegating to the underlying user-defined function.
     * @throws ProcedureException if something went wrong when compiling the user-defined function.
     */
    static CallableUserFunction compileFunction(
            UserFunctionSignature signature,
            List<FieldSetter> fieldSetters,
            Method methodToCall,
            ClassLoader parentClassLoader)
            throws ProcedureException {

        ClassHandle handle;
        try {
            CodeGenerator codeGenerator = codeGenerator(parentClassLoader);
            try (ClassGenerator generator =
                    codeGenerator.generateClass(PACKAGE, className(signature), CallableUserFunction.class)) {
                // static fields
                FieldReference signatureField =
                        generator.publicStaticField(typeReference(UserFunctionSignature.class), SIGNATURE_NAME);
                List<FieldReference> fieldsToSet = createContextSetters(fieldSetters, generator);

                // CallableUserFunction::apply
                try (CodeBlock method = generator.generate(USER_FUNCTION)) {
                    try (var body = method.tryCatch(
                            onError -> onError(onError, format("function `%s`", signature.name())),
                            param(Throwable.class, "T"))) {
                        functionBody(body, fieldSetters, fieldsToSet, methodToCall);
                    }
                }

                // CallableUserFunction::signature
                try (CodeBlock method = generator.generateMethod(UserFunctionSignature.class, "signature")) {
                    method.returns(getStatic(signatureField));
                }

                handle = generator.handle();
            }
            Class<?> clazz = handle.loadClass();

            // set all static fields
            setAllStaticFields(signature, fieldSetters, clazz);

            return (CallableUserFunction) clazz.getConstructor().newInstance();
        } catch (Throwable e) {
            throw new ProcedureException(
                    Status.Procedure.ProcedureRegistrationFailed,
                    e,
                    "Failed to compile function defined in `%s`: %s",
                    methodToCall.getDeclaringClass().getSimpleName(),
                    e.getMessage());
        }
    }

    /**
     * Generates code for a user-defined procedure.
     * <p>
     * Given a user-defined function defined by
     *
     * <pre>
     *     class MyClass {
     *       {@literal @}Context
     *        public Log log;
     *
     *       {@literal @}Procedure
     *        public Stream<MyOut> myStream(long value) {
     *            ...
     *        }
     *     }
     * </pre>
     * <p>
     * we will generate something like
     *
     * <pre>
     *     class GeneratedMyStream implements CallableProcedure {
     *         public static ProcedureSignature SIGNATURE;
     *         public static FieldSetter SETTER_0;
     *
     *          RawIterator<AnyValue[], ProcedureException> apply( Context ctx, AnyValue[] in, ResourceMonitor monitor ) throws ProcedureException {
     *              try {
     *                  MyClass userClass = new MyClass();
     *                  userClass.log = (Log) SETTER_0.get(ctx);
     *                  Stream<MyOut> fromUser = userClass.myStream(((NumberValue) input[0]).longValue() );
     *                  return new GeneratedIterator(fromUser, tracker, SIGNATURE);
     *              } catch (Throwable T) {
     *                  throw new ProcedureException([appropriate error msg], T);
     *              }
     *         }
     *
     *         public ProcedureSignature signature() {return SIGNATURE;}
     *     }
     * </pre>
     * <p>
     * where the static fields are set once during loading via reflection and where the <tt>GeneratedIterator</tt>
     * implements the appropriate mapping from user-types Object[] to AnyValue[].
     *
     * @param signature the signature of the procedure
     * @param fieldSetters the fields to set before each call.
     * @param methodToCall the method to call
     * @param parentClassLoader the classloader to resolve with.
     * @return a CallableProcedure delegating to the underlying procedure method.
     * @throws ProcedureException if something went wrong when compiling the user-defined function.
     */
    static CallableProcedure compileProcedure(
            ProcedureSignature signature,
            List<FieldSetter> fieldSetters,
            Method methodToCall,
            ClassLoader parentClassLoader)
            throws ProcedureException {

        ClassHandle handle;
        try {
            CodeGenerator codeGenerator = codeGenerator(parentClassLoader);
            Class<?> iterator = generateIterator(codeGenerator, procedureType(methodToCall));

            try (ClassGenerator generator =
                    codeGenerator.generateClass(PACKAGE, className(signature), CallableProcedure.class)) {
                // static fields
                FieldReference signatureField =
                        generator.publicStaticField(typeReference(ProcedureSignature.class), SIGNATURE_NAME);
                List<FieldReference> fieldsToSet = createContextSetters(fieldSetters, generator);

                // CallableProcedure::apply
                try (CodeBlock method = generator.generate(USER_PROCEDURE)) {
                    try (var body = method.tryCatch(
                            onError -> onError(onError, format("procedure `%s`", signature.name())),
                            param(Throwable.class, "T"))) {
                        procedureBody(body, fieldSetters, fieldsToSet, signatureField, methodToCall, iterator);
                    }
                }

                // CallableUserFunction::signature
                try (CodeBlock method = generator.generateMethod(ProcedureSignature.class, "signature")) {
                    method.returns(getStatic(signatureField));
                }

                handle = generator.handle();
            }
            Class<?> clazz = handle.loadClass();

            // set all static fields
            setAllStaticFields(signature, fieldSetters, clazz);
            return (CallableProcedure) clazz.getConstructor().newInstance();
        } catch (Throwable e) {
            throw new ProcedureException(
                    Status.Procedure.ProcedureRegistrationFailed,
                    e,
                    "Failed to compile procedure defined in `%s`: %s",
                    methodToCall.getDeclaringClass().getSimpleName(),
                    e.getMessage());
        }
    }

    /**
     * Generates code for a user-defined aggregation function.
     * <p>
     * Given a user-defined aggregation function defined by
     *
     * <pre>
     *     class MyClass {
     *       {@literal @}Context
     *        public Log log;
     *
     *       {@literal @}UserAggregationFunction
     *        public Adder create() {
     *            return new Adder;
     *        }
     *     }
     *
     *     class Adder {
     *       private long sum;
     *       {@literal @}UserAggregationUpdate
     *       public void update(long in) {
     *           sum += in;
     *       }
     *       {@literal @}UserAggregationResult
     *       public long result() {
     *         return sum;
     *       }
     *    }
     *     }
     * </pre>
     * <p>
     * we will generate two classe looking something like
     *
     * <pre>
     *     class Generated implements CallableUserAggregationFunction {
     *         public static UserFunctionSignature SIGNATURE;
     *         public static FieldSetter SETTER_0;
     *
     *         public UserAggregator create(Context ctx) {
     *              try {
     *                  MyClass userClass = new MyClass();
     *                  userClass.log = (Log) SETTER_0.get(ctx);
     *                  return new GeneratedAdder(userClass.create());
     *              } catch (Throwable T) {
     *                  throw new ProcedureException([appropriate error msg], T);
     *              }
     *         }
     *
     *         public UserFunctionSignature signature() {return SIGNATURE;}
     *     }
     *     class GeneratedAdder implements UserAggregator {
     *       private Adder adder;
     *
     *       GeneratedAdder(Adder adder) {
     *         this.adder = adder;
     *       }
     *
     *       void update(AnyValue[] in) {
     *          adder.update(((NumberValue) in).longValue());
     *       }
     *
     *       AnyValue result() {
     *          return adder.result(Values.longValue(adder.result());
     *       }
     *     }
     * </pre>
     * <p>
     * where the static fields are set once during loading via reflection.
     *
     * @param signature the signature of the user-defined function
     * @param fieldSetters the fields to set before each call.
     * @param create the method that creates the aggregator
     * @param update the update method of the aggregator
     * @param result the result method of the aggregator
     * @param parentClassLoader the classloader to resolve with.
     * @return a CallableUserFunction delegating to the underlying user-defined function.
     * @throws ProcedureException if something went wrong when compiling the user-defined function.
     */
    static CallableUserAggregationFunction compileAggregation(
            UserFunctionSignature signature,
            List<FieldSetter> fieldSetters,
            Method create,
            Method update,
            Method result,
            ClassLoader parentClassLoader)
            throws ProcedureException {

        ClassHandle handle;
        try {
            CodeGenerator codeGenerator = codeGenerator(parentClassLoader);
            Class<?> aggregator = generateAggregator(codeGenerator, update, result, signature);
            try (ClassGenerator generator = codeGenerator.generateClass(
                    CallableUserAggregationFunction.BasicUserAggregationFunction.class,
                    PACKAGE,
                    className(signature),
                    CallableUserAggregationFunction.class)) {
                // static fields
                List<FieldReference> fieldsToSet = createContextSetters(fieldSetters, generator);

                // constructor
                try (CodeBlock constructor =
                        generator.generateConstructor(param(UserFunctionSignature.class, "signature"))) {
                    constructor.expression(invokeSuper(
                            typeReference(CallableUserAggregationFunction.BasicUserAggregationFunction.class),
                            constructor.load("signature")));
                }

                // CallableUserAggregationFunction::create
                try (CodeBlock method = generator.generate(AGGREGATION_CREATE)) {
                    try (var body = method.tryCatch(
                            onError -> onError(onError, format("function `%s`", signature.name())),
                            param(Throwable.class, "T"))) {
                        createAggregationBody(body, fieldSetters, fieldsToSet, create, aggregator);
                    }
                }

                handle = generator.handle();
            }
            Class<?> clazz = handle.loadClass();

            // set all static fields
            setAllStaticFields(fieldSetters, clazz);

            return (CallableUserAggregationFunction)
                    clazz.getConstructor(UserFunctionSignature.class).newInstance(signature);
        } catch (Throwable e) {
            throw new ProcedureException(
                    Status.Procedure.ProcedureRegistrationFailed,
                    e,
                    "Failed to compile function defined in `%s`: %s",
                    create.getDeclaringClass().getSimpleName(),
                    e.getMessage());
        }
    }

    /**
     * Used by generated code. Needs to be public.
     * @param throwable The thrown exception
     * @param typeAndName the type and name of the caller, e.g "function `my.udf`"
     * @return an exception with an appropriate message.
     */
    public static ProcedureException rethrowProcedureException(Throwable throwable, String typeAndName) {
        if (throwable instanceof ProcedureException) {
            return (ProcedureException) throwable;
        }
        if (throwable instanceof Status.HasStatus) {
            return new ProcedureException(
                    ((Status.HasStatus) throwable).status(), throwable, throwable.getMessage(), throwable);
        } else {
            Throwable cause = getRootCause(throwable);
            return new ProcedureException(
                    Status.Procedure.ProcedureCallFailed,
                    throwable,
                    "Failed to invoke %s: %s",
                    typeAndName,
                    "Caused by: " + (cause != null ? cause : throwable));
        }
    }

    /**
     * Byte arrays needs special treatment since it is not a proper Cypher type
     * @param input either a ByteArray or ListValue of bytes
     * @return input value converted to a byte[]
     */
    public static byte[] toByteArray(AnyValue input) {
        if (input instanceof ByteArray) {
            return ((ByteArray) input).asObjectCopy();
        }
        if (input instanceof SequenceValue list) {
            if (list.iterationPreference() == RANDOM_ACCESS) {
                byte[] bytes = new byte[list.length()];
                for (int a = 0; a < bytes.length; a++) {
                    bytes[a] = asByte(list.value(a));
                }
                return bytes;
            } else {
                // list.length may have linear complexity, still worth doing it upfront
                byte[] bytes = new byte[list.length()];
                int i = 0;
                for (AnyValue anyValue : list) {
                    bytes[i++] = asByte(anyValue);
                }

                return bytes;
            }
        } else {
            throw new IllegalArgumentException(
                    "Cannot convert " + input.getClass().getSimpleName() + " to byte[] for input to procedure");
        }
    }

    /**
     * Generates a tailored iterator mapping from the user-stream to the internal RawIterator
     *
     * The generated iterator extends {@link BaseStreamIterator} and generates a tailored
     * {@link BaseStreamIterator#map(Object)} method based on the signature of the user procedure.
     *
     * @param codeGenerator the current generator
     * @param outputType the output type of the user procedure
     * @return a tailored iterator with appropriate code mappings
     */
    private static Class<?> generateIterator(CodeGenerator codeGenerator, Class<?> outputType) {
        if (outputType.equals(void.class)) {
            return VOID_ITERATOR.getClass();
        }

        ClassHandle handle;
        try (ClassGenerator generator =
                codeGenerator.generateClass(BaseStreamIterator.class, PACKAGE, iteratorName(outputType))) {
            FieldReference context = generator.field(Context.class, "ctx");
            try (CodeBlock constructor = generator.generateConstructor(
                    param(Stream.class, "stream"),
                    param(ResourceMonitor.class, "monitor"),
                    param(ProcedureSignature.class, "signature"),
                    param(Context.class, "ctx"))) {
                constructor.expression(invokeSuper(
                        typeReference(BaseStreamIterator.class),
                        constructor.load("stream"),
                        constructor.load("monitor"),
                        constructor.load("signature")));
                constructor.put(constructor.self(), context, constructor.load("ctx"));
            }

            try (CodeBlock method = generator.generate(method(AnyValue[].class, "map", param(Object.class, "in")))) {
                method.assign(outputType, "casted", cast(outputType, method.load("in")));
                // we know all fields are properly typed
                List<Field> fields = ProcedureOutputSignatureCompiler.instanceFields(outputType);
                Expression[] mapped = new Expression[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    Field f = fields.get(i);
                    if (outputType.isRecord()) {
                        // Records must be accessed through the public method with the same name as the field
                        mapped[i] = toAnyValue(
                                invoke(method.load("casted"), methodReference(outputType, f.getType(), f.getName())),
                                f.getType(),
                                get(method.self(), context));
                    } else {
                        mapped[i] = toAnyValue(
                                get(method.load("casted"), field(f)), f.getType(), get(method.self(), context));
                    }
                }
                method.returns(Expression.newInitializedArray(typeReference(AnyValue.class), mapped));
            }
            handle = generator.handle();
        }

        try {
            return handle.loadClass();
        } catch (
                CompilationFailureException
                        e) { // We are being called from a lambda so it'll have to do with runtime exceptions here
            throw new RuntimeException("Failed to generate iterator", e);
        }
    }

    private static Class<?> generateAggregator(
            CodeGenerator codeGenerator, Method update, Method result, UserFunctionSignature signature) {
        assert update.getDeclaringClass().equals(result.getDeclaringClass());

        Class<?> userAggregatorClass = update.getDeclaringClass();

        ClassHandle handle;
        try (ClassGenerator generator = codeGenerator.generateClass(
                PACKAGE,
                "Aggregator" + userAggregatorClass.getSimpleName() + System.nanoTime(),
                UserAggregator.class)) {
            FieldReference aggregator = generator.field(userAggregatorClass, "aggregator");
            FieldReference context = generator.field(Context.class, "ctx");
            // constructor
            try (CodeBlock constructor = generator.generateConstructor(
                    param(userAggregatorClass, "aggregator"), param(Context.class, "ctx"))) {
                constructor.expression(invokeSuper(OBJECT));
                constructor.put(constructor.self(), aggregator, constructor.load("aggregator"));
                constructor.put(constructor.self(), context, constructor.load("ctx"));
            }

            // update
            try (CodeBlock block = generator.generate(AGGREGATION_UPDATE)) {
                try (var body = block.tryCatch(
                        onError -> onError(onError, format("function `%s`", signature.name())),
                        param(Throwable.class, "T"))) {
                    body.expression(invoke(
                            get(body.self(), aggregator),
                            methodReference(update),
                            parameters(body, update, get(body.self(), context))));
                }
            }

            // result
            try (CodeBlock block = generator.generate(AGGREGATION_RESULT)) {
                try (var body = block.tryCatch(
                        onError -> onError(onError, format("function `%s`", signature.name())),
                        param(Throwable.class, "T"))) {
                    body.returns(toAnyValue(
                            body,
                            "result",
                            invoke(get(body.self(), aggregator), methodReference(result)),
                            result.getReturnType(),
                            get(body.self(), context)));
                }
            }

            handle = generator.handle();
        }

        try {
            return handle.loadClass();
        } catch (
                CompilationFailureException
                        e) { // We are being called from a lambda so it'll have to do with runtime exceptions here
            throw new RuntimeException("Failed to generate iterator", e);
        }
    }

    private static String className(UserFunctionSignature signature) {
        return format("GeneratedFunction_%s%d", signature.name().name(), System.nanoTime());
    }

    private static String className(ProcedureSignature signature) {
        return format("GeneratedProcedure_%s%d", signature.name().name(), System.nanoTime());
    }

    private static String iteratorName(Class<?> out) {
        return format("Iterator_%s%d", out.getSimpleName(), System.nanoTime());
    }

    /**
     * Generates the actual body of the function. Generated the code will look something like:
     * <p>
     * userClass.field1 = (type1) SETTER_1.get(context)
     * userClass.field2 = (type2) SETTER_2.get(context)
     * ...
     * return [CONVERT TO AnyVALUE](userClass.call( [CONVERT_TO_JAVA] input[0], ... );
     */
    private static void functionBody(
            CodeBlock block, List<FieldSetter> fieldSetters, List<FieldReference> fieldsToSet, Method methodToCall) {
        // generate: `UserClass userClass = new UserClass();
        block.assign(
                typeReference(methodToCall.getDeclaringClass()),
                USER_CLASS,
                invoke(
                        newInstance(methodToCall.getDeclaringClass()),
                        constructorReference(methodToCall.getDeclaringClass())));
        // inject fields in user class
        injectFields(block, fieldSetters, fieldsToSet);

        // get parameters to the user-method to call
        Expression[] parameters = parameters(block, methodToCall, block.load("ctx"));

        // call the actual function
        block.assign(
                methodToCall.getReturnType(),
                "fromFunction",
                invoke(block.load(USER_CLASS), methodReference(methodToCall), parameters));
        block.returns(toAnyValue(block.load("fromFunction"), methodToCall.getReturnType(), block.load("ctx")));
    }

    private static void procedureBody(
            CodeBlock block,
            List<FieldSetter> fieldSetters,
            List<FieldReference> fieldsToSet,
            FieldReference signature,
            Method methodToCall,
            Class<?> iterator) {
        // generate: `UserClass userClass = new UserClass();
        block.assign(
                typeReference(methodToCall.getDeclaringClass()),
                USER_CLASS,
                invoke(
                        newInstance(methodToCall.getDeclaringClass()),
                        constructorReference(methodToCall.getDeclaringClass())));
        injectFields(block, fieldSetters, fieldsToSet);
        Expression[] parameters = parameters(block, methodToCall, block.load("ctx"));

        if (iterator.equals(
                VOID_ITERATOR.getClass())) { // if we are calling a void method we just need to call and return empty
            block.expression(invoke(block.load(USER_CLASS), methodReference(methodToCall), parameters));
            block.returns(getStatic(FieldReference.field(
                    typeReference(ProcedureCompilation.class), typeReference(RawIterator.class), "VOID_ITERATOR")));
        } else { // here we must both call and map stream to an iterator
            block.assign(
                    parameterizedType(Stream.class, procedureType(methodToCall)),
                    "fromProcedure",
                    invoke(block.load(USER_CLASS), methodReference(methodToCall), parameters));
            block.returns(invoke(
                    newInstance(iterator),
                    constructorReference(
                            iterator, Stream.class, ResourceMonitor.class, ProcedureSignature.class, Context.class),
                    block.load("fromProcedure"),
                    block.load("monitor"),
                    getStatic(signature),
                    block.load("ctx")));
        }
    }

    private static Class<?> procedureType(Method method) {
        // return type is always Stream<> or void
        if (method.getReturnType().equals(void.class)) {
            return void.class;
        } else {
            ParameterizedType returnType = (ParameterizedType) method.getGenericReturnType();
            return (Class<?>) returnType.getActualTypeArguments()[0];
        }
    }

    private static void createAggregationBody(
            CodeBlock block,
            List<FieldSetter> fieldSetters,
            List<FieldReference> fieldsToSet,
            Method createMethod,
            Class<?> aggregator) {
        // generate: `UserClass userClass = new UserClass();
        block.assign(
                typeReference(createMethod.getDeclaringClass()),
                USER_CLASS,
                invoke(
                        newInstance(createMethod.getDeclaringClass()),
                        constructorReference(createMethod.getDeclaringClass())));
        // inject fields in user class
        injectFields(block, fieldSetters, fieldsToSet);

        // call the actual function
        block.assign(
                createMethod.getReturnType(),
                "fromUser",
                invoke(block.load(USER_CLASS), methodReference(createMethod)));
        block.returns(invoke(
                newInstance(aggregator),
                constructorReference(aggregator, createMethod.getReturnType(), Context.class),
                block.load("fromUser"),
                block.load("ctx")));
    }

    /**
     * Handle errors by redirecting to {@link #rethrowProcedureException(Throwable, String)}  }
     */
    private static void onError(CodeBlock block, String typeAndName) {

        block.throwException(invoke(
                methodReference(
                        ProcedureCompilation.class,
                        ProcedureException.class,
                        "rethrowProcedureException",
                        Throwable.class,
                        String.class),
                block.load("T"),
                constant(typeAndName)));
    }

    /**
     * Used for properly setting a primitive field
     *
     * For example say that the class has a field of type long. Then we will generate the RHS of.
     * <pre>
     *     userClass.longField = ((Long) invoke).longValue()
     * </pre>
     *
     * @param fieldType the type of the field
     * @param invoke the expression used to set the field
     * @return an expression where casting and boxing/unboxing has been taken care of.
     */
    private static Expression unboxIfNecessary(Class<?> fieldType, Expression invoke) {
        if (fieldType.isPrimitive()) {
            return unbox(cast(toBoxedType(typeReference(fieldType)), invoke));
        } else {
            return cast(fieldType, invoke);
        }
    }

    private static CodeGenerator codeGenerator(ClassLoader parentClassLoader)
            throws CodeGenerationNotSupportedException {
        if (DEBUG) {
            return generateCode(parentClassLoader, SOURCECODE, PRINT_SOURCE);
        } else {
            return generateCode(parentClassLoader, BYTECODE);
        }
    }

    private static byte asByte(AnyValue value) {
        if (value instanceof ByteValue) {
            return ((ByteValue) value).value();
        } else {
            throw new IllegalArgumentException(
                    "Cannot convert " + value.map(new DefaultValueMapper(null)) + " to byte for input to procedure");
        }
    }

    /**
     * Takes an expression evaluating to one of the supported java values and turns
     * it into the corresponding AnyValue
     *
     * The expression is evaluated once and stored in a local variable
     *
     * @param block the surrounding code block in which the expression will be evaluated
     * @param expressionVariableName the name of the local variable in which the evaluated expression will be stored.
     * @param expression the expression to evaluate
     * @param userType the type of the expression to map
     * @return an expression properly mapped to AnyValue
     */
    private static Expression toAnyValue(
            CodeBlock block,
            String expressionVariableName,
            Expression expression,
            Class<?> userType,
            Expression context) {
        block.assign(userType, expressionVariableName, expression);
        return toAnyValue(block.load(expressionVariableName), userType, context);
    }

    /**
     * Takes an expression evaluating to one of the supported java values and turns
     * it into the corresponding AnyValue
     *
     * The expression may be evaluated twice, depending on the userType.
     * If the expression needs to be evaluated only once, use {@link #toAnyValue(CodeBlock, String, Expression, Class, Expression)}.
     *
     * @param expression the expression to evaluate
     * @param userType the type of the expression to map
     * @return an expression properly mapped to AnyValue
     */
    private static Expression toAnyValue(Expression expression, Class<?> userType, Expression context) {
        if (AnyValue.class.isAssignableFrom(userType)) {
            return nullCheck(expression, cast(userType, expression));
        }

        String type = userType.getCanonicalName();
        if (type.equals(LONG)) {
            return invoke(methodReference(Values.class, LongValue.class, "longValue", long.class), expression);
        } else if (type.equals(BOXED_LONG)) {
            return nullCheck(
                    expression,
                    invoke(methodReference(Values.class, LongValue.class, "longValue", long.class), unbox(expression)));
        } else if (type.equals(DOUBLE)) {
            return invoke(methodReference(Values.class, DoubleValue.class, "doubleValue", double.class), expression);
        } else if (type.equals(BOXED_DOUBLE)) {
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(Values.class, DoubleValue.class, "doubleValue", double.class),
                            unbox(expression)));
        } else if (type.equals(NUMBER)) {
            return nullCheck(
                    expression,
                    invoke(methodReference(Values.class, NumberValue.class, "numberValue", Number.class), expression));
        } else if (type.equals(BOOLEAN)) {
            return invoke(methodReference(Values.class, BooleanValue.class, "booleanValue", boolean.class), expression);
        } else if (type.equals(BOXED_BOOLEAN)) {
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(Values.class, BooleanValue.class, "booleanValue", boolean.class),
                            unbox(expression)));
        } else if (type.equals(STRING)) {
            return invoke(methodReference(Values.class, Value.class, "stringOrNoValue", String.class), expression);
        } else if (type.equals(BYTE_ARRAY)) {
            return nullCheck(
                    expression,
                    invoke(methodReference(Values.class, ByteArray.class, "byteArray", byte[].class), expression));
        } else if (type.equals(LIST)) {
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(
                                    ValueUtils.class, ListValue.class, "asListValue", Iterable.class, boolean.class),
                            expression,
                            constant(true)));
        } else if (type.equals(MAP)) {
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(ValueUtils.class, MapValue.class, "asMapValue", Map.class, boolean.class),
                            expression,
                            constant(true)));
        } else if (type.equals(ZONED_DATE_TIME)) {
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(DateTimeValue.class, DateTimeValue.class, "datetime", ZonedDateTime.class),
                            expression));
        } else if (type.equals(OFFSET_TIME)) {
            return nullCheck(
                    expression,
                    invoke(methodReference(TimeValue.class, TimeValue.class, "time", OffsetTime.class), expression));
        } else if (type.equals(LOCAL_DATE)) {
            return nullCheck(
                    expression,
                    invoke(methodReference(DateValue.class, DateValue.class, "date", LocalDate.class), expression));
        } else if (type.equals(LOCAL_TIME)) {
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(LocalTimeValue.class, LocalTimeValue.class, "localTime", LocalTime.class),
                            expression));
        } else if (type.equals(LOCAL_DATE_TIME)) {
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(
                                    LocalDateTimeValue.class,
                                    LocalDateTimeValue.class,
                                    "localDateTime",
                                    LocalDateTime.class),
                            expression));
        } else if (type.equals(TEMPORAL_AMOUNT)) {
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(Values.class, DurationValue.class, "durationValue", TemporalAmount.class),
                            expression));
        } else if (type.equals(NODE)) {
            Expression internalTransaction = invoke(
                    context, methodReference(Context.class, InternalTransaction.class, "internalTransactionOrNull"));
            Expression dbValidation = invoke(
                    internalTransaction,
                    methodReference(InternalTransaction.class, Entity.class, "validateSameDB", Entity.class),
                    expression);
            Expression getNode = ternary(equal(internalTransaction, constant(null)), expression, dbValidation);
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(
                                    ValueUtils.class, VirtualNodeValue.class, "maybeWrapNodeEntity", Node.class),
                            getNode));
        } else if (type.equals(RELATIONSHIP)) {
            Expression internalTransaction = invoke(
                    context, methodReference(Context.class, InternalTransaction.class, "internalTransactionOrNull"));
            Expression dbValidation = invoke(
                    internalTransaction,
                    methodReference(InternalTransaction.class, Entity.class, "validateSameDB", Entity.class),
                    expression);
            Expression getRelationship = ternary(equal(internalTransaction, constant(null)), expression, dbValidation);
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(
                                    ValueUtils.class,
                                    VirtualRelationshipValue.class,
                                    "maybeWrapRelationshipEntity",
                                    Relationship.class),
                            getRelationship));
        } else if (type.equals(PATH)) {
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(ValueUtils.class, VirtualPathValue.class, "maybeWrapPath", Path.class),
                            expression));
        } else if (type.equals(POINT)) {
            return nullCheck(
                    expression,
                    invoke(
                            methodReference(ValueUtils.class, PointValue.class, "asPointValue", Point.class),
                            expression));
        } else {
            return invoke(
                    methodReference(ValueUtils.class, AnyValue.class, "of", Object.class, boolean.class),
                    expression,
                    constant(true));
        }
    }

    /**
     * Converts from an `AnyValue` to the type specified by the procedure or function.
     * <p>
     * In a lot of cases we can figure out the conversion statically, for example `LongValue` will
     * be turned into ((LongValue) value).longValue() etc. For composite types as Lists and Maps and also
     * for Graph types such as Nodes, Relationships and Paths we will use a ValueMapper.
     *
     * @param expectedType the java type expected by the procedure or function
     * @param expression an expression that will evaluate to an AnyValue
     * @param block The current code block.
     * @param context The current context.
     * @return an expression properly typed to be consumed by function or procedure
     */
    private static Expression fromAnyValue(
            Class<?> expectedType, Expression expression, CodeBlock block, Expression context) {
        if (AnyValue.class.isAssignableFrom(expectedType)) {
            return cast(expectedType, expression);
        }

        String type = expectedType.getCanonicalName();
        if (type.equals(LONG)) {
            return invoke(
                    cast(NumberValue.class, expression), methodReference(NumberValue.class, long.class, "longValue"));
        } else if (type.equals(BOXED_LONG)) {
            return noValueCheck(
                    expression,
                    box(invoke(
                            cast(NumberValue.class, expression),
                            methodReference(NumberValue.class, long.class, "longValue"))));
        } else if (type.equals(DOUBLE)) {
            return invoke(
                    cast(NumberValue.class, expression),
                    methodReference(NumberValue.class, double.class, "doubleValue"));
        } else if (type.equals(BOXED_DOUBLE)) {
            return noValueCheck(
                    expression,
                    box(invoke(
                            cast(NumberValue.class, expression),
                            methodReference(NumberValue.class, double.class, "doubleValue"))));
        } else if (type.equals(NUMBER)) {
            return noValueCheck(
                    expression,
                    invoke(
                            cast(NumberValue.class, expression),
                            methodReference(NumberValue.class, Number.class, "asObjectCopy")));
        } else if (type.equals(BOOLEAN)) {
            return invoke(
                    cast(BooleanValue.class, expression),
                    methodReference(BooleanValue.class, boolean.class, "booleanValue"));
        } else if (type.equals(BOXED_BOOLEAN)) {
            return noValueCheck(
                    expression,
                    box(invoke(
                            cast(BooleanValue.class, expression),
                            methodReference(BooleanValue.class, boolean.class, "booleanValue"))));
        } else if (type.equals(STRING)) {
            return noValueCheck(
                    expression,
                    invoke(
                            cast(TextValue.class, expression),
                            methodReference(TextValue.class, String.class, "stringValue")));
        } else if (type.equals(BYTE_ARRAY)) {
            return noValueCheck(
                    expression,
                    invoke(
                            methodReference(ProcedureCompilation.class, byte[].class, "toByteArray", AnyValue.class),
                            expression));
        } else if (type.equals(ZONED_DATE_TIME)) {
            return noValueCheck(
                    expression,
                    cast(
                            ZonedDateTime.class,
                            invoke(
                                    cast(DateTimeValue.class, expression),
                                    methodReference(DateTimeValue.class, Temporal.class, "asObjectCopy"))));
        } else if (type.equals(OFFSET_TIME)) {
            return noValueCheck(
                    expression,
                    cast(
                            OffsetTime.class,
                            invoke(
                                    cast(TimeValue.class, expression),
                                    methodReference(TimeValue.class, Temporal.class, "asObjectCopy"))));
        } else if (type.equals(LOCAL_DATE)) {
            return noValueCheck(
                    expression,
                    cast(
                            LocalDate.class,
                            invoke(
                                    cast(DateValue.class, expression),
                                    methodReference(DateValue.class, Temporal.class, "asObjectCopy"))));
        } else if (type.equals(LOCAL_TIME)) {
            return noValueCheck(
                    expression,
                    cast(
                            LocalTime.class,
                            invoke(
                                    cast(LocalTimeValue.class, expression),
                                    methodReference(LocalTimeValue.class, Temporal.class, "asObjectCopy"))));
        } else if (type.equals(LOCAL_DATE_TIME)) {
            return noValueCheck(
                    expression,
                    cast(
                            LocalDateTime.class,
                            invoke(
                                    cast(LocalDateTimeValue.class, expression),
                                    methodReference(LocalDateTimeValue.class, Temporal.class, "asObjectCopy"))));
        } else if (type.equals(TEMPORAL_AMOUNT)) {
            return noValueCheck(expression, cast(TemporalAmount.class, expression));
        } else if (type.equals(POINT)) {
            return noValueCheck(
                    expression,
                    invoke(
                            cast(PointValue.class, expression),
                            methodReference(PointValue.class, Point.class, "asObjectCopy")));
        } else {
            return cast(
                    expectedType,
                    invoke(
                            expression,
                            methodReference(AnyValue.class, Object.class, "map", ValueMapper.class),
                            invoke(context, methodReference(Context.class, ValueMapper.class, "valueMapper"))));
        }
    }

    /**
     * toCheck == null ? Values.NO_VALUE : onNotNull;
     */
    private static Expression nullCheck(Expression toCheck, Expression onNotNull) {
        return ternary(equal(toCheck, constant(null)), noValue(), onNotNull);
    }

    /**
     * toCheck == NO_VALUE ? null : onNotNoValue;
     */
    private static Expression noValueCheck(Expression toCheck, Expression onNotNoValue) {
        return ternary(equal(toCheck, noValue()), constant(null), onNotNoValue);
    }

    /**
     * @return Values.NO_VALUE;
     */
    private static Expression noValue() {
        return getStatic(field(typeReference(Values.class), typeReference(Value.class), "NO_VALUE"));
    }

    private static Expression[] parameters(CodeBlock block, Method methodToCall, Expression context) {
        Class<?>[] parameterTypes = methodToCall.getParameterTypes();
        Expression[] parameters = new Expression[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            parameters[i] =
                    fromAnyValue(parameterTypes[i], arrayLoad(block.load("input"), constant(i)), block, context);
        }
        return parameters;
    }

    private static void injectFields(
            CodeBlock block, List<FieldSetter> fieldSetters, List<FieldReference> fieldsToSet) {
        for (int i = 0; i < fieldSetters.size(); i++) {
            FieldSetter setter = fieldSetters.get(i);
            Field field = setter.field();
            Class<?> fieldType = field.getType();
            block.put(
                    block.load(USER_CLASS),
                    field(field),
                    unboxIfNecessary(
                            fieldType,
                            invoke(
                                    getStatic(fieldsToSet.get(i)),
                                    methodReference(
                                            typeReference(FieldSetter.class),
                                            OBJECT,
                                            "get",
                                            typeReference(Context.class)),
                                    block.load("ctx"))));
        }
    }

    private static List<FieldReference> createContextSetters(List<FieldSetter> fieldSetters, ClassGenerator generator) {
        List<FieldReference> fieldsToSet = new ArrayList<>(fieldSetters.size());
        for (int i = 0; i < fieldSetters.size(); i++) {
            fieldsToSet.add(generator.publicStaticField(typeReference(FieldSetter.class), "SETTER_" + i));
        }
        return fieldsToSet;
    }

    private static void setAllStaticFields(Object signature, List<FieldSetter> fieldSetters, Class<?> clazz)
            throws IllegalAccessException, NoSuchFieldException {
        clazz.getDeclaredField(SIGNATURE_NAME).set(null, signature);
        setAllStaticFields(fieldSetters, clazz);
    }

    private static void setAllStaticFields(List<FieldSetter> fieldSetters, Class<?> clazz)
            throws IllegalAccessException, NoSuchFieldException {
        for (int i = 0; i < fieldSetters.size(); i++) {
            clazz.getDeclaredField("SETTER_" + i).set(null, fieldSetters.get(i));
        }
    }
}
