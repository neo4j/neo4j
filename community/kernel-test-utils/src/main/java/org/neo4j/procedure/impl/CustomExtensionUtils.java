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

import static java.lang.reflect.Modifier.PUBLIC;
import static net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy.Default.NO_CONSTRUCTORS;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.asm.MemberAttributeExtension;
import net.bytebuddy.description.annotation.AnnotationDescription;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.Context;
import org.objectweb.asm.Opcodes;

/**
 * Create extension JARs without accidental classloading
 * <p>&nbsp;</p>
 * This utility class provides a number of methods, that allows its users to be able to
 * conveniently create JARs containing extensions and procedures, without accidentally
 * contaminating the classloader.
 * <p>&nbsp;</p>
 * Accidental contamination of the classloader can occur, e.g. when defining a class such as:
 * <pre>
 *     class Procedure {
 *         ...
 *     }
 * </pre>
 * Unless care is taken, this class can accidentally contaminate the classloader. E.g. if the class is referenced
 * using {@code Procedure.class} in test creating a test artifact (see {@link org.neo4j.test.jar.JarBuilder}) the
 * test classloader will find and load the referenced class.
 * <p>&nbsp;</p>
 * The accidental classloading can prevent the discovery of errors, especially related to reloading of procedures,
 * where we must be particularly careful with what classloader is used.
 * <p>&nbsp;</p>
 * Another class that is prone to cause pollution of other tests, is {@link org.neo4j.kernel.extension.ExtensionFactory}.
 * Since a test implementation of a {@link org.neo4j.kernel.extension.ExtensionFactory} probably lives on the test
 * classloader's classpath, these factories are discovered when performing service loading (for example in tests utilizing
 * {@link org.neo4j.test.extension.DbmsExtension}). This has the potential for causing issues in unrelated tests.
 * <p>&nbsp;</p>
 * By using the utilities that this class provides, the above issues can be circumvented.
 * This is achieved thanks to {@link net.bytebuddy.ByteBuddy}s facilities for generating byte code programmatically,
 * without letting the classloader seeing the classes.
 * */
public final class CustomExtensionUtils {

    private CustomExtensionUtils() {}

    public static final String PROCEDURE_NAME = "myCustomProcedure";
    public static final String CANONICAL_PROCEDURE_NAME = packageName(PROCEDURE_NAME);
    public static final String LOG_MARKER = "HELLO WORLD";

    /** Create a set of unloaded classes that implement an extension
     *  and a {@link org.neo4j.kernel.extension.ExtensionFactory}.
     *
     * @return the unloaded classes
     */
    public static Extension createExtension() {
        var dependenciesInterfaceType = makeDependenciesInterfaceType();
        var dependenciesImplementationType = makeDependenciesImplementationType(dependenciesInterfaceType);
        var extensionType = makeExtensionType(dependenciesInterfaceType);
        var extensionFactoryType = makeExtensionFactoryType(dependenciesInterfaceType, extensionType);

        return new Extension(
                dependenciesInterfaceType, dependenciesImplementationType, extensionType, extensionFactoryType);
    }

    /** Create a set of unloaded classes that implement a procedure
     *  using the {@link org.neo4j.procedure.Procedure} annotation.
     *
     * @return the unloaded classes
     */
    public static Procedure createProcedure() {
        var outputType = makeOutputType();
        var procedureType = makeProcedureType(outputType);
        return new Procedure(outputType, procedureType);
    }

    /** Create a set of unloaded classes that implement a procedure
     *  using the {@link org.neo4j.procedure.Procedure} annotation and that
     *  also rely on an extension from an {@link org.neo4j.kernel.extension.ExtensionFactory}.
     *
     * @return the unloaded classes
     */
    public static ProcedureWithExtension createProcedureWithExtension() {
        var extensionTypes = createExtension();
        var outputType = makeOutputType();
        var procedureType = makeProcedureType(outputType, extensionTypes.extensionType);

        return new ProcedureWithExtension(extensionTypes, outputType, procedureType);
    }

    /** Create a JAR containing a set of unloaded classes that implement a procedure
     *  using the {@link org.neo4j.procedure.Procedure} annotation.
     */
    public static void createProcedureJar(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        createProcedure().toJar(path);
    }

    /** Create a JAR containing set of unloaded classes that implement a procedure
     *  using the {@link org.neo4j.procedure.Procedure} annotation and that
     *  also rely on an extension from an {@link org.neo4j.kernel.extension.ExtensionFactory}.
     * <p>&nbsp;</p>
     *  The JAR contains the necessary metadata to allow for service loading by the DBMS.
     */
    public static void createProcedureWithExtensionJar(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        createProcedureWithExtension().toJar(path);
    }

    public static class Extension {

        DynamicType.Unloaded<?> dependenciesInterfaceType;
        DynamicType.Unloaded<?> dependenciesImplementationType;

        DynamicType.Unloaded<?> extensionType;
        DynamicType.Unloaded<?> extensionFactoryType;

        private Extension(
                DynamicType.Unloaded<?> dependenciesInterfaceType,
                DynamicType.Unloaded<?> dependenciesImplementationType,
                DynamicType.Unloaded<?> extensionType,
                DynamicType.Unloaded<?> extensionFactoryType) {
            this.dependenciesInterfaceType = dependenciesInterfaceType;
            this.dependenciesImplementationType = dependenciesImplementationType;
            this.extensionType = extensionType;
            this.extensionFactoryType = extensionFactoryType;
        }

        public void toJar(Path pth) throws IOException {
            try (JarOutputStream jarOut = new JarOutputStream(Files.newOutputStream(pth))) {
                //noinspection DataFlowIssue
                addService(
                        jarOut,
                        ExtensionFactory.class.getCanonicalName(),
                        extensionFactoryType.getTypeDescription().getCanonicalName());
            }

            var file = pth.toFile();
            dependenciesInterfaceType.inject(file);
            extensionType.inject(file);
            extensionFactoryType.inject(file);
        }
    }

    public static class Procedure {

        DynamicType.Unloaded<?> outputType;
        DynamicType.Unloaded<?> procedureType;

        private Procedure(DynamicType.Unloaded<?> outputType, DynamicType.Unloaded<?> procedureType) {
            this.outputType = outputType;
            this.procedureType = procedureType;
        }

        public void toJar(Path pth) throws IOException {
            var file = pth.toFile();
            outputType.toJar(file);
            procedureType.inject(file);
        }
    }

    public static class ProcedureWithExtension {
        Extension extensionTypes;

        DynamicType.Unloaded<?> outputType;
        DynamicType.Unloaded<?> procedureType;

        private ProcedureWithExtension(
                Extension extensionTypes, DynamicType.Unloaded<?> outputType, DynamicType.Unloaded<?> procedureType) {
            this.extensionTypes = extensionTypes;

            this.outputType = outputType;
            this.procedureType = procedureType;
        }

        public void toJar(Path pth) throws IOException {
            extensionTypes.toJar(pth);
            var file = pth.toFile();
            outputType.inject(file);
            procedureType.inject(file);
        }
    }

    /***********************************************************************************
     * BELOW FOLLOWS BYTEBUDDY INSTRUCTIONS TO CREATE CLASSES THAT ARE NOT CLASSLOADED *
     ***********************************************************************************/
    private static DynamicType.Unloaded<?> makeOutputType() {
        /* Below translates to:
          public class Output {
                String output;

                Output(String output) {
                    this.output = output;
                }
           }
        */
        return constructorWithFieldsWithGetter(
                        new ByteBuddy().subclass(Object.class).name(packageName("CustomOutput")),
                        getConstructor(Object.class),
                        "output",
                        String.class)
                .make();
    }

    private static DynamicType.Builder<?> procedureBuilder(TypeDescription outputType) {
        var annotation = AnnotationDescription.Latent.Builder.ofType(org.neo4j.procedure.Procedure.class)
                .build();

        /* Below translates to:
           Stream<Output>
        */
        var genericStreamType = TypeDescription.Generic.Builder.parameterizedType(
                        TypeDescription.ForLoadedType.of(Stream.class), outputType)
                .build();

        /* Below translates to:
           public class CustomProcedure {

               @Procedure
               Stream<Output> myProcedure() {
                   return null;
               }
           }
        */

        return new ByteBuddy()
                .subclass(Object.class)
                .name(packageName("CustomProcedure"))
                .defineMethod(PROCEDURE_NAME, genericStreamType, Opcodes.ACC_PUBLIC)
                .intercept(FixedValue.nullValue())
                .visit(new MemberAttributeExtension.ForMethod()
                        .annotateMethod(annotation)
                        .on(ElementMatchers.nameEndsWith("Procedure")));
    }

    private static DynamicType.Unloaded<?> makeProcedureType(DynamicType.Unloaded<?> outputType) {
        return procedureBuilder(outputType.getTypeDescription()).make().include(outputType);
    }

    private static DynamicType.Unloaded<?> makeProcedureType(
            DynamicType.Unloaded<?> outputType, DynamicType.Unloaded<?> extensionType) {
        var annotation =
                AnnotationDescription.Latent.Builder.ofType(Context.class).build();
        /* Below translates to:
           class CustomProcedure {

               @Context
               public CustomExtension extension;

               /.../
           }
        */
        return procedureBuilder(outputType.getTypeDescription())
                .defineField("extension", extensionType.getTypeDescription(), Opcodes.ACC_PUBLIC)
                .visit(new MemberAttributeExtension.ForField()
                        .annotate(annotation)
                        .on(ElementMatchers.named("extension")))
                .make()
                .include(outputType, extensionType);
    }

    private static DynamicType.Unloaded<?> makeDependenciesInterfaceType() {
        /* Below translates to:
        public interface Dependencies {
            LogService log();
            GlobalProcedures globalProcedures();
        }
         */
        return new ByteBuddy()
                .makeInterface()
                .name(packageName("Dependencies"))
                .defineMethod("log", LogService.class, PUBLIC)
                .withoutCode()
                .defineMethod("globalProcedures", GlobalProcedures.class, PUBLIC)
                .withoutCode()
                .make();
    }

    private static DynamicType.Unloaded<?> makeDependenciesImplementationType(
            DynamicType.Unloaded<?> dependencyProxyInterface) {
        /* Below translates to:
           public class DependenciesImpl implements Dependencies {
               public LogService log;
               public GlobalProcedures globalProcedures;

               DependenciesImpl(LogService log, GlobalProcedures globalProcedures) {
                   this.log = log;
                   this.globalProcedures = globalProcedures;
               }

               LogService log() {
                   return log;
               }

               GlobalProcedures globalProcedures() {
                   return globalProcedures;
               }
           }
        */
        return constructorWithFieldsWithGetter(
                        new ByteBuddy()
                                .subclass(Object.class)
                                .implement(dependencyProxyInterface.getTypeDescription())
                                .name(packageName("DependenciesProxy")),
                        getConstructor(Object.class),
                        "log",
                        LogService.class,
                        "globalProcedures",
                        GlobalProcedures.class)
                .make()
                .include(dependencyProxyInterface);
    }

    private static DynamicType.Unloaded<?> makeExtensionType(DynamicType.Unloaded<?> dependencyInterfaceType) {

        /* Below translates to:
           log.getInternalLog().info(LOG_MARKER());
        */
        Implementation logMessage = MethodCall.invoke(
                        ElementMatchers.named("info").and(ElementMatchers.takesArguments(String.class)))
                .onMethodCall(MethodCall.invoke(getDeclaredMethod(LogService.class, "getInternalLog", Class.class))
                        .onField("log")
                        .withOwnType())
                .with(LOG_MARKER);

        return constructorWithFieldsWithGetter(
                        new ByteBuddy().subclass(LifecycleAdapter.class).name(packageName("CustomExtension")),
                        getConstructor(LifecycleAdapter.class),
                        "log",
                        LogService.class,
                        "globalProcedures",
                        GlobalProcedures.class)
                /* Below translates to:
                   CustomExtension(Dependencies dependencies) {
                       this(dependencies.log(), dependencies.globalProcedures());
                   }
                */
                .defineConstructor(Opcodes.ACC_PUBLIC)
                .withParameters(dependencyInterfaceType.getTypeDescription()) // that accepts `Dependency dependencies`
                .intercept(MethodCall.invoke(ElementMatchers.isConstructor()
                                .and(ElementMatchers.takesArguments(LogService.class, GlobalProcedures.class)))
                        .withMethodCall(
                                MethodCall.invoke(ElementMatchers.named("log")).onArgument(0))
                        .withMethodCall(MethodCall.invoke(ElementMatchers.named("globalProcedures"))
                                .onArgument(0)))
                /* Below translates to:
                   void init() {
                       registrationHelper(globalProcedures, CustomExtension.class, this);
                       // inlined content of logMessage
                   }
                */
                .defineMethod("init", void.class, Opcodes.ACC_PUBLIC)
                .intercept(MethodCall.invoke(getDeclaredMethod(
                                CustomExtensionUtils.class,
                                "registrationHelper",
                                GlobalProcedures.class,
                                Class.class,
                                Object.class))
                        .withField("globalProcedures")
                        .withOwnType()
                        .withThis()
                        .andThen(logMessage))
                .make()
                .include(dependencyInterfaceType);
    }

    private static DynamicType.Unloaded<?> makeExtensionFactoryType(
            DynamicType.Unloaded<?> dependenciesInterfaceType, DynamicType.Unloaded<?> extensionType) {

        var extensionContextType = TypeDescription.ForLoadedType.of(ExtensionContext.class);
        /* Below translates to:
           ExtensionFactory<Dependencies>
        */
        var genericExtensionFactoryType = TypeDescription.Generic.Builder.parameterizedType(
                        TypeDescription.ForLoadedType.of(ExtensionFactory.class),
                        dependenciesInterfaceType.getTypeDescription())
                .build();

        return new ByteBuddy()
                .subclass(genericExtensionFactoryType.asGenericType(), NO_CONSTRUCTORS)
                .name(packageName("CustomExtensionFactory"))
                /* Below translates to:
                   CustomExtensionFactory() {
                       super(ExtensionType.DATABASE, "CUSTOM_EXTENSION_FACTORY")
                   }
                */
                .defineConstructor(Visibility.PUBLIC)
                .intercept(MethodCall.invoke(ElementMatchers.isConstructor()
                                .and(ElementMatchers.takesArguments(ExtensionType.class, String.class)))
                        .with(ExtensionType.DATABASE, "CUSTOM_EXTENSION_FACTORY"))
                /* Below translates to:
                   LifeCycle newInstance(LogService log, GlobalProcedures procedures) {
                       return new CustomExtension(log, procedures);
                   }
                */
                .defineMethod("newInstance", Lifecycle.class)
                .withParameters(LogService.class, GlobalProcedures.class)
                .intercept(MethodDelegation.toConstructor(extensionType.getTypeDescription()))
                /* Below translates to:
                   LifeCycle newInstance(ExtensionContext ctx, Dependencies dependencies) {
                       return newInstance(dependencies.log(), dependencies.globalProcedures());
                   }
                */
                .defineMethod("newInstance", Lifecycle.class)
                .withParameters(extensionContextType, dependenciesInterfaceType.getTypeDescription())
                .intercept(MethodCall.invoke(ElementMatchers.named("newInstance")
                                .and(ElementMatchers.takesArguments(LogService.class, GlobalProcedures.class)))
                        .withMethodCall(
                                MethodCall.invoke(ElementMatchers.named("log")).onArgument(1))
                        .withMethodCall(MethodCall.invoke(ElementMatchers.named("globalProcedures"))
                                .onArgument(1)))
                .make()
                .include(dependenciesInterfaceType, extensionType);
    }

    // Note: This function must be public in order for ByteBuddy to be able to use it.
    @SuppressWarnings("unused")
    public static <T> void registrationHelper(GlobalProcedures registry, Class<T> cls, Object self) {
        // Providing the ThrowingFunction as a ByteBuddy argument proved too
        // difficult, thus we delegate to this helper method instead.
        registry.registerComponent(cls, (ctx) -> cls.cast(self), true);
    }

    private static DynamicType.Builder<?> constructorWithFieldsWithGetter(
            DynamicType.Builder<?> builder, Constructor<?> superConstructor, String f1, Class<?> t1) {
        return builder.defineField(f1, t1, Opcodes.ACC_PUBLIC)
                .defineConstructor(Opcodes.ACC_PUBLIC)
                .withParameters(t1)
                .intercept(MethodCall.invoke(superConstructor)
                        .andThen(FieldAccessor.ofField(f1).setsArgumentAt(0)))
                .defineMethod(f1, t1)
                .intercept(FieldAccessor.ofField(f1));
    }

    private static DynamicType.Builder<?> constructorWithFieldsWithGetter(
            DynamicType.Builder<?> builder,
            Constructor<?> superConstructor,
            String f1,
            Class<?> t1,
            String f2,
            Class<?> t2) {
        return builder.defineField(f1, t1, Opcodes.ACC_PUBLIC)
                .defineField(f2, t2, Opcodes.ACC_PUBLIC)
                .defineConstructor(Opcodes.ACC_PUBLIC)
                .withParameters(t1, t2)
                .intercept(MethodCall.invoke(superConstructor)
                        .andThen(FieldAccessor.ofField(f1).setsArgumentAt(0))
                        .andThen(FieldAccessor.ofField(f2).setsArgumentAt(1)))
                .defineMethod(f1, t1)
                .intercept(FieldAccessor.ofField(f1))
                .defineMethod(f2, t2)
                .intercept(FieldAccessor.ofField(f2));
    }

    private static Constructor<?> getConstructor(Class<?> cls) {
        try {
            return cls.getConstructor();
        } catch (NoSuchMethodException exc) {
            throw new RuntimeException(exc);
        }
    }

    private static Method getDeclaredMethod(Class<?> cls, String name, Class<?>... arguments) {
        try {
            return cls.getDeclaredMethod(name, arguments);
        } catch (NoSuchMethodException exc) {
            throw new RuntimeException(exc);
        }
    }

    private static void addService(JarOutputStream stream, String iface, String cls) throws IOException {
        // Adds a META-INF/services entry in order to make the class discoverable for service loading.
        stream.putNextEntry(new ZipEntry("META-INF/services/" + iface));
        stream.write(cls.getBytes(StandardCharsets.UTF_8));
    }

    private static String packageName(String name) {
        return "org.neo4j.procedure.impl." + name;
    }
}
