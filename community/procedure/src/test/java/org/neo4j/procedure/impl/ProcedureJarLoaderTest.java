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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.ResourceTracker.EMPTY_RESOURCE_TRACKER;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserFunction;
import org.neo4j.string.Globbing;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.jar.JarBuilder;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.Values;

@SuppressWarnings("WeakerAccess")
@TestDirectoryExtension
public class ProcedureJarLoaderTest {
    @Inject
    private TestDirectory testDirectory;

    private final InternalLog log = mock(InternalLog.class);
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper(mock(InternalTransaction.class));

    private AssertableLogProvider logProvider;
    private ProcedureJarLoader jarloader;

    private Path jarDirectory;

    @BeforeEach
    void setup() {
        var config = Config.defaults(procedure_unrestricted, List.of("org.neo4j.kernel.impl.proc.unsafeFullAccess*"));
        logProvider = new AssertableLogProvider(true);
        jarloader = makeJarLoader(config, logProvider);
        jarDirectory = testDirectory.absolutePath();
    }

    @Test
    void shouldLoadProcedureFromJar() throws Throwable {
        // Given
        JarBuilder.createJarFor(jarDirectory.resolve("my.jar"), ClassWithOneProcedure.class);

        // When
        List<CallableProcedure> procedures =
                jarloader.loadProceduresFromDir(jarDirectory).procedures();

        // Then
        List<ProcedureSignature> signatures =
                procedures.stream().map(CallableProcedure::signature).toList();
        assertThat(signatures)
                .containsExactly(procedureSignature("org", "neo4j", "procedure", "impl", "myProcedure")
                        .out("someNumber", NTInteger)
                        .build());

        assertThat(asList(procedures.get(0).apply(prepareContext(), new AnyValue[0], EMPTY_RESOURCE_TRACKER)))
                .containsExactly(new AnyValue[] {Values.longValue(1337L)});
    }

    @Test
    void shouldLoadProcedureFromJarWithSpacesInFilename() throws Throwable {
        // Given
        JarBuilder.createJarFor(jarDirectory.resolve("my file with spaces.jar"), ClassWithOneProcedure.class);

        // When
        List<CallableProcedure> procedures =
                jarloader.loadProceduresFromDir(jarDirectory).procedures();

        // Then
        List<ProcedureSignature> signatures =
                procedures.stream().map(CallableProcedure::signature).toList();
        assertThat(signatures)
                .containsExactly(procedureSignature("org", "neo4j", "procedure", "impl", "myProcedure")
                        .out("someNumber", NTInteger)
                        .build());

        assertThat(asList(procedures.get(0).apply(prepareContext(), new AnyValue[0], EMPTY_RESOURCE_TRACKER)))
                .containsExactly(new AnyValue[] {Values.longValue(1337L)});
    }

    @Test
    void shouldLoadProcedureWithArgumentFromJar() throws Throwable {
        // Given
        JarBuilder.createJarFor(jarDirectory.resolve("my.jar"), ClassWithProcedureWithArgument.class);

        // When
        List<CallableProcedure> procedures =
                jarloader.loadProceduresFromDir(jarDirectory).procedures();

        // Then
        List<ProcedureSignature> signatures =
                procedures.stream().map(CallableProcedure::signature).toList();
        assertThat(signatures)
                .containsExactly(procedureSignature("org", "neo4j", "procedure", "impl", "myProcedure")
                        .in("value", NTInteger)
                        .out("someNumber", NTInteger)
                        .build());

        assertThat(asList(procedures
                        .get(0)
                        .apply(prepareContext(), new AnyValue[] {Values.longValue(42)}, EMPTY_RESOURCE_TRACKER)))
                .containsExactly(new AnyValue[] {Values.longValue(42)});
    }

    @Test
    void shouldLoadProcedureFromJarWithMultipleProcedureClasses() throws Throwable {
        // Given
        JarBuilder.createJarFor(
                jarDirectory.resolve("my.jar"),
                ClassWithOneProcedure.class,
                ClassWithAnotherProcedure.class,
                ClassWithNoProcedureAtAll.class);

        // When
        List<CallableProcedure> procedures =
                jarloader.loadProceduresFromDir(jarDirectory).procedures();

        // Then
        List<ProcedureSignature> signatures =
                procedures.stream().map(CallableProcedure::signature).toList();
        assertThat(signatures)
                .contains(
                        procedureSignature("org", "neo4j", "procedure", "impl", "myOtherProcedure")
                                .out("someNumber", NTInteger)
                                .build(),
                        procedureSignature("org", "neo4j", "procedure", "impl", "myProcedure")
                                .out("someNumber", NTInteger)
                                .build());
    }

    @Test
    void shouldGiveHelpfulErrorOnInvalidProcedure() throws Throwable {
        // Given
        JarBuilder.createJarFor(
                jarDirectory.resolve("my.jar"), ClassWithOneProcedure.class, ClassWithInvalidProcedure.class);

        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> jarloader.loadProceduresFromDir(jarDirectory));
        assertThat(exception.getMessage())
                .isEqualTo(format("Procedures must return a Stream of records, where a record is a concrete class%n"
                        + "that you define, with public non-final fields defining the fields in the record.%n"
                        + "If you''d like your procedure to return `boolean`, you could define a record class like:%n"
                        + "public class Output '{'%n"
                        + "    public boolean out;%n"
                        + "'}'%n%n"
                        + "And then define your procedure as returning `Stream<Output>`."));
    }

    @Test
    void shouldLoadProceduresFromDirectory() throws Throwable {
        // Given
        JarBuilder.createJarFor(jarDirectory.resolve("my.jar"), ClassWithOneProcedure.class);
        JarBuilder.createJarFor(jarDirectory.resolve("my_other.jar"), ClassWithAnotherProcedure.class);

        // When
        List<CallableProcedure> procedures =
                jarloader.loadProceduresFromDir(jarDirectory).procedures();

        // Then
        List<ProcedureSignature> signatures =
                procedures.stream().map(CallableProcedure::signature).toList();
        assertThat(signatures)
                .contains(
                        procedureSignature("org", "neo4j", "procedure", "impl", "myOtherProcedure")
                                .out("someNumber", NTInteger)
                                .build(),
                        procedureSignature("org", "neo4j", "procedure", "impl", "myProcedure")
                                .out("someNumber", NTInteger)
                                .build());
    }

    @Test
    void shouldLoadMultiReleaseJarsAndLoadVersionedClass() throws Exception {
        // given
        createMrJarFor(Runtime.version(), ClassWithOneProcedure.class);

        // when
        List<CallableProcedure> procedures =
                jarloader.loadProceduresFromDir(jarDirectory).procedures();
        List<ProcedureSignature> signatures =
                procedures.stream().map(CallableProcedure::signature).toList();

        // then
        assertThat(signatures)
                .containsExactly(procedureSignature("org", "neo4j", "procedure", "impl", "myProcedure")
                        .out("someNumber", NTInteger)
                        .build());

        assertThat(logProvider).doesNotHaveAnyLogs();
    }

    @Test
    void shouldLoadMultiReleaseJarsAndIgnoreFutureVersions() throws Exception {
        // given
        Runtime.Version nextVersion =
                Runtime.Version.parse(String.valueOf(Runtime.version().feature() + 1));
        createMrJarFor(nextVersion, ClassWithOneProcedure.class);

        // when
        List<CallableProcedure> procedures =
                jarloader.loadProceduresFromDir(jarDirectory).procedures();
        List<ProcedureSignature> signatures =
                procedures.stream().map(CallableProcedure::signature).toList();

        // then
        assertThat(signatures)
                .containsExactly(procedureSignature("org", "neo4j", "procedure", "impl", "myProcedure")
                        .out("someNumber", NTInteger)
                        .build());

        assertThat(logProvider).doesNotHaveAnyLogs();
    }

    @Test
    void shouldGiveHelpfulErrorOnWildCardProcedure() throws Throwable {
        // Given
        JarBuilder.createJarFor(jarDirectory.resolve("my.jar"), ClassWithWildCardStream.class);

        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> jarloader.loadProceduresFromDir(jarDirectory));
        assertThat(exception.getMessage())
                .isEqualTo(format("Procedures must return a Stream of records, where a record is a concrete class%n"
                        + "that you define and not a Stream<?>."));
    }

    @Test
    void shouldGiveHelpfulErrorOnRawStreamProcedure() throws Throwable {
        // Given
        JarBuilder.createJarFor(jarDirectory.resolve("my.jar"), ClassWithRawStream.class);

        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> jarloader.loadProceduresFromDir(jarDirectory));
        assertThat(exception.getMessage())
                .isEqualTo(format("Procedures must return a Stream of records, where a record is a concrete class%n"
                        + "that you define and not a raw Stream."));
    }

    @Test
    void shouldGiveHelpfulErrorOnGenericStreamProcedure() throws Throwable {
        // Given
        JarBuilder.createJarFor(jarDirectory.resolve("my.jar"), ClassWithGenericStream.class);

        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> jarloader.loadProceduresFromDir(jarDirectory));
        assertThat(exception.getMessage())
                .isEqualTo(
                        format(
                                "Procedures must return a Stream of records, where a record is a concrete class%n"
                                        + "that you define and not a parameterized type such as java.util.List<org.neo4j.procedure.impl.ProcedureJarLoaderTest$Output>."));
    }

    @Test
    void shouldLogHelpfullyWhenPluginJarIsCorrupt() throws Exception {
        // given
        var jar = jarDirectory.resolve("my.jar");
        JarBuilder.createJarFor(
                jar, ClassWithOneProcedure.class, ClassWithAnotherProcedure.class, ClassWithNoProcedureAtAll.class);
        corruptJar(jar);

        // when
        assertThatThrownBy(() -> jarloader.loadProceduresFromDir(jarDirectory))
                .isInstanceOf(ZipException.class)
                .hasMessageContaining(String.format(
                        "Some jar procedure files (%s) are invalid, see log for details.", jar.getFileName()));
        assertThat(logProvider).containsMessages(format("Plugin jar file: %s corrupted.", jar));
    }

    @Test
    void shouldLogHelpfullyWhenMultiplePluginJarsAreCorrupt() throws Exception {
        // given
        var jarOne = jarDirectory.resolve("my.jar");
        var jarTwo = jarDirectory.resolve("my_other.jar");
        JarBuilder.createJarFor(
                jarOne, ClassWithOneProcedure.class, ClassWithAnotherProcedure.class, ClassWithNoProcedureAtAll.class);
        JarBuilder.createJarFor(
                jarTwo, ClassWithOneProcedure.class, ClassWithAnotherProcedure.class, ClassWithNoProcedureAtAll.class);
        corruptJar(jarOne);
        corruptJar(jarTwo);

        // when
        assertThatThrownBy(() -> jarloader.loadProceduresFromDir(testDirectory.homePath()))
                .isInstanceOf(ZipException.class)
                .hasMessageContaining("Some jar procedure files (")
                .hasMessageContaining(") are invalid, see log for details.")
                .hasMessageContaining(jarOne.getFileName().toString())
                .hasMessageContaining(jarTwo.getFileName().toString());
        assertThat(logProvider)
                .containsMessages(
                        format("Plugin jar file: %s corrupted.", jarOne),
                        format("Plugin jar file: %s corrupted.", jarTwo));
    }

    @Test
    void shouldLogHelpfullyWhenJarContainsClassTriggeringVerifyError() throws Exception {
        String className = "BrokenProcedureClass";
        // generate a class that is broken and would not normally compile
        DynamicType.Unloaded<Object> unloaded = new ByteBuddy()
                // provide a name so that we can assert on it showing up in the log
                .with(oneNameStrategy(className))
                .subclass(Object.class)
                .defineMethod("get", String.class)
                .intercept(
                        // String is not assignable from int -- this triggers a VerifyError when the class is loaded
                        MethodCall.invoke(Integer.class.getMethod("valueOf", int.class))
                                .with(42)
                                // override the assigner to circumvent ByteBuddy's validation, so the malformed class
                                // can be generated
                                .withAssigner(
                                        (source, target, typing) -> StackManipulation.Trivial.INSTANCE,
                                        Assigner.Typing.STATIC))
                .make();

        Path jar = unloaded.toJar(testDirectory
                        .createFile(new Random().nextInt() + ".jar")
                        .toFile())
                .toPath();

        jarloader.loadProceduresFromDir(jar.getParent());

        assertThat(logProvider)
                .containsMessages(
                        format("Failed to load `%s` from plugin jar `%s`", className, jar), "Bad return type");
    }

    @Test
    void shouldLogHelpfullyWhenJarContainsClassTriggeringLinkageErrorFromParent() throws Exception {
        String baseClassName = "FinalBase";
        String className = "BrokenProcedureClass";

        // generate a class that extends another class
        var base = new ByteBuddy()
                .with(oneNameStrategy(baseClassName))
                .subclass(Object.class)
                .make();
        var unloaded = new ByteBuddy()
                // provide a name so that we can assert on it showing up in the log
                .with(oneNameStrategy(className))
                .subclass(base.getTypeDescription())
                .make();

        Path jar = testDirectory.createFile(new Random().nextInt() + ".jar");
        unloaded.toJar(jar.toFile());

        // replace base class with final one
        var finalBase = new ByteBuddy()
                .with(oneNameStrategy(baseClassName))
                .subclass(Object.class)
                .modifiers(TypeManifestation.FINAL)
                .make();
        finalBase.inject(jar.toFile());

        jarloader.loadProceduresFromDir(jar.getParent());

        assertThat(logProvider)
                .containsMessages(
                        format("Failed to load `%s` from plugin jar `%s`", className, jar),
                        "cannot inherit from final class");
    }

    @Test
    void shouldLogHelpfullyWhenJarContainsClassTriggeringNoClassDefErrorFromMethod() throws Exception {
        var notFoundClassName = "IAmNotHere";
        var willNotBeFound = new ByteBuddy()
                .with(oneNameStrategy(notFoundClassName))
                .subclass(Object.class)
                .make();

        String brokenClassName = "BrokenProcedureClass";
        // generate a class that is broken and would not normally compile
        DynamicType.Unloaded<Object> unloaded = new ByteBuddy()
                // provide a name so that we can assert on it showing up in the log
                .with(oneNameStrategy(brokenClassName))
                .subclass(Object.class)
                .defineMethod("get", willNotBeFound.getTypeDescription())
                .intercept(FixedValue.nullValue())
                .make();

        Path jar = unloaded.toJar(testDirectory
                        .createFile(new Random().nextInt() + ".jar")
                        .toFile())
                .toPath();

        jarloader.loadProceduresFromDir(jar.getParent());

        assertThat(logProvider)
                .containsMessages(
                        format("Failed to load `%s` from plugin jar `%s`", brokenClassName, jar),
                        notFoundClassName,
                        NoClassDefFoundError.class.getName());
    }

    @Test
    void shouldLogHelpfullyWhenJarContainsClassTriggeringNoClassDefErrorFromField() throws Exception {
        var notFoundClassName = "IAmNotHere";
        var willNotBeFound = new ByteBuddy()
                .with(oneNameStrategy(notFoundClassName))
                .subclass(Object.class)
                .make();

        String brokenClassName = "BrokenProcedureClass";
        // generate a class that is broken and would not normally compile
        DynamicType.Unloaded<Object> unloaded = new ByteBuddy()
                // provide a name so that we can assert on it showing up in the log
                .with(oneNameStrategy(brokenClassName))
                .subclass(Object.class)
                .defineField("service", willNotBeFound.getTypeDescription())
                .make();

        Path jar = unloaded.toJar(testDirectory
                        .createFile(new Random().nextInt() + ".jar")
                        .toFile())
                .toPath();

        jarloader.loadProceduresFromDir(jar.getParent());

        assertThat(logProvider)
                .containsMessages(
                        format("Failed to load `%s` from plugin jar `%s`", brokenClassName, jar),
                        notFoundClassName,
                        NoClassDefFoundError.class.getName());
    }

    @Test
    void proceduresCanDependOnOtherJARInDirectory() throws Exception {
        // given
        var neighbourhood = new ByteBuddy().subclass(Object.class).make();

        var ourHero = new ByteBuddy()
                .subclass(Object.class)
                .defineMethod("get", neighbourhood.getTypeDescription())
                .intercept(FixedValue.nullValue())
                .make();

        // when
        neighbourhood.toJar(
                testDirectory.createFile("friendly_neighbourhood.jar").toFile());
        ourHero.toJar(testDirectory.createFile("our_hero.jar").toFile());

        jarloader.loadProceduresFromDir(testDirectory.absolutePath());

        // then
        assertThat(logProvider).forLevel(AssertableLogProvider.Level.WARN).doesNotHaveAnyLogs();
    }

    private static NamingStrategy.AbstractBase oneNameStrategy(String className) {
        return new NamingStrategy.AbstractBase() {
            @Override
            protected String name(TypeDescription superClass) {
                return className;
            }
        };
    }

    @Test
    void shouldWorkOnPathsWithSpaces() throws Exception {
        // given
        Path fileWithSpacesInName =
                testDirectory.createFile(new Random().nextInt() + "  some spaces in the filename" + ".jar");
        JarBuilder.createJarFor(fileWithSpacesInName, ClassWithOneProcedure.class);
        corruptJar(fileWithSpacesInName);

        // when
        assertThrows(ZipException.class, () -> jarloader.loadProceduresFromDir(jarDirectory));
        assertThat(logProvider).containsMessages(format("Plugin jar file: %s corrupted.", fileWithSpacesInName));
    }

    @Test
    void shouldReturnEmptySetOnNullArgument() throws Exception {
        // when
        ProcedureJarLoader.Callables callables = jarloader.loadProceduresFromDir(null);

        // then
        assertEquals(0, callables.procedures().size() + callables.functions().size());
    }

    static Stream<Arguments> namespaceLimits() {
        return Stream.of(
                Arguments.of(List.of(), List.of(), List.of()), // restrictive defaults
                Arguments.of(List.of("*"), List.of(), List.of("A.a", "B.b")),
                Arguments.of(List.of("*"), List.of("*"), List.of()), // exclude takes precedence
                Arguments.of(List.of("A.*"), List.of(), List.of("A.a")),
                Arguments.of(List.of("*"), List.of("A.*"), List.of("B.b")),
                Arguments.of(List.of("*"), List.of("A.a"), List.of("B.b")), // exclude takes precedence
                Arguments.of(List.of("A.a"), List.of("*"), List.of()) // exclude takes precedence
                );
    }

    @ParameterizedTest
    @MethodSource("namespaceLimits")
    void shouldBeAbleToLimitLoadedNamespaces(List<String> include, List<String> exclude, List<String> expected)
            throws Exception {
        // Given
        JarBuilder.createJarFor(jarDirectory.resolve("my.jar"), ClassWithProcedureNamespaces.class);
        var methodNameFilter = Globbing.compose(include, exclude);

        // when
        var procedures = jarloader.loadProceduresFromDir(jarDirectory, methodNameFilter).procedures().stream()
                .map(p -> p.signature().name().toString())
                .toList();

        assertThat(procedures).containsExactlyInAnyOrderElementsOf(expected);
    }

    private org.neo4j.kernel.api.procedure.Context prepareContext() {
        return buildContext(dependencyResolver, valueMapper).context();
    }

    private void corruptJar(Path jarFile) throws IOException, URISyntaxException {
        long fileLength = Files.size(jarFile);
        byte[] bytes = Files.readAllBytes(jarFile);
        for (long i = fileLength / 2; i < fileLength; i++) {
            bytes[(int) i] = 0;
        }
        Files.write(jarFile, bytes);
    }

    private URL createMrJarFor(Runtime.Version version, Class<?>... targets) throws IOException, URISyntaxException {
        String versionPrefix = String.format("META-INF/versions/%d/", version.feature());
        var manifest = new Manifest();
        // need to set a version, otherwise no main attributes will be written
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Multi-Release", "true");

        var finalJar = testDirectory.createFile(new Random().nextInt() + ".jar");
        try (JarOutputStream jarOut = new JarOutputStream(Files.newOutputStream(finalJar), manifest)) {
            Path pth = testDirectory.createFile("my.jar").toAbsolutePath();
            JarBuilder.createJarFor(pth, targets);

            try (ZipInputStream jarInStream = new ZipInputStream(new FileInputStream(pth.toFile()))) {
                ZipEntry nextEntry;

                while ((nextEntry = jarInStream.getNextEntry()) != null) {
                    if (nextEntry.getName().equals("META-INF/MANIFEST.MF")) {
                        continue;
                    }

                    byte[] byteCode = jarInStream.readAllBytes();

                    jarOut.putNextEntry(nextEntry);
                    jarOut.write(byteCode);
                    jarOut.closeEntry();

                    jarOut.putNextEntry(new ZipEntry(versionPrefix + nextEntry.getName()));
                    jarOut.write(byteCode);
                    jarOut.closeEntry();
                }
            }

            Files.delete(pth);
        }

        return finalJar.toUri().toURL();
    }

    public static class Output {
        public long someNumber = 1337; // Public because needed by a mapper

        public Output() {}

        public Output(long anotherNumber) {
            this.someNumber = anotherNumber;
        }
    }

    public static class ClassWithInvalidProcedure {
        @Procedure
        public boolean booleansAreNotAcceptableReturnTypes() {
            return false;
        }
    }

    public static class ClassWithOneProcedure {
        @Procedure
        public Stream<Output> myProcedure() {
            return Stream.of(new Output());
        }
    }

    public static class ClassWithProcedureNamespaces {
        public static final String PROCEDURE_NAME_A = "A.a";
        public static final String PROCEDURE_NAME_B = "B.b";

        @Procedure(name = PROCEDURE_NAME_A)
        public Stream<Output> a() {
            return Stream.of(new Output());
        }

        @Procedure(name = PROCEDURE_NAME_B)
        public Stream<Output> b() {
            return Stream.of(new Output());
        }
    }

    public static class ClassWithNoProcedureAtAll {
        void thisMethodIsEntirelyUnrelatedToAllThisExcitement() {}
    }

    public static class ClassWithAnotherProcedure {
        @Procedure
        public Stream<Output> myOtherProcedure() {
            return Stream.of(new Output());
        }
    }

    public static class ClassWithProcedureWithArgument {
        @Procedure
        public Stream<Output> myProcedure(@Name("value") long value) {
            return Stream.of(new Output(value));
        }
    }

    public static class ClassWithWildCardStream {
        @Procedure
        public Stream<?> wildCardProc() {
            return Stream.of(new Output());
        }
    }

    public static class ClassWithRawStream {
        @Procedure
        public Stream rawStreamProc() {
            return Stream.of(new Output());
        }
    }

    public static class ClassWithGenericStream {
        @Procedure
        public Stream<List<Output>> genericStream() {
            return Stream.of(Collections.singletonList(new Output()));
        }
    }

    public static class ClassWithUnsafeComponent {
        @Context
        public UnsafeAPI api;

        @Procedure
        public Stream<Output> unsafeProcedure() {
            return Stream.of(new Output(api.getNumber()));
        }

        @UserFunction
        public long unsafeFunction() {
            return api.getNumber();
        }
    }

    public static class ClassWithUnsafeConfiguredComponent {
        @Context
        public UnsafeAPI api;

        @Procedure
        public Stream<Output> unsafeFullAccessProcedure() {
            return Stream.of(new Output(api.getNumber()));
        }

        @UserFunction
        public long unsafeFullAccessFunction() {
            return api.getNumber();
        }
    }

    private static class UnsafeAPI {
        public long getNumber() {
            return 7331;
        }
    }

    private static ComponentRegistry registryWithUnsafeAPI() {
        ComponentRegistry allComponents = new ComponentRegistry();
        allComponents.register(UnsafeAPI.class, ctx -> new UnsafeAPI());
        return allComponents;
    }

    private static ProcedureJarLoader makeJarLoader(Config config, InternalLogProvider logProvider) {
        final var cfg = new ProcedureConfig(config, true);
        final InternalLog log = logProvider.getLog(ProcedureJarLoader.class);
        final var compiler =
                new ProcedureCompiler(new TypeCheckers(), new ComponentRegistry(), registryWithUnsafeAPI(), log, cfg);
        return new ProcedureJarLoader(compiler, log, cfg.procedureReloadEnabled());
    }
}
