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
package org.neo4j.tooling.procedure;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;
import static com.google.testing.compile.JavaSourcesSubjectFactory.javaSources;
import static java.util.Arrays.asList;

import com.google.testing.compile.CompilationRule;
import com.google.testing.compile.CompileTester.UnsuccessfulCompilationClause;
import javax.annotation.processing.Processor;
import javax.tools.JavaFileObject;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.tooling.procedure.testutils.JavaFileObjectUtils;

public class ProcedureProcessorTest extends ExtensionTestBase {

    @Rule
    public CompilationRule compilation = new CompilationRule();

    private final Processor processor = new ProcedureProcessor();

    @Test
    public void fails_if_parameters_are_not_properly_annotated() {
        JavaFileObject sproc =
                JavaFileObjectUtils.INSTANCE.procedureSource("invalid/missing_name/MissingNameSproc.java");

        UnsuccessfulCompilationClause compilation = assert_()
                .about(javaSource())
                .that(sproc)
                .processedWith(processor())
                .failsToCompile()
                .withErrorCount(2);

        compilation
                .withErrorContaining("@org.neo4j.procedure.Name usage error: missing on parameter <parameter>")
                .in(sproc)
                .onLine(33);

        compilation
                .withErrorContaining("@org.neo4j.procedure.Name usage error: missing on parameter <otherParam>")
                .in(sproc)
                .onLine(33);
    }

    @Test
    public void fails_if_return_type_is_not_stream() {
        JavaFileObject sproc =
                JavaFileObjectUtils.INSTANCE.procedureSource("invalid/bad_return_type/BadReturnTypeSproc.java");

        var compiler = assert_()
                .about(javaSource())
                .that(sproc)
                .processedWith(processor())
                .failsToCompile()
                .withErrorCount(17);

        compiler.withErrorContaining(
                        "Return type of BadReturnTypeSproc#invalidSproc1 must be java.util.stream.Stream<T>")
                .in(sproc)
                .onLine(37);

        compiler.withErrorContaining(
                        "Return type of BadReturnTypeSproc#invalidSproc2 must be java.util.stream.Stream<T> where T is a custom class per procedure, but was java.lang.String")
                .in(sproc)
                .onLine(42);

        compiler.withErrorContaining(
                        "Return type of BadReturnTypeSproc#invalidSproc3 must be java.util.stream.Stream<T> where T is a custom class per procedure, but was org.neo4j.graphdb.Relationship")
                .in(sproc)
                .onLine(47);

        compiler.withErrorContaining(
                        "Return type of BadReturnTypeSproc#invalidSproc4 must be java.util.stream.Stream<T> where T is a custom class per procedure, but was java.util.Map<java.lang.String,java.lang.String>")
                .in(sproc)
                .onLine(52);

        // Paper-cut for subtypes such as Hashmap
        var errors =
                """
                Record definition error: field HashMap#table must be public
                Record definition error: field HashMap#entrySet must be public
                Record definition error: field HashMap#size must be public
                Record definition error: field HashMap#modCount must be public
                Record definition error: field HashMap#threshold must be public
                Record definition error: field HashMap#loadFactor must be public
                Record definition error: field HashMap#table of type java.util.HashMap.Node<K,V>[] is not supported
                Record definition error: field HashMap#entrySet of type java.util.Set<java.util.Map.Entry<K,V>> is not supported
                Record definition error: field HashMap#size of type int is not supported
                Record definition error: field HashMap#modCount of type int is not supported
                Record definition error: field HashMap#threshold of type int is not supported
                Record definition error: field HashMap#loadFactor of type float is not supported
                """
                        .split("\n");
        for (var error : errors) {
            compiler.withErrorContaining(error);
        }

        compiler.withErrorContaining(
                        "Return type of BadReturnTypeSproc#invalidSproc6 must be java.util.stream.Stream<T> where T is a custom class per procedure, but was java.lang.Object")
                .in(sproc)
                .onLine(62);
    }

    @Test
    public void fails_if_record_type_has_nonpublic_fields() {
        JavaFileObject record = JavaFileObjectUtils.INSTANCE.procedureSource("invalid/bad_record_type/BadRecord.java");

        UnsuccessfulCompilationClause compilation = assert_()
                .about(javaSources())
                .that(asList(
                        JavaFileObjectUtils.INSTANCE.procedureSource("invalid/bad_record_type/BadRecordTypeSproc.java"),
                        record))
                .processedWith(processor())
                .failsToCompile()
                .withErrorCount(2);

        compilation
                .withErrorContaining("Record definition error: field BadRecord#label must be public")
                .in(record)
                .onLine(25);

        compilation
                .withErrorContaining("Record definition error: field BadRecord#age must be public")
                .in(record)
                .onLine(26);
    }

    @Test
    public void fails_if_procedure_primitive_input_type_is_not_supported() {
        JavaFileObject sproc =
                JavaFileObjectUtils.INSTANCE.procedureSource("invalid/bad_proc_input_type/BadPrimitiveInputSproc.java");

        assert_()
                .about(javaSource())
                .that(sproc)
                .processedWith(processor())
                .failsToCompile()
                .withErrorCount(1)
                .withErrorContaining(
                        "Unsupported parameter type <short> of procedure|function BadPrimitiveInputSproc#doSomething")
                .in(sproc)
                .onLine(31);
    }

    @Test
    public void fails_if_procedure_generic_input_type_is_not_supported() {
        JavaFileObject sproc =
                JavaFileObjectUtils.INSTANCE.procedureSource("invalid/bad_proc_input_type/BadGenericInputSproc.java");

        UnsuccessfulCompilationClause compilation = assert_()
                .about(javaSource())
                .that(sproc)
                .processedWith(processor())
                .failsToCompile()
                .withErrorCount(4);

        compilation
                .withErrorContaining("Unsupported parameter type "
                        + "<java.util.List<java.util.List<java.util.Map<java.lang.String,java.lang.Thread>>>>"
                        + " of procedure|function BadGenericInputSproc#doSomething")
                .in(sproc)
                .onLine(34);

        compilation
                .withErrorContaining("Unsupported parameter type "
                        + "<java.util.Map<java.lang.String,java.util.List<java.util.concurrent.ExecutorService>>>"
                        + " of procedure|function BadGenericInputSproc#doSomething2")
                .in(sproc)
                .onLine(37);

        compilation
                .withErrorContaining(
                        "Unsupported parameter type <java.util.Map> of procedure|function BadGenericInputSproc#doSomething3")
                .in(sproc)
                .onLine(40);

        compilation
                .withErrorContaining(
                        "Unsupported parameter type <java.lang.String[]> of procedure|function BadGenericInputSproc#doSomething4")
                .in(sproc)
                .onLine(43);
    }

    @Test
    public void fails_if_procedure_primitive_record_field_type_is_not_supported() {
        JavaFileObject record = JavaFileObjectUtils.INSTANCE.procedureSource(
                "invalid/bad_record_field_type/BadRecordSimpleFieldType.java");

        var compilation = assert_()
                .about(javaSources())
                .that(asList(
                        JavaFileObjectUtils.INSTANCE.procedureSource(
                                "invalid/bad_record_field_type/BadRecordSimpleFieldTypeSproc.java"),
                        record))
                .processedWith(processor())
                .failsToCompile()
                .withErrorCount(3);

        compilation
                .withErrorContaining(
                        "Record definition error: field BadRecordSimpleFieldType#wrongType1 of type java.lang.Integer is not supported")
                .in(record)
                .onLine(28);

        compilation
                .withErrorContaining(
                        "Record definition error: field BadRecordSimpleFieldType#wrongType2 of type long[] is not supported")
                .in(record)
                .onLine(29);

        compilation
                .withErrorContaining(
                        "Record definition error: field BadRecordSimpleFieldType#wrongType3 of type java.lang.String[] is not supported")
                .in(record)
                .onLine(30);
    }

    @Test
    public void fails_if_procedure_generic_record_field_type_is_not_supported() {
        JavaFileObject record = JavaFileObjectUtils.INSTANCE.procedureSource(
                "invalid/bad_record_field_type/BadRecordGenericFieldType.java");

        UnsuccessfulCompilationClause compilation = assert_()
                .about(javaSources())
                .that(asList(
                        JavaFileObjectUtils.INSTANCE.procedureSource(
                                "invalid/bad_record_field_type/BadRecordGenericFieldTypeSproc.java"),
                        record))
                .processedWith(processor())
                .failsToCompile()
                .withErrorCount(3);

        compilation
                .withErrorContaining(
                        "Record definition error: field BadRecordGenericFieldType#wrongType1 of type java.util.Map<java.lang.String,java.lang.Integer> is not supported")
                .in(record)
                .onLine(30);
        compilation
                .withErrorContaining(
                        "Record definition error: field BadRecordGenericFieldType#wrongType2 of type java.util.List<java.lang.Integer> is not supported")
                .in(record)
                .onLine(31);
        compilation
                .withErrorContaining(
                        "Record definition error: field BadRecordGenericFieldType#wrongType3 of type java.util.List<java.util.List<java.util.Map<java.lang.String,java.lang.Integer>>> is not supported")
                .in(record)
                .onLine(32);
    }

    @Test
    public void fails_if_duplicate_procedures_are_declared() {
        JavaFileObject firstDuplicate = JavaFileObjectUtils.INSTANCE.procedureSource("invalid/duplicated/Sproc1.java");
        JavaFileObject secondDuplicate = JavaFileObjectUtils.INSTANCE.procedureSource("invalid/duplicated/Sproc2.java");

        assert_()
                .about(javaSources())
                .that(asList(firstDuplicate, secondDuplicate))
                .processedWith(processor())
                .failsToCompile()
                .withErrorCount(2)
                .withErrorContaining(
                        "Procedure|function name <org.neo4j.tooling.procedure.procedures.invalid.duplicated.foobar> is "
                                + "already defined 2 times. It should be defined only once!");
    }

    @Test
    public void fails_if_procedure_class_has_no_public_no_arg_constructor() {
        JavaFileObject procedure = JavaFileObjectUtils.INSTANCE.procedureSource(
                "invalid/missing_constructor/MissingConstructorProcedure.java");

        assert_()
                .about(javaSource())
                .that(procedure)
                .processedWith(processor())
                .failsToCompile()
                .withErrorCount(1)
                .withErrorContaining(
                        "Extension class org.neo4j.tooling.procedure.procedures.invalid.missing_constructor.MissingConstructorProcedure "
                                + "should contain a public no-arg constructor, none found.")
                .in(procedure)
                .onLine(24);
    }

    @Test
    public void succeeds_to_process_valid_stored_procedures() {
        assert_()
                .about(javaSources())
                .that(asList(
                        JavaFileObjectUtils.INSTANCE.procedureSource("valid/Procedures.java"),
                        JavaFileObjectUtils.INSTANCE.procedureSource("valid/Records.java")))
                .processedWith(processor())
                .compilesWithoutError();
    }

    @Override
    Processor processor() {
        return processor;
    }
}
