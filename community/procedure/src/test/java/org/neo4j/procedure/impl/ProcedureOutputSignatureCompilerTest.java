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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.FieldSignature;

@SuppressWarnings("WeakerAccess")
public class ProcedureOutputSignatureCompilerTest {
    public static class SingleStringFieldRecord {
        public String name;

        public SingleStringFieldRecord(String name) {
            this.name = name;
        }
    }

    public static class UnmappableRecord {
        public UnmappableRecord wat;
    }

    public static class RecordWithPrivateField {
        private String wat;
    }

    public static class RecordWithNonStringKeyMap {
        public Map<RecordWithNonStringKeyMap, Object> wat;
    }

    public static class RecordWithStaticFields {
        public static String skipMePublic;
        public String includeMe;
        private static String skipMePrivate;

        public RecordWithStaticFields(String val) {
            this.includeMe = val;
        }
    }

    public static class RecordWithDeprecatedFields {
        @Deprecated
        public String deprecated;

        public String replacement;

        @Deprecated
        public String alsoDeprecated;
    }

    public static class RecordWithInheritedFields extends SingleStringFieldRecord {
        public String anotherName;

        public RecordWithInheritedFields(String name, String anotherName) {
            super(name);
            this.anotherName = anotherName;
        }
    }

    @Test
    void shouldNoteDeprecatedFields() throws Exception {
        // when

        // then
        assertThat(signatures(RecordWithDeprecatedFields.class))
                .contains(
                        outputField("deprecated", NTString, true),
                        outputField("alsoDeprecated", NTString, true),
                        outputField("replacement", NTString, false));
    }

    @Test
    void shouldInheritFields() throws Exception {
        // when

        // then
        assertThat(signatures(RecordWithInheritedFields.class))
                .contains(outputField("name", NTString, false), outputField("anotherName", NTString, false));
    }

    @Test
    void shouldGiveHelpfulErrorOnUnmappable() {
        ProcedureException exception = assertThrows(ProcedureException.class, () -> signatures(UnmappableRecord.class));
        assertThat(exception.getMessage())
                .startsWith(
                        "Field `wat` in record `UnmappableRecord` cannot be converted to a Neo4j type: "
                                + "Don't know how to map `org.neo4j.procedure.impl.ProcedureOutputSignatureCompilerTest$UnmappableRecord`");
    }

    @Test
    void shouldGiveHelpfulErrorOnPrivateField() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> signatures(RecordWithPrivateField.class));
        assertThat(exception.getMessage())
                .startsWith(
                        "Field `wat` in record `RecordWithPrivateField` cannot be accessed. Please ensure the field is marked as `public`.");
    }

    @Test
    void shouldGiveHelpfulErrorOnMapWithNonStringKeyMap() {
        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> signatures(RecordWithNonStringKeyMap.class));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Field `wat` in record `RecordWithNonStringKeyMap` cannot be converted "
                                + "to a Neo4j type: Maps are required to have `String` keys - but this map "
                                + "has `org.neo4j.procedure.impl.ProcedureOutputSignatureCompilerTest$RecordWithNonStringKeyMap` keys.");
    }

    @Test
    void shouldWarnAgainstStdLibraryClassesSinceTheseIndicateUserError() {
        // Impl note: We may want to change this behavior and actually allow procedures to return `Long` etc,
        //            with a default column name. So Stream<Long> would become records like (out: Long)
        //            Drawback of that is that it'd cause cognitive dissonance, it's not obvious what's a record
        //            and what is a primitive value..

        ProcedureException exception = assertThrows(ProcedureException.class, () -> signatures(Long.class));
        assertThat(exception.getMessage())
                .isEqualTo(
                        String.format("Procedures must return a Stream of records, where a record is a concrete class%n"
                                + "that you define, with public non-final fields defining the fields in the record.%n"
                                + "If you''d like your procedure to return `Long`, you could define a record class like:%n"
                                + "public class Output '{'%n"
                                + "    public Long out;%n"
                                + "'}'%n%n"
                                + "And then define your procedure as returning `Stream<Output>`."));
    }

    private List<FieldSignature> signatures(Class<?> clazz) throws ProcedureException {
        return new ProcedureOutputSignatureCompiler(new Cypher5TypeCheckers()).fieldSignatures(clazz);
    }
}
