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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.procedure.Context;

class FieldInjectionsTest {
    @Test
    void shouldNotAllowClassesWithNonInjectedFields() {
        // Given
        FieldInjections injections = new FieldInjections(new ComponentRegistry());

        ProcedureException exception = assertThrows(
                ProcedureException.class, () -> injections.setters(ProcedureWithNonInjectedMemberFields.class));
        assertThat(exception.getMessage())
                .isEqualTo(
                        "Field `someState` on `ProcedureWithNonInjectedMemberFields` is not annotated as a @Context and is not static. "
                                + "If you want to store state along with your procedure, please use a static field.");
    }

    @Test
    void shouldNotAllowNonPublicFieldsForInjection() {
        // Given
        FieldInjections injections = new FieldInjections(new ComponentRegistry());

        ProcedureException exception =
                assertThrows(ProcedureException.class, () -> injections.setters(ProcedureWithPrivateMemberField.class));
        assertThat(exception.getMessage())
                .isEqualTo("Field `someState` on `ProcedureWithPrivateMemberField` must be non-final and public.");
    }

    @Test
    void staticFieldsAreAllowed() throws Throwable {
        // Given
        FieldInjections injections = new FieldInjections(new ComponentRegistry());

        // When
        List<FieldSetter> setters = injections.setters(ProcedureWithStaticFields.class);

        // Then
        assertEquals(0, setters.size());
    }

    @Test
    void syntheticsAllowed() throws Throwable {
        // Given
        ComponentRegistry components = new ComponentRegistry();
        components.register(int.class, ctx -> 1337);
        FieldInjections injections = new FieldInjections(components);

        // When
        List<FieldSetter> setters = injections.setters(Outer.ClassWithSyntheticField.class);

        // Then
        new Outer().classWithSyntheticField();
        for (FieldSetter setter : setters) {
            assertFalse(setter.field().isSynthetic());
        }
    }

    public static class ProcedureWithNonInjectedMemberFields {
        public boolean someState;
    }

    public static class ProcedureWithPrivateMemberField {
        @Context
        private boolean someState;
    }

    public static class ProcedureWithStaticFields {
        private static boolean someState;
    }

    public static class ParentProcedure {
        @Context
        public int parentField;
    }

    public static class ChildProcedure extends ParentProcedure {
        @Context
        public int childField;
    }

    // The outer class is just here to force a synthetic field in the inner class.
    // This is not a realistic scenario but we merely want to make sure the loader
    // does not choke on synthetic fields since compilers, e.g. groovy, can generate
    // these.
    public static class Outer {
        ClassWithSyntheticField classWithSyntheticField() {
            return new ClassWithSyntheticField();
        }

        public class ClassWithSyntheticField {
            // this class will have a generated field:
            // synthetic Outer this$0;

            @Context
            public int innerField;
        }
    }
}
