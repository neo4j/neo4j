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
package org.neo4j.tooling.procedure.visitors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.google.testing.compile.CompilationRule;
import java.util.stream.Stream;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.VariableElement;
import javax.tools.Diagnostic;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.URLAccessChecker;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.InternalLog;
import org.neo4j.procedure.TerminationGuard;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.testutils.ElementTestUtils;
import org.neo4j.tooling.procedure.visitors.examples.FinalContextMisuse;
import org.neo4j.tooling.procedure.visitors.examples.NonPublicContextMisuse;
import org.neo4j.tooling.procedure.visitors.examples.RestrictedContextTypes;
import org.neo4j.tooling.procedure.visitors.examples.StaticContextMisuse;
import org.neo4j.tooling.procedure.visitors.examples.UnknownContextType;

public class ContextFieldVisitorTest {
    private static final org.assertj.core.groups.Tuple UNKNOWN_CONTEXT_ERROR_MSG = tuple(
            Diagnostic.Kind.ERROR,
            "@org.neo4j.procedure.Context usage error: found unknown type <java.lang.String> on field "
                    + "UnknownContextType#unsupportedType, expected one of: <"
                    + GraphDatabaseService.class.getName()
                    + ">, <" + InternalLog.class.getName()
                    + ">, <" + TerminationGuard.class.getName()
                    + ">, <" + SecurityContext.class.getName()
                    + ">, <" + Transaction.class.getName()
                    + ">, <" + URLAccessChecker.class.getName()
                    + ">");

    @Rule
    public CompilationRule compilationRule = new CompilationRule();

    private ElementTestUtils elementTestUtils;
    private ElementVisitor<Stream<CompilationMessage>, Void> contextFieldVisitor;

    @Before
    public void prepare() {
        elementTestUtils = new ElementTestUtils(compilationRule);
        contextFieldVisitor = new ContextFieldVisitor(compilationRule.getTypes(), compilationRule.getElements(), false);
    }

    @Test
    public void rejects_non_public_context_fields() {
        Stream<VariableElement> fields = elementTestUtils.getFields(NonPublicContextMisuse.class);

        Stream<CompilationMessage> result = fields.flatMap(contextFieldVisitor::visit);

        assertThat(result)
                .extracting(CompilationMessage::getCategory, CompilationMessage::getContents)
                .containsExactly(tuple(
                        Diagnostic.Kind.ERROR,
                        "@org.neo4j.procedure.Context usage error: field NonPublicContextMisuse#arithm should be public, "
                                + "non-static and non-final"));
    }

    @Test
    public void rejects_static_context_fields() {
        Stream<VariableElement> fields = elementTestUtils.getFields(StaticContextMisuse.class);

        Stream<CompilationMessage> result = fields.flatMap(contextFieldVisitor::visit);

        assertThat(result)
                .extracting(CompilationMessage::getCategory, CompilationMessage::getContents)
                .containsExactly(tuple(
                        Diagnostic.Kind.ERROR,
                        "@org.neo4j.procedure.Context usage error: field StaticContextMisuse#db should be public, non-static "
                                + "and non-final"));
    }

    @Test
    public void rejects_final_context_fields() {
        Stream<VariableElement> fields = elementTestUtils.getFields(FinalContextMisuse.class);

        Stream<CompilationMessage> result = fields.flatMap(contextFieldVisitor::visit);

        assertThat(result)
                .extracting(CompilationMessage::getCategory, CompilationMessage::getContents)
                .containsExactly(tuple(
                        Diagnostic.Kind.ERROR,
                        "@org.neo4j.procedure.Context usage error: field FinalContextMisuse#graphDatabaseService should be "
                                + "public, non-static and non-final"));
    }

    @Test
    public void warns_against_restricted_injected_types() {
        Stream<VariableElement> fields = elementTestUtils.getFields(RestrictedContextTypes.class);

        Stream<CompilationMessage> result = fields.flatMap(contextFieldVisitor::visit);

        assertThat(result)
                .extracting(CompilationMessage::getCategory, CompilationMessage::getContents)
                .containsExactlyInAnyOrder(
                        tuple(
                                Diagnostic.Kind.WARNING,
                                warning(GraphDatabaseAPI.class.getName(), "RestrictedContextTypes#graphDatabaseAPI")),
                        tuple(
                                Diagnostic.Kind.WARNING,
                                warning(KernelTransaction.class.getName(), "RestrictedContextTypes#kernelTransaction")),
                        tuple(
                                Diagnostic.Kind.WARNING,
                                warning(
                                        DependencyResolver.class.getName(),
                                        "RestrictedContextTypes#dependencyResolver")));
    }

    @Test
    public void does_not_warn_against_restricted_injected_types_when_warnings_are_suppressed() {
        ContextFieldVisitor contextFieldVisitor =
                new ContextFieldVisitor(compilationRule.getTypes(), compilationRule.getElements(), true);
        Stream<VariableElement> fields = elementTestUtils.getFields(RestrictedContextTypes.class);

        Stream<CompilationMessage> result = fields.flatMap(contextFieldVisitor::visit);

        assertThat(result).isEmpty();
    }

    @Test
    public void rejects_unsupported_injected_type() {
        Stream<VariableElement> fields = elementTestUtils.getFields(UnknownContextType.class);

        Stream<CompilationMessage> result = fields.flatMap(contextFieldVisitor::visit);

        assertThat(result)
                .extracting(CompilationMessage::getCategory, CompilationMessage::getContents)
                .containsExactly(UNKNOWN_CONTEXT_ERROR_MSG);
    }

    @Test
    public void rejects_unsupported_injected_type_when_warnings_are_suppressed() {
        ContextFieldVisitor contextFieldVisitor =
                new ContextFieldVisitor(compilationRule.getTypes(), compilationRule.getElements(), true);
        Stream<VariableElement> fields = elementTestUtils.getFields(UnknownContextType.class);

        Stream<CompilationMessage> result = fields.flatMap(contextFieldVisitor::visit);

        assertThat(result)
                .extracting(CompilationMessage::getCategory, CompilationMessage::getContents)
                .containsExactly(UNKNOWN_CONTEXT_ERROR_MSG);
    }

    private String warning(String fieldType, String fieldName) {
        return String.format(
                "@org.neo4j.procedure.Context usage warning: found unsupported restricted type <%s> on %2$s.\n"
                        + "The procedure will not load unless declared via the configuration option 'dbms.security.procedures.unrestricted'.\n"
                        + "You can ignore this warning by passing the option -AIgnoreContextWarnings to the Java compiler",
                fieldType, fieldName);
    }
}
