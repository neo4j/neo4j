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
package org.neo4j.tooling.procedure.visitors;

import com.google.testing.compile.CompilationRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.Stream;
import javax.lang.model.element.ElementVisitor;
import javax.lang.model.element.VariableElement;
import javax.tools.Diagnostic;

import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.testutils.ElementTestUtils;
import org.neo4j.tooling.procedure.visitors.examples.GoodContextUse;
import org.neo4j.tooling.procedure.visitors.examples.StaticNonContextMisuse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class FieldVisitorTest
{

    @Rule
    public CompilationRule compilationRule = new CompilationRule();
    private ElementVisitor<Stream<CompilationMessage>,Void> fieldVisitor;
    private ElementTestUtils elementTestUtils;

    @Before
    public void prepare()
    {
        elementTestUtils = new ElementTestUtils( compilationRule );
        fieldVisitor = new FieldVisitor( compilationRule.getTypes(), compilationRule.getElements(), true );
    }

    @Test
    public void validates_visibility_of_fields()
    {
        Stream<VariableElement> fields = elementTestUtils.getFields( GoodContextUse.class );

        Stream<CompilationMessage> result = fields.flatMap( fieldVisitor::visit );

        assertThat( result ).isEmpty();
    }

    @Test
    public void rejects_non_static_non_context_fields()
    {
        Stream<VariableElement> fields = elementTestUtils.getFields( StaticNonContextMisuse.class );

        Stream<CompilationMessage> result = fields.flatMap( fieldVisitor::visit );

        assertThat( result ).extracting( CompilationMessage::getCategory, CompilationMessage::getContents )
                .containsExactly(
                        tuple( Diagnostic.Kind.ERROR, "Field StaticNonContextMisuse#value should be static" ) );
    }

}

