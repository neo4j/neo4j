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
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.testutils.ElementTestUtils;
import org.neo4j.tooling.procedure.visitors.examples.PerformsWriteProcedures;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.Stream;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementVisitor;
import javax.tools.Diagnostic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class PerformsWriteMethodVisitorTest
{
    @Rule
    public CompilationRule compilationRule = new CompilationRule();

    private ElementVisitor<Stream<CompilationMessage>,Void> visitor = new PerformsWriteMethodVisitor();
    private ElementTestUtils elementTestUtils;

    @Before
    public void prepare()
    {
        elementTestUtils = new ElementTestUtils( compilationRule );
    }

    @Test
    public void rejects_non_procedure_methods()
    {
        Element element =
                elementTestUtils.findMethodElement( PerformsWriteProcedures.class, "missingProcedureAnnotation" );

        Stream<CompilationMessage> errors = visitor.visit( element );

        assertThat( errors ).hasSize( 1 ).extracting( CompilationMessage::getCategory, CompilationMessage::getElement,
                CompilationMessage::getContents ).contains( tuple( Diagnostic.Kind.ERROR, element,
                "@PerformsWrites usage error: missing @Procedure annotation on method" ) );
    }

    @Test
    public void rejects_conflicted_mode_usage()
    {
        Element element = elementTestUtils.findMethodElement( PerformsWriteProcedures.class, "conflictingMode" );

        Stream<CompilationMessage> errors = visitor.visit( element );

        assertThat( errors ).hasSize( 1 ).extracting( CompilationMessage::getCategory, CompilationMessage::getElement,
                CompilationMessage::getContents ).contains( tuple( Diagnostic.Kind.ERROR, element,
                "@PerformsWrites usage error: cannot use mode other than Mode.DEFAULT" ) );
    }

    @Test
    public void validates_regular_procedure()
    {
        Element element = elementTestUtils.findMethodElement( PerformsWriteProcedures.class, "ok" );

        Stream<CompilationMessage> errors = visitor.visit( element );

        assertThat( errors ).isEmpty();
    }
}
