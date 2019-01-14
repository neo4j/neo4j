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
import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.messages.CompilationMessage;
import org.neo4j.tooling.procedure.testutils.TypeMirrorTestUtils;
import org.neo4j.tooling.procedure.visitors.examples.InvalidRecord;
import org.neo4j.tooling.procedure.visitors.examples.ValidRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.stream.Stream;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class RecordTypeVisitorTest
{

    @Rule
    public CompilationRule compilation = new CompilationRule();
    private TypeMirrorTestUtils typeMirrorTestUtils;
    private RecordTypeVisitor visitor;

    @Before
    public void prepare()
    {
        Types types = compilation.getTypes();
        Elements elements = compilation.getElements();
        TypeMirrorUtils typeMirrors = new TypeMirrorUtils( types, elements );

        typeMirrorTestUtils = new TypeMirrorTestUtils( compilation );
        visitor = new RecordTypeVisitor( types, typeMirrors );
    }

    @Test
    public void validates_supported_record()
    {
        TypeMirror recordStreamType = typeMirrorTestUtils.typeOf( Stream.class, ValidRecord.class );

        assertThat( visitor.visit( recordStreamType ) ).isEmpty();
    }

    @Test
    public void does_not_validate_record_with_nonpublic_fields()
    {
        TypeMirror recordStreamType = typeMirrorTestUtils.typeOf( Stream.class, InvalidRecord.class );

        assertThat( visitor.visit( recordStreamType ) ).hasSize( 1 )
                .extracting( CompilationMessage::getCategory, CompilationMessage::getContents ).containsExactly(
                tuple( Diagnostic.Kind.ERROR,
                        "Record definition error: field InvalidRecord#foo must" + " be public" ) );
    }

}
