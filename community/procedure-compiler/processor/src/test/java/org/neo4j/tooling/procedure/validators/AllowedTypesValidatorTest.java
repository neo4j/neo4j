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
package org.neo4j.tooling.procedure.validators;

import com.google.testing.compile.CompilationRule;
import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.testutils.TypeMirrorTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import static javax.lang.model.type.TypeKind.BOOLEAN;
import static javax.lang.model.type.TypeKind.DOUBLE;
import static javax.lang.model.type.TypeKind.LONG;
import static org.assertj.core.api.Assertions.assertThat;

public class AllowedTypesValidatorTest
{

    @Rule
    public CompilationRule compilation = new CompilationRule();
    private TypeMirrorTestUtils typeMirrorTestUtils;
    private Predicate<TypeMirror> validator;

    @Before
    public void prepare()
    {
        Types types = compilation.getTypes();
        Elements elements = compilation.getElements();
        TypeMirrorUtils typeMirrors = new TypeMirrorUtils( types, elements );

        typeMirrorTestUtils = new TypeMirrorTestUtils( compilation );
        validator = new AllowedTypesValidator( typeMirrors, types );
    }

    @Test
    public void unsupported_simple_type_is_invalid()
    {
        assertThat( validator.test( typeMirrorTestUtils.typeOf( CharSequence.class ) ) ).isFalse();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Thread.class ) ) ).isFalse();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Character.class ) ) ).isFalse();
    }

    @Test
    public void supported_simple_type_is_valid()
    {
        assertThat( validator.test( typeMirrorTestUtils.typeOf( BOOLEAN ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( LONG ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( DOUBLE ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Boolean.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Long.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Double.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( String.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Number.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Object.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Node.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Relationship.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Path.class ) ) ).isTrue();
    }

    @Test
    public void supported_list_type_is_valid()
    {
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, Boolean.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, Long.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, Double.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, String.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, Number.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, Object.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, Node.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, Relationship.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, Path.class ) ) ).isTrue();
    }

    @Test
    public void unsupported_list_type_is_invalid()
    {
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, CharSequence.class ) ) ).isFalse();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, Thread.class ) ) ).isFalse();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, Character.class ) ) ).isFalse();
    }

    @Test
    public void supported_recursive_list_type_is_valid()
    {
        assertThat( validator.test( typeMirrorTestUtils
                .typeOf( List.class, typeMirrorTestUtils.typeOf( List.class, Boolean.class ) ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class,
                typeMirrorTestUtils.typeOf( List.class, typeMirrorTestUtils.typeOf( List.class, Object.class ) ) ) ) )
                .isTrue();
    }

    @Test
    public void unsupported_recursive_list_type_is_invalid()
    {
        assertThat( validator.test( typeMirrorTestUtils
                .typeOf( List.class, typeMirrorTestUtils.typeOf( List.class, CharSequence.class ) ) ) ).isFalse();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class,
                typeMirrorTestUtils.typeOf( List.class, typeMirrorTestUtils.typeOf( List.class, Thread.class ) ) ) ) )
                .isFalse();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, typeMirrorTestUtils
                .typeOf( List.class, typeMirrorTestUtils.typeOf( List.class, Character.class ) ) ) ) ).isFalse();
    }

    @Test
    public void supported_map_type_is_valid()
    {
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, String.class, Boolean.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, String.class, Long.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, String.class, Double.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, String.class, String.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, String.class, Number.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, String.class, Object.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, String.class, Node.class ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, String.class, Relationship.class ) ) )
                .isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, String.class, Path.class ) ) ).isTrue();
    }

    @Test
    public void unsupported_map_type_is_invalid()
    {
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, Object.class, Boolean.class ) ) ).isFalse();
    }

    @Test
    public void supported_recursive_map_type_is_valid()
    {
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, typeMirrorTestUtils.typeOf( String.class ),
                typeMirrorTestUtils.typeOf( Map.class, String.class, Boolean.class ) ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, typeMirrorTestUtils.typeOf( String.class ),
                typeMirrorTestUtils.typeOf( Map.class, typeMirrorTestUtils.typeOf( String.class ),
                        typeMirrorTestUtils.typeOf( Map.class, String.class, Boolean.class ) ) ) ) ).isTrue();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, typeMirrorTestUtils
                .typeOf( List.class, typeMirrorTestUtils.typeOf( Map.class, String.class, Boolean.class ) ) ) ) )
                .isTrue();
    }

    @Test
    public void unsupported_recursive_map_type_is_invalid()
    {
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, typeMirrorTestUtils.typeOf( String.class ),
                typeMirrorTestUtils.typeOf( Map.class, String.class, Thread.class ) ) ) ).isFalse();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( Map.class, typeMirrorTestUtils.typeOf( String.class ),
                typeMirrorTestUtils.typeOf( Map.class, typeMirrorTestUtils.typeOf( String.class ),
                        typeMirrorTestUtils.typeOf( Map.class, String.class, CharSequence.class ) ) ) ) ).isFalse();
        assertThat( validator.test( typeMirrorTestUtils.typeOf( List.class, typeMirrorTestUtils
                .typeOf( List.class, typeMirrorTestUtils.typeOf( Map.class, String.class, Character.class ) ) ) ) )
                .isFalse();
    }

}
