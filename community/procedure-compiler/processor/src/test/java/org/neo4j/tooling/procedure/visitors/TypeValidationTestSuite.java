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

import org.neo4j.tooling.procedure.testutils.TypeMirrorTestUtils;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeVisitor;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import static org.assertj.core.api.Assertions.assertThat;

abstract class TypeValidationTestSuite
{

    @Test
    public void validates_supported_simple_types()
    {
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( String.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Number.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Long.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( TypeKind.LONG ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Double.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( TypeKind.DOUBLE ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Boolean.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( TypeKind.BOOLEAN ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Path.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Node.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Relationship.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Object.class ) ) ).isTrue();
    }

    @Test
    public void validates_supported_generic_types()
    {
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Map.class, String.class, Object.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( HashMap.class, String.class, Object.class ) ) )
                .isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( LinkedHashMap.class, String.class, Object.class ) ) )
                .isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( List.class, String.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( LinkedList.class, Number.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( ArrayList.class, Long.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( List.class, Double.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( List.class, Boolean.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( List.class, Path.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( List.class, Node.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( List.class, Relationship.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( List.class, Object.class ) ) ).isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils()
                .typeOf( List.class, typeMirrorTestUtils().typeOf( Map.class, String.class, Object.class ) ) ) )
                .isTrue();
        assertThat( visitor().visit( typeMirrorTestUtils()
                .typeOf( List.class, typeMirrorTestUtils().typeOf( LinkedList.class, Long.class ) ) ) ).isTrue();
    }

    @Test
    public void rejects_unsupported_types()
    {
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Thread.class ) ) ).isFalse();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Map.class, String.class, Integer.class ) ) )
                .isFalse();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Map.class, Integer.class, Object.class ) ) )
                .isFalse();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( Map.class, Integer.class, Integer.class ) ) )
                .isFalse();
        assertThat( visitor().visit( typeMirrorTestUtils().typeOf( List.class, BigDecimal.class ) ) ).isFalse();
        assertThat( visitor().visit( typeMirrorTestUtils()
                .typeOf( List.class, typeMirrorTestUtils().typeOf( Map.class, String.class, Integer.class ) ) ) )
                .isFalse();
        assertThat( visitor().visit( typeMirrorTestUtils()
                .typeOf( List.class, typeMirrorTestUtils().typeOf( List.class, CharSequence.class ) ) ) ).isFalse();
    }

    protected abstract TypeVisitor<Boolean,Void> visitor();

    protected abstract TypeMirrorTestUtils typeMirrorTestUtils();

}
