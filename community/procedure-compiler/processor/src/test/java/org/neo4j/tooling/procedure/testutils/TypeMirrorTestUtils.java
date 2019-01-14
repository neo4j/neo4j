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
package org.neo4j.tooling.procedure.testutils;

import com.google.testing.compile.CompilationRule;
import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;

import java.util.stream.Stream;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public class TypeMirrorTestUtils
{

    private final Types types;
    private final Elements elements;
    private final TypeMirrorUtils typeMirrors;

    public TypeMirrorTestUtils( CompilationRule rule )
    {
        this( rule.getTypes(), rule.getElements(), new TypeMirrorUtils( rule.getTypes(), rule.getElements() ) );
    }

    private TypeMirrorTestUtils( Types types, Elements elements, TypeMirrorUtils typeMirrors )
    {
        this.types = types;
        this.elements = elements;
        this.typeMirrors = typeMirrors;
    }

    public TypeMirror typeOf( Class<?> type, Class<?>... parameterTypes )
    {
        return types.getDeclaredType( elements.getTypeElement( type.getName() ), typesOf( parameterTypes ) );
    }

    public TypeMirror typeOf( Class<?> type, TypeMirror... parameterTypes )
    {
        return types.getDeclaredType( elements.getTypeElement( type.getName() ), parameterTypes );
    }

    public PrimitiveType typeOf( TypeKind kind )
    {
        return typeMirrors.primitive( kind );
    }

    public TypeMirror typeOf( Class<?> type )
    {
        return typeMirrors.typeMirror( type );
    }

    private TypeMirror[] typesOf( Class<?>... parameterTypes )
    {
        Stream<TypeMirror> mirrorStream = stream( parameterTypes ).map( this::typeOf );
        return mirrorStream.collect( toList() ).toArray( new TypeMirror[parameterTypes.length] );
    }
}
