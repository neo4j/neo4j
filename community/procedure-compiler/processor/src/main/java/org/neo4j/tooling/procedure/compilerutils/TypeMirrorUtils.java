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
package org.neo4j.tooling.procedure.compilerutils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

import static java.util.Arrays.asList;

public class TypeMirrorUtils
{

    private Types typeUtils;
    private Elements elementUtils;

    public TypeMirrorUtils( Types typeUtils, Elements elementUtils )
    {
        this.typeUtils = typeUtils;
        this.elementUtils = elementUtils;
    }

    public final Collection<TypeMirror> procedureAllowedTypes()
    {
        PrimitiveType bool = primitive( TypeKind.BOOLEAN );
        PrimitiveType longType = primitive( TypeKind.LONG );
        PrimitiveType doubleType = primitive( TypeKind.DOUBLE );
        return asList( bool, boxed( bool ), longType, boxed( longType ), doubleType, boxed( doubleType ),
                typeMirror( String.class ), typeMirror( Number.class ), typeMirror( Object.class ),
                typeMirror( Map.class ), typeMirror( List.class ), typeMirror( Node.class ),
                typeMirror( Relationship.class ), typeMirror( Path.class ) );
    }

    public PrimitiveType primitive( TypeKind kind )
    {
        return typeUtils.getPrimitiveType( kind );
    }

    public TypeMirror typeMirror( Class<?> type )
    {
        return elementUtils.getTypeElement( type.getName() ).asType();
    }

    private TypeMirror boxed( PrimitiveType bool )
    {
        return typeUtils.boxedClass( bool ).asType();
    }
}
