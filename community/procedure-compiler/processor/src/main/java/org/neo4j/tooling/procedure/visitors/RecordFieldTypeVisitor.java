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

import java.util.function.Predicate;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.PrimitiveType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.SimpleTypeVisitor8;
import javax.lang.model.util.Types;

import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;
import org.neo4j.tooling.procedure.validators.AllowedTypesValidator;

class RecordFieldTypeVisitor extends SimpleTypeVisitor8<Boolean,Void>
{

    private final Predicate<TypeMirror> allowedTypesValidator;

    RecordFieldTypeVisitor( Types typeUtils, TypeMirrorUtils typeMirrors )
    {
        allowedTypesValidator = new AllowedTypesValidator( typeMirrors, typeUtils );
    }

    @Override
    public Boolean visitDeclared( DeclaredType declaredType, Void ignored )
    {
        return allowedTypesValidator.test( declaredType ) &&
                declaredType.getTypeArguments().stream().map( this::visit ).reduce( ( a, b ) -> a && b ).orElse( Boolean.TRUE );
    }

    @Override
    public Boolean visitPrimitive( PrimitiveType primitiveType, Void ignored )
    {
        return allowedTypesValidator.test( primitiveType );
    }
}
