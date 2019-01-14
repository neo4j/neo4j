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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.SimpleTypeVisitor8;
import javax.lang.model.util.Types;

import org.neo4j.tooling.procedure.compilerutils.TypeMirrorUtils;

/**
 * This predicate makes sure that a given declared type (record field type,
 * procedure parameter type...) is supported by Neo4j stored procedures.
 */
public class AllowedTypesValidator implements Predicate<TypeMirror>
{

    private final TypeMirrorUtils typeMirrors;
    private final Collection<TypeMirror> whitelistedTypes;
    private final Types typeUtils;

    public AllowedTypesValidator( TypeMirrorUtils typeMirrors, Types typeUtils )
    {

        this.typeMirrors = typeMirrors;
        this.whitelistedTypes = typeMirrors.procedureAllowedTypes();
        this.typeUtils = typeUtils;
    }

    @Override
    public boolean test( TypeMirror typeMirror )
    {
        TypeMirror erasedActualType = typeUtils.erasure( typeMirror );

        return isValidErasedType( erasedActualType ) &&
                (!isSameErasedType( List.class, typeMirror ) || isValidListType( typeMirror )) &&
                (!isSameErasedType( Map.class, typeMirror ) || isValidMapType( typeMirror ));
    }

    private boolean isValidErasedType( TypeMirror actualType )
    {
        return whitelistedTypes.stream().anyMatch( type ->
        {
            TypeMirror erasedAllowedType = typeUtils.erasure( type );

            TypeMirror map = typeUtils.erasure( typeMirrors.typeMirror( Map.class ) );
            TypeMirror list = typeUtils.erasure( typeMirrors.typeMirror( List.class ) );
            if ( typeUtils.isSameType( erasedAllowedType, map ) || typeUtils.isSameType( erasedAllowedType, list ) )
            {
                return typeUtils.isSubtype( actualType, erasedAllowedType );
            }

            return typeUtils.isSameType( actualType, erasedAllowedType );
        } );
    }

    /**
     * Recursively visits List type arguments
     *
     * @param typeMirror the List type mirror
     * @return true if the declaration is valid, false otherwise
     */
    private boolean isValidListType( TypeMirror typeMirror )
    {
        return new SimpleTypeVisitor8<Boolean,Void>()
        {
            @Override
            public Boolean visitDeclared( DeclaredType list, Void aVoid )
            {
                List<? extends TypeMirror> typeArguments = list.getTypeArguments();
                return typeArguments.size() == 1 && test( typeArguments.get( 0 ) );
            }
        }.visit( typeMirror );
    }

    /**
     * Recursively visits Map type arguments
     * Map key type argument must be a String as of Neo4j stored procedure specification
     * Map value type argument is recursively visited
     *
     * @param typeMirror Map type mirror
     * @return true if the declaration is valid, false otherwise
     */
    private boolean isValidMapType( TypeMirror typeMirror )
    {
        return new SimpleTypeVisitor8<Boolean,Void>()
        {
            @Override
            public Boolean visitDeclared( DeclaredType map, Void ignored )
            {
                List<? extends TypeMirror> typeArguments = map.getTypeArguments();
                if ( typeArguments.size() != 2 )
                {
                    return Boolean.FALSE;
                }

                TypeMirror key = typeArguments.get( 0 );
                return typeUtils.isSameType( key, typeMirrors.typeMirror( String.class ) ) &&
                        test( typeArguments.get( 1 ) );
            }
        }.visit( typeMirror );
    }

    private boolean isSameErasedType( Class<?> type, TypeMirror typeMirror )
    {
        return typeUtils
                .isSameType( typeUtils.erasure( typeMirrors.typeMirror( type ) ), typeUtils.erasure( typeMirror ) );
    }

}
