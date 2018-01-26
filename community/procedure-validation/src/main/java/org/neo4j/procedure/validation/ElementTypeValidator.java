/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.procedure.validation;

import java.util.Optional;
import java.util.function.Function;

import org.neo4j.procedure.validation.errors.ElementTypeValidationFailure;
import org.neo4j.procedure.validation.errors.ExtensionValidationFailure;
import org.neo4j.procedure.validation.functional.ConditionalPredicate;
import org.neo4j.procedure.validation.model.ElementType;

public class ElementTypeValidator implements Function<ElementType,Optional<ExtensionValidationFailure>>
{
    private final ConditionalPredicate<ElementType> isBasicTypeSupported;
    private final ConditionalPredicate<ElementType> isValidListType;
    private final ConditionalPredicate<ElementType> isValidMapType;

    public ElementTypeValidator( ConditionalPredicate<ElementType> isBasicTypeSupported,
            ConditionalPredicate<ElementType> isValidListType, ConditionalPredicate<ElementType> isValidMapType )
    {
        this.isBasicTypeSupported = isBasicTypeSupported;
        this.isValidListType = isValidListType;
        this.isValidMapType = isValidMapType;
    }

    @Override
    public Optional<ExtensionValidationFailure> apply( ElementType elementType )
    {
        if ( isValidListType.isApplicableTo( elementType ) )
        {
            if ( !isValidListType.test( elementType ) )
            {
                return Optional.of( ElementTypeValidationFailure.UNSUPPORTED_TYPE );
            }
            return Optional.empty();
        }

        if ( isValidMapType.isApplicableTo( elementType ) )
        {
            if ( !isValidMapType.test( elementType ) )
            {
                return Optional.of( ElementTypeValidationFailure.UNSUPPORTED_MAP_TYPE );
            }
            return Optional.empty();
        }

        if ( !isBasicTypeSupported.test( elementType ) )
        {
            return Optional.of( ElementTypeValidationFailure.UNSUPPORTED_TYPE );
        }
        return Optional.empty();
    }
}
