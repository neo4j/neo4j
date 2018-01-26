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

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import org.neo4j.procedure.validation.errors.ExtensionParameterValidationFailure;
import org.neo4j.procedure.validation.errors.ExtensionValidationFailure;
import org.neo4j.procedure.validation.model.ElementType;
import org.neo4j.procedure.validation.model.ExtensionParameter;

public class ExtensionParameterValidator implements Function<ExtensionParameter,Optional<ExtensionValidationFailure>>
{
    private final Predicate<ExtensionParameter> hasNameAnnotation;
    private final Predicate<ExtensionParameter> hasNonEmptyNameAnnotationValue;
    private final Function<ElementType,Optional<ExtensionValidationFailure>> typeValidator;
    private final Predicate<List<ExtensionParameter>> defaultValuedParametersAreLast;

    public ExtensionParameterValidator( Predicate<ExtensionParameter> hasNameAnnotation,
            Predicate<ExtensionParameter> hasNonEmptyNameAnnotationValue,
            Function<ElementType,Optional<ExtensionValidationFailure>> typeValidator,
            Predicate<List<ExtensionParameter>> defaultValuedParametersAreLast )
    {
        this.hasNameAnnotation = hasNameAnnotation;
        this.hasNonEmptyNameAnnotationValue = hasNonEmptyNameAnnotationValue;
        this.typeValidator = typeValidator;
        this.defaultValuedParametersAreLast = defaultValuedParametersAreLast;
    }

    @Override
    public Optional<ExtensionValidationFailure> apply( ExtensionParameter extensionParameter )
    {
        if ( !hasNameAnnotation.test( extensionParameter ) )
        {
            return Optional.of( ExtensionParameterValidationFailure.MISSING_NAME_ANNOTATION );
        }
        if ( !hasNonEmptyNameAnnotationValue.test( extensionParameter ) )
        {
            return Optional.of( ExtensionParameterValidationFailure.EMPTY_NAME_ANNOTATION_VALUE );
        }

        return typeValidator.apply( extensionParameter.getType() );

    }

    public Optional<ExtensionValidationFailure> validateDefaultValuedParameterPosition(
            List<ExtensionParameter> parameters )
    {
        if ( !defaultValuedParametersAreLast.test( parameters ) )
        {
            return Optional.of( ExtensionParameterValidationFailure.DEFAULT_VALUE_NOT_FOUND_LAST );
        }
        return Optional.empty();
    }
}
