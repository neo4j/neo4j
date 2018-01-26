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

import org.neo4j.procedure.validation.errors.ExtensionValidationFailure;
import org.neo4j.procedure.validation.errors.FunctionValidationFailure;
import org.neo4j.procedure.validation.functional.OptionalStreams;
import org.neo4j.procedure.validation.model.ExtensionConstructor;
import org.neo4j.procedure.validation.model.ExtensionParameter;
import org.neo4j.procedure.validation.model.ExtensionQualifiedName;
import org.neo4j.procedure.validation.model.UserFunctionDefinition;

public class UserFunctionValidator implements Function<UserFunctionDefinition,Optional<ExtensionValidationFailure>>
{

    private final Predicate<ExtensionConstructor> isNoArgConstructor;
    private final Predicate<ExtensionQualifiedName> hasWhitelistedName;
    private final Predicate<ExtensionQualifiedName> isRootNamespace;
    private final ExtensionParameterValidator extensionParameterValidator;
    private final ElementTypeValidator elementTypeValidator;
    private final ExtensionFieldValidator extensionFieldValidator;

    public UserFunctionValidator( Predicate<ExtensionConstructor> isNoArgConstructor,
            Predicate<ExtensionQualifiedName> whitelistedName, Predicate<ExtensionQualifiedName> isRootNamespace,
            ExtensionParameterValidator extensionParameterValidator, ElementTypeValidator elementTypeValidator,
            ExtensionFieldValidator extensionFieldValidator )
    {

        this.isNoArgConstructor = isNoArgConstructor;
        this.hasWhitelistedName = whitelistedName;
        this.isRootNamespace = isRootNamespace;
        this.extensionParameterValidator = extensionParameterValidator;
        this.elementTypeValidator = elementTypeValidator;
        this.extensionFieldValidator = extensionFieldValidator;
    }

    @Override
    public Optional<ExtensionValidationFailure> apply( UserFunctionDefinition userFunctionDefinition )
    {
        ExtensionQualifiedName name = userFunctionDefinition.getName();
        if ( !isNoArgConstructor.test( userFunctionDefinition.getConstructor() ) )
        {
            return Optional.of( FunctionValidationFailure.MISSING_NO_ARG_CONSTRUCTOR );
        }
        if ( !hasWhitelistedName.test( name ) )
        {
            return Optional.of( FunctionValidationFailure.NOT_WHITELISTED );
        }
        if ( !isRootNamespace.test( name ) )
        {
            return Optional.of( FunctionValidationFailure.IN_ROOT_NAMESPACE );
        }

        List<ExtensionParameter> parameters = userFunctionDefinition.getParameters();
        Optional<ExtensionValidationFailure> parameterFailure =
                OptionalStreams.flatMap( parameters.stream(), extensionParameterValidator ).findFirst();

        if ( parameterFailure.isPresent() )
        {
            return parameterFailure;
        }

        Optional<ExtensionValidationFailure> defaultValuePositionFailure =
                extensionParameterValidator.validateDefaultValuedParameterPosition( parameters );
        if ( defaultValuePositionFailure.isPresent() )
        {
            return defaultValuePositionFailure;
        }

        Optional<ExtensionValidationFailure> returnTypeFailure =
                elementTypeValidator.apply( userFunctionDefinition.getReturnType() );
        if ( returnTypeFailure.isPresent() )
        {
            return returnTypeFailure;
        }

        return OptionalStreams.flatMap( userFunctionDefinition.getExtensionFields().stream(), extensionFieldValidator )
                .findFirst();

    }
}
