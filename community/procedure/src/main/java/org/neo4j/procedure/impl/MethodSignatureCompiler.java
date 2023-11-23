/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.impl;

import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Sensitive;
import org.neo4j.procedure.impl.TypeCheckers.DefaultValueConverter;

/**
 * Given a java method, figures out a valid {@link ProcedureSignature} field signature.
 * Basically, it takes the java signature and spits out the same signature described as Neo4j types.
 */
class MethodSignatureCompiler {
    private final TypeCheckers typeCheckers;

    MethodSignatureCompiler(TypeCheckers typeCheckers) {
        this.typeCheckers = typeCheckers;
    }

    List<FieldSignature> signatureFor(Method method) throws ProcedureException {
        Parameter[] params = method.getParameters();
        Type[] types = method.getGenericParameterTypes();
        List<FieldSignature> signature = new ArrayList<>(params.length);
        boolean seenDefault = false;
        for (int i = 0; i < params.length; i++) {
            Parameter param = params[i];
            Type type = types[i];

            if (!param.isAnnotationPresent(Name.class)) {
                throw new ProcedureException(
                        Status.Procedure.ProcedureRegistrationFailed,
                        "Argument at position %d in method `%s` is missing an `@%s` annotation.%n"
                                + "Please add the annotation, recompile the class and try again.",
                        i,
                        method.getName(),
                        Name.class.getSimpleName());
            }
            Name parameter = param.getAnnotation(Name.class);
            String name = parameter.value();

            if (name.isBlank()) {
                throw new ProcedureException(
                        Status.Procedure.ProcedureRegistrationFailed,
                        "Argument at position %d in method `%s` is annotated with a name,%n"
                                + "but the name is empty, please provide a non-empty name for the argument.",
                        i,
                        method.getName());
            }

            try {
                DefaultValueConverter valueConverter = typeCheckers.converterFor(type);
                Optional<DefaultParameterValue> defaultValue = valueConverter.defaultValue(parameter.defaultValue());
                // it is not allowed to have holes in default values
                if (seenDefault && !defaultValue.isPresent()) {
                    throw new ProcedureException(
                            Status.Procedure.ProcedureRegistrationFailed,
                            "Non-default argument at position %d with name %s in method %s follows default argument. "
                                    + "Add a default value or rearrange arguments so that the non-default values comes first.",
                            i,
                            parameter.value(),
                            method.getName());
                }

                seenDefault = defaultValue.isPresent();

                boolean isSensitive = param.isAnnotationPresent(Sensitive.class);

                signature.add(defaultValue
                        .map(neo4jValue -> inputField(name, valueConverter.type(), neo4jValue, isSensitive))
                        .orElseGet(() -> inputField(name, valueConverter.type(), isSensitive)));
            } catch (ProcedureException e) {
                throw new ProcedureException(
                        e.status(),
                        "Argument `%s` at position %d in `%s` with%n"
                                + "type `%s` cannot be converted to a Neo4j type: %s",
                        name,
                        i,
                        method.getName(),
                        param.getType().getSimpleName(),
                        e.getMessage());
            }
        }

        return signature;
    }
}
