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

import java.util.List;
import java.util.Objects;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.string.Globbing;

interface NamingRestrictions {
    void verify(QualifiedName name) throws IllegalNamingException;

    static NamingRestrictions allOf(NamingRestrictions... restrictions) {
        return (name) -> {
            for (var restriction : restrictions) {
                restriction.verify(name);
            }
        };
    }

    static NamingRestrictions rejectEmptyNamespace() {
        return (name) -> {
            if (name.namespace() == null || name.namespace().length == 0) {
                throw new IllegalNamingException(
                        "It is not allowed to define functions in the root namespace. Please define a namespace, "
                                + "e.g. `@UserFunction(\"org.example.com.%s\")",
                        name.name());
            }
        };
    }

    static NamingRestrictions rejectReservedNamespace(List<String> namespaces) {
        Objects.requireNonNull(namespaces);
        final var filter = Globbing.compose(namespaces, List.of());
        return (name) -> {
            if (filter.test(name.toString())) {
                throw new IllegalNamingException(
                        "It is not allowed to define procedures or functions in the reserved namespaces %s, consider using a proper package name instead e.g. \"org.example.com.%s\"",
                        namespaces, name.name());
            }
        };
    }

    static NamingRestrictions rejectNone() {
        return (name) -> {};
    }

    class IllegalNamingException extends ProcedureException {
        IllegalNamingException(String message, Object... parameters) {
            super(Status.Procedure.ProcedureRegistrationFailed, message, parameters);
        }

        IllegalNamingException(ErrorGqlStatusObject gqlStatusObject, String message, Object... parameters) {
            super(gqlStatusObject, Status.Procedure.ProcedureRegistrationFailed, message, parameters);
        }
    }
}
