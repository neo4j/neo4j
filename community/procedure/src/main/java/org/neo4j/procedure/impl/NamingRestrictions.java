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
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.InternalLog;
import org.neo4j.string.Globbing;

public interface NamingRestrictions {
    void verify(QualifiedName name, QueryLanguage queryLanguage, InternalLog log) throws IllegalNamingException;

    static NamingRestrictions rejectNone() {
        return (name, queryLanguage, log) -> {};
    }

    static NamingRestrictions rejectEmptyNamespace() {
        return (name, queryLanguage, log) -> {
            if (name.namespace() == null || name.namespace().length == 0) {
                throw IllegalNamingException.invalidNameRootNamespace(name.name());
            }
        };
    }

    static NamingRestrictions rejectProcedureReservations() {
        return (name, queryLanguage, log) -> {
            final var reservationFilter = Globbing.compose(reservedProcedureNamespaces, List.of());
            if (reservationFilter.test(name.toString())) {
                throw IllegalNamingException.invalidNameReservedNameSpace(reservedProcedureNamespaces, name.name());
            }
            var deprecatedNamespaces = deprecatedProcedureNamespaces(queryLanguage);
            final var deprecationFilter = Globbing.compose(deprecatedNamespaces, List.of());
            if (deprecationFilter.test(name.toString()) || isDeprecatedInRoot(name, queryLanguage)) {
                log.warn(
                        "The procedure `%s` is in a deprecated namespace. The namespace is deprecated in %s.",
                        name.toString(), queryLanguage.name().replace("_", " "));
            }
        };
    }

    static NamingRestrictions rejectFunctionReservations() {
        return (name, queryLanguage, log) -> {
            if (name.namespace() == null || name.namespace().length == 0) {
                throw IllegalNamingException.invalidNameRootNamespace(name.name());
            }

            final var reservationFilter = Globbing.compose(reservedFunctionNamespaces, List.of());
            if (reservationFilter.test(name.toString())) {
                throw IllegalNamingException.invalidNameReservedNameSpace(reservedFunctionNamespaces, name.name());
            }
            var deprecatedNamespaces = deprecatedFunctionNamespaces(queryLanguage);
            final var deprecationFilter = Globbing.compose(deprecatedNamespaces, List.of());
            if (deprecationFilter.test(name.toString())) {
                log.warn(
                        "The function `%s` is in a deprecated namespace. The namespace is deprecated in %s.",
                        name.toString(), queryLanguage.name().replace("_", " "));
            }
        };
    }

    static Boolean isDeprecatedProcedureNamespace(QualifiedName name, QueryLanguage queryLanguage) {
        var deprecatedNamespaces = deprecatedProcedureNamespaces(queryLanguage);
        final var filter = Globbing.compose(deprecatedNamespaces, List.of());
        return filter.test(name.toString()) || isDeprecatedInRoot(name, queryLanguage);
    }

    static Boolean isDeprecatedFunctionNamespace(QualifiedName name, QueryLanguage queryLanguage) {
        var deprecatedNamespaces = deprecatedFunctionNamespaces(queryLanguage);
        final var filter = Globbing.compose(deprecatedNamespaces, List.of());
        return filter.test(name.toString());
    }

    static Boolean isShadowingBuiltInFunction(QualifiedName name, QueryLanguage queryLanguage) {
        return namespacedBuiltInFunctions(queryLanguage).contains(name.toString());
    }

    static Boolean isDeprecatedInRoot(QualifiedName name, QueryLanguage queryLanguage) {
        return queryLanguage != QueryLanguage.CYPHER_5 && (name.namespace() == null || name.namespace().length < 1);
    }

    // The reserved namespaces are the same for Cypher 5 and Cypher 25
    List<String> reservedProcedureNamespaces = List.of(
            "date",
            "datetime",
            "duration",
            "localdatetime",
            "localtime",
            "time",
            "cdc.*",
            "date.*",
            "datetime.*",
            "db.*",
            "dbms.*",
            "duration.*",
            "graph.*",
            "internal.*",
            "localdatetime.*",
            "localtime.*",
            "time.*",
            "tx.*",
            "unsupported.*");

    // The reserved namespaces are the same for Cypher 5 and Cypher 25
    List<String> reservedFunctionNamespaces = List.of(
            "date",
            "date.realtime",
            "date.statement",
            "date.transaction",
            "date.truncate",
            "datetime",
            "datetime.fromepoch",
            "datetime.fromepochmillis",
            "datetime.realtime",
            "datetime.statement",
            "datetime.transaction",
            "datetime.truncate",
            "db.nameFromElementId",
            "duration",
            "duration.between",
            "duration.inDays",
            "duration.inMonths",
            "duration.inSeconds",
            "graph.byElementId",
            "graph.byName",
            "graph.names",
            "graph.propertiesByName",
            "localdatetime",
            "localdatetime.realtime",
            "localdatetime.statement",
            "localdatetime.transaction",
            "localdatetime.truncate",
            "localtime",
            "localtime.realtime",
            "localtime.statement",
            "localtime.transaction",
            "localtime.truncate",
            "point.distance",
            "point.withinBBox",
            "time",
            "time.realtime",
            "time.statement",
            "time.transaction",
            "time.truncate",
            "vector.similarity.cosine",
            "vector.similarity.euclidean");

    static List<String> deprecatedProcedureNamespaces(QueryLanguage queryLanguage) {
        return switch (queryLanguage) {
            case CYPHER_5 -> List.of();

            case CYPHER_25 ->
                List.of(
                        // Deprecated namespaces
                        "abac.*",
                        "aura.*",
                        "builtin.*",
                        "coll.*",
                        "math.*",
                        "plugin.*",
                        "point.*",
                        "stored.*",
                        "string.*",
                        "vector.*");
        };
    }

    static List<String> deprecatedFunctionNamespaces(QueryLanguage queryLanguage) {
        return switch (queryLanguage) {
            case CYPHER_5 -> List.of();

            case CYPHER_25 ->
                List.of(
                        // Deprecated namespaces
                        "abac.*",
                        "aura.*",
                        "builtin.*",
                        "cdc.*",
                        "coll.*",
                        "date.*",
                        "datetime.*",
                        "db.*",
                        "dbms.*",
                        "duration.*",
                        "graph.*",
                        "internal.*",
                        "localdatetime.*",
                        "localtime.*",
                        "math.*",
                        "plugin.*",
                        "point.*",
                        "stored.*",
                        "string.*",
                        "time.*",
                        "tx.*",
                        "unsupported.*",
                        "vector.*");
        };
    }

    static List<String> namespacedBuiltInFunctions(QueryLanguage queryLanguage) {
        // It is a list of all built-in functions that have a namespace
        // so they can be checked for shadowing
        var combined = List.of(
                "date.realtime",
                "date.statement",
                "date.transaction",
                "date.truncate",
                "datetime.fromepoch",
                "datetime.fromepochmillis",
                "datetime.realtime",
                "datetime.statement",
                "datetime.transaction",
                "datetime.truncate",
                "db.nameFromElementId",
                "duration.between",
                "duration.inDays",
                "duration.inMonths",
                "duration.inSeconds",
                "graph.byElementId",
                "graph.byName",
                "graph.names",
                "graph.propertiesByName",
                "localdatetime.realtime",
                "localdatetime.statement",
                "localdatetime.transaction",
                "localdatetime.truncate",
                "localtime.realtime",
                "localtime.statement",
                "localtime.transaction",
                "localtime.truncate",
                "point.distance",
                "point.withinBBox",
                "time.realtime",
                "time.statement",
                "time.transaction",
                "time.truncate",
                "vector.similarity.cosine",
                "vector.similarity.euclidean");

        var cypher25Additional = List.of(
                "coll.distinct",
                "coll.flatten",
                "coll.indexOf",
                "coll.insert",
                "coll.max",
                "coll.min",
                "coll.remove",
                "coll.sort");

        var cypher25All = new java.util.ArrayList<>(combined);
        cypher25All.addAll(cypher25Additional);

        return switch (queryLanguage) {
            case CYPHER_5 -> combined;
            case CYPHER_25 -> cypher25All;
        };
    }

    class IllegalNamingException extends ProcedureException {

        private IllegalNamingException(ErrorGqlStatusObject gqlStatusObject, String message, Object... parameters) {
            super(gqlStatusObject, Status.Procedure.ProcedureRegistrationFailed, message, parameters);
        }

        public static IllegalNamingException invalidNameReservedNameSpace(List<String> namespaces, String name) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                    .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N17)
                            .withParam(GqlParams.StringParam.procFun, name)
                            .build())
                    .build();
            return new IllegalNamingException(
                    gql,
                    "It is not allowed to define procedures or functions in the reserved namespaces %s, consider using a proper package name instead e.g. \"org.example.com.%s\"",
                    namespaces,
                    name);
        }

        public static IllegalNamingException invalidNameRootNamespace(String name) {
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                    .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N17)
                            .withParam(GqlParams.StringParam.procFun, name)
                            .build())
                    .build();
            return new IllegalNamingException(
                    gql,
                    "It is not allowed to define functions in the root namespace. Please define a namespace, "
                            + "e.g. `@UserFunction(\"org.example.com.%s\")",
                    name);
        }
    }
}
