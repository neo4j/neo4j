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
package org.neo4j.internal.kernel.api.exceptions;

import static java.lang.String.format;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.neo4j.configuration.GraphDatabaseSettings.procedure_unrestricted;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Database.Unknown;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.Status.Security;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationResult;
import org.neo4j.procedure.UserAggregationUpdate;

public class ProcedureException extends KernelException {

    private ProcedureException(
            ErrorGqlStatusObject gqlStatusObject,
            Status statusCode,
            Throwable cause,
            String message,
            Object... parameters) {
        super(gqlStatusObject, statusCode, cause, message, parameters);
    }

    protected ProcedureException(
            ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Object... parameters) {
        super(gqlStatusObject, statusCode, message, parameters);
    }

    // KNL-034
    public static ProcedureException indexInFailedState(String indexName, String errorMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N62)
                .withParam(GqlParams.StringParam.idx, indexName)
                .build();
        return new ProcedureException(gql, Status.Schema.IndexCreationFailed, errorMessage);
    }

    public static ProcedureException indexDidNotComeOnline(
            String procedureName, String indexDescription, long timeout, TimeUnit timeoutUnits) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02)
                .withParam(GqlParams.StringParam.proc, procedureName)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N01)
                        .withParam(GqlParams.StringParam.proc, procedureName)
                        .withParam(GqlParams.NumberParam.timeAmount, timeout)
                        .withParam(
                                GqlParams.StringParam.timeUnit,
                                timeoutUnits.toString().toLowerCase(Locale.ROOT))
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureTimedOut,
                "Index on '%s' did not come online within %s %s",
                indexDescription,
                timeout,
                timeoutUnits);
    }

    public static ProcedureException noSuchProcedure(QualifiedName name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N08)
                        .withParam(GqlParams.StringParam.procFun, name.toString())
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureNotFound,
                "There is no procedure with the name `%s` registered for this database instance. "
                        + "Please ensure you've spelled the procedure name correctly and that the "
                        + "procedure is properly deployed.",
                name);
    }

    public static ProcedureException noSuchProcedureWithVersionHint(QualifiedName name, int cypherVersion) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N08)
                        .withParam(GqlParams.StringParam.procFun, name.toString())
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I78)
                                .withParam(GqlParams.NumberParam.version1, cypherVersion)
                                .build())
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureNotFound,
                "There is no procedure with the name `%s` registered for this database instance. "
                        + "Please ensure you've spelled the procedure name correctly and that the "
                        + "procedure is properly deployed.",
                name);
    }

    public static ProcedureException noSuchProcedure(int id) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N08)
                        .withParam(GqlParams.StringParam.procFun, Integer.toString(id))
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureNotFound,
                "There is no procedure with the internal id `%d` registered for this database instance.",
                id);
    }

    public static ProcedureException noSuchFunction(int id) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N08)
                        .withParam(GqlParams.StringParam.procFun, Integer.toString(id))
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureNotFound,
                "There is no function with the internal id `%d` registered for this database instance.",
                id);
    }

    public static ProcedureException noSuchProcedureOrFunction(String name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N08)
                .withParam(GqlParams.StringParam.procFun, name)
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                "There is no `%s` in the current procedure call context.",
                name);
    }

    public static ProcedureException unsupportedProcedureOnComposite(QualifiedName name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N7B)
                .withParam(GqlParams.StringParam.feat, "Unsupported procedure call")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N08)
                        .withParam(GqlParams.StringParam.procFun, name.toString())
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                "Procedure `%s` is not supported in a composite database.",
                name);
    }

    public static ProcedureException unsupportedProcedureOnShardedDb(QualifiedName name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N71)
                .withParam(GqlParams.StringParam.feat, "Unsupported procedure call")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N08)
                        .withParam(GqlParams.StringParam.procFun, name.toString())
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                "Procedure `%s` is not supported in a sharded database.",
                name);
    }

    public static ProcedureException noSuchConstituentGraph(String graphName, String ctxDatabaseName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N01)
                        .withParam(GqlParams.StringParam.graph, graphName)
                        .withParam(GqlParams.StringParam.db, ctxDatabaseName)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                "'%s' is not a constituent of composite database '%s'".formatted(graphName, ctxDatabaseName));
    }

    public static ProcedureException faultyClassFieldAnnotationStatic(String procField, String procClass) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N01)
                        .withParam(GqlParams.StringParam.procField, procField)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "The field `%s` in the class named `%s` is annotated as a @Context field,%n"
                        + "but it is static. @Context fields must be public, non-final and non-static,%n"
                        + "because they are reset each time a procedure is invoked.",
                procField,
                procClass);
    }

    public static ProcedureException unableToAccessFieldInjection(String procClass, String procField) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N03)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .withParam(GqlParams.StringParam.procField, procField)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Unable to set up injection for `%s`, failed to access field `%s",
                procClass,
                procField);
    }

    public static ProcedureException unableToAccessField(String procClass, String procField) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N03)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .withParam(GqlParams.StringParam.procField, procField)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.TypeError,
                "Field `%s` in record `%s` cannot be accessed. Please ensure the field is marked as `public`.",
                procField,
                procClass);
    }

    public static ProcedureException missingClassFieldAnnotation(String procClass, String procField) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N04)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .withParam(GqlParams.StringParam.procField, procField)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Field `%s` on `%s` is not annotated as a @" + Context.class.getSimpleName()
                        + " and is not static. If you want to store state along with your procedure,"
                        + " please use a static field.",
                procField,
                procClass);
    }

    public static ProcedureException faultyClassFieldAnnotation(String procClass, String procField) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N05)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .withParam(GqlParams.StringParam.procField, procField)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Field `%s` on `%s` must be non-final and public.",
                procField,
                procClass);
    }

    public static ProcedureException missingArgumentAnnotation(int position, String procMethod) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N06)
                        .withParam(GqlParams.NumberParam.pos, position)
                        .withParam(GqlParams.StringParam.procMethod, procMethod)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Argument at position %d in method `%s` is missing an `@%s` annotation.%n"
                        + "Please add the annotation, recompile the class and try again.",
                position,
                procMethod,
                Name.class.getSimpleName());
    }

    public static ProcedureException missingArgumentName(int position, String procMethod) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N06)
                        .withParam(GqlParams.NumberParam.pos, position)
                        .withParam(GqlParams.StringParam.procMethod, procMethod)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Argument at position %d in method `%s` is annotated with a name,%n"
                        + "but the name is empty, please provide a non-empty name for the argument.",
                position,
                procMethod,
                Name.class.getSimpleName());
    }

    public static ProcedureException invalidOrderingOfDefaultArguments(
            int position, String parameterValue, String procMethod) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N07)
                        .withParam(GqlParams.StringParam.procFun, "procedure")
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Non-default argument at position %d with name %s in method %s follows default argument. "
                        + "Add a default value or rearrange arguments so that the non-default values comes first.",
                position,
                parameterValue,
                procMethod);
    }

    public static ProcedureException duplicatedAnnotatedMethods(String procClass, String className) {

        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N08)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Class '%s' contains multiple methods annotated with '@%s'.",
                procClass,
                className);
    }

    public static ProcedureException missingAnnotatedMethods(String procClass) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N08)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .build())
                .build();

        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Class '%s' must contain methods annotated with both '@%s' as well as '@%s'.",
                procClass,
                UserAggregationResult.class.getSimpleName(),
                UserAggregationUpdate.class.getSimpleName());
    }

    public static ProcedureException methodMustBeVoid(String procClass, String procMethod, String returnType) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N09)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .withParam(GqlParams.StringParam.procMethod, procMethod)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Update method '%s' in %s has type '%s' but must have return type 'void'.",
                procMethod,
                procClass,
                returnType);
    }

    public static ProcedureException aggregationUpdateMethodNotPublic(String procClass, String procMethod) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N09)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .withParam(GqlParams.StringParam.procMethod, procMethod)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Aggregation update method '%s' in %s must be public.",
                procMethod,
                procClass);
    }

    public static ProcedureException aggregationMethodNotPublic(String procClass, String procMethod) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N10)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .withParam(GqlParams.StringParam.procMethod, procMethod)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Aggregation method '%s' in %s must be public.",
                procMethod,
                procClass);
    }

    public static ProcedureException aggregationResultMethodNotPublic(String procClass, String procMethod) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N10)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .withParam(GqlParams.StringParam.procMethod, procMethod)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Aggregation result method '%s' in %s must be public.",
                procMethod,
                procClass);
    }

    public static ProcedureException aggregationClassNotPublic(String procClass) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N11)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .build())
                .build();
        return new ProcedureException(
                gql, Status.Procedure.ProcedureRegistrationFailed, "Aggregation class '%s' must be public.", procClass);
    }

    public static ProcedureException unableToFindPublicConstructor(String procClass) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N11)
                        .withParam(GqlParams.StringParam.procClass, procClass)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Unable to find a usable public no-argument constructor in the class `%s`. "
                        + "Please add a valid, public constructor, recompile the class and try again.",
                procClass);
    }

    public static ProcedureException classNotVoid(String proc) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N12)
                        .withParam(GqlParams.StringParam.proc, proc)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Procedures with zero return columns must be declared as VOID");
    }

    public static ProcedureException procedureNameAlreadyInUse(String name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N13)
                        .withParam(GqlParams.StringParam.procFun, name)
                        .build())
                .build();

        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Unable to register procedure, because the name `%s` is already in use.",
                name);
    }

    public static ProcedureException functionNameAlreadyInUse(String name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N13)
                        .withParam(GqlParams.StringParam.procFun, name)
                        .build())
                .build();

        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Unable to register function, because the name `%s` is already in use.",
                name);
    }

    public static ProcedureException aggregationFunctionNameAlreadyInUse(String name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N13)
                        .withParam(GqlParams.StringParam.procFun, name)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Unable to register aggregation function, because the name `%s` is already in use.",
                name);
    }

    public static ProcedureException aggregationFunctionNameAlreadyInUseAsFunction(String name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N13)
                        .withParam(GqlParams.StringParam.procFun, name)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Unable to register aggregation function, because the name `%s` is already in use as a function.",
                name);
    }

    public static ProcedureException aggregationFunctionNameAlreadyInUseAsAggregationFunction(String name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N13)
                        .withParam(GqlParams.StringParam.procFun, name)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Unable to register function, because the name `%s` is already in use as an aggregation function.",
                name);
    }

    public static ProcedureException duplicateFieldName(String proc, String fieldType, String field) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N14)
                        .withParam(GqlParams.StringParam.proc, proc)
                        .withParam(GqlParams.StringParam.procFieldType, fieldType)
                        .withParam(GqlParams.StringParam.procField, field)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Procedure `%s` cannot be registered, because it contains a duplicated " + fieldType + " field, '%s'. "
                        + "You need to rename or remove one of the duplicate fields.",
                proc,
                field);
    }

    public static ProcedureException invalidMapKeyType(String typeName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N15)
                        .withParam(GqlParams.StringParam.valueType, typeName)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Maps are required to have `String` keys - but this map has `%s` keys.",
                typeName);
    }

    public static ProcedureException invalidDefaultValueType(String defaultValue, String type) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N16)
                        .withParam(GqlParams.StringParam.valueType, type)
                        .withParam(GqlParams.StringParam.input, defaultValue)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Default value `%s` could not be parsed as a %s",
                defaultValue,
                type);
    }

    public static ProcedureException nonReloadableNamespaces(List<String> nonReloadableNamespaces, Status statusCode) {
        ErrorGqlStatusObject gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N23)
                        .withParam(GqlParams.ListParam.namespaceList, nonReloadableNamespaces)
                        .build())
                .build();
        return new ProcedureException(
                gql, statusCode, "The following namespaces are not reloadable: %s.".formatted(nonReloadableNamespaces));
    }

    public static ProcedureException loadFailedProcedureRestricted(String proc) {
        var gql = GqlHelper.get52N34(proc);
        return new ProcedureException(gql, Status.Procedure.ProcedureRegistrationFailed, restrictedOldMessage(proc));
    }

    public static ProcedureException loadFailedFunctionRestricted(String func) {
        var gql = GqlHelper.get53N34(func);
        return new ProcedureException(gql, Status.Procedure.ProcedureRegistrationFailed, restrictedOldMessage(func));
    }

    private static String restrictedOldMessage(String name) {
        return name + " is unavailable because it is sandboxed and has dependencies outside of the sandbox. "
                + "Sandboxing is controlled by the "
                + procedure_unrestricted.name() + " setting. "
                + "Only unrestrict procedures you can trust with access to database internals.";
    }

    public static ProcedureException noSuchIndex(String indexName, String procedureName, Boolean formatIndex) {
        var gql = GqlHelper.get22N69_52N02(indexName, "db." + procedureName);
        if (formatIndex) {
            indexName = "'" + indexName + "'";
        }
        return new ProcedureException(gql, Status.Schema.IndexNotFound, "No such index %s", indexName);
    }

    public static ProcedureException surpressedRegisterFailed(List<Throwable> surpressedExceptions) {
        var exception = surpressedExceptions.get(surpressedExceptions.size() - 1);
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N00)
                .withParam(GqlParams.StringParam.msgTitle, exception.getClass().getName())
                .withParam(GqlParams.StringParam.msg, exception.getMessage())
                .build();
        for (int i = surpressedExceptions.size() - 2; i >= 0; i--) {
            exception = surpressedExceptions.get(i);
            gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N00)
                    .withParam(
                            GqlParams.StringParam.msgTitle, exception.getClass().getName())
                    .withParam(GqlParams.StringParam.msg, exception.getMessage())
                    .withCause(gql)
                    .build();
        }
        gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(gql)
                .build();

        var exc = new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                "Failed to register procedures for the following reasons:");
        for (var surpressedException : surpressedExceptions) {
            exc.addSuppressed(surpressedException);
        }
        return exc;
    }

    public static ProcedureException compilationFailed(boolean isProcedure, String name, Throwable cause) {
        ErrorGqlStatusObject gql;
        if (isProcedure) {

            gql = GqlHelper.get51N00_52N35(name, cause.getMessage());
        } else {
            gql = GqlHelper.get51N00_53N35(name, cause.getMessage());
        }
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureRegistrationFailed,
                cause,
                "Failed to compile %s defined in `%s`: %s",
                isProcedure ? "procedure" : "function",
                name,
                cause.getMessage());
    }

    public static ProcedureException invocationFailed(String type, String name, Throwable cause) {
        Throwable rootCause = getRootCause(cause); // Do we risk losing valuable information here
        String typeAndName = String.format("%s `%s`", type, name);
        ErrorGqlStatusObject gql = getInvocationFailedGqlStatus(cause, rootCause, type, name);

        if (cause instanceof Status.HasStatus statusException) {
            var message = cause.getMessage();
            if (cause instanceof ErrorGqlStatusObject gqlStatusObject) {
                message = gqlStatusObject.legacyMessage();
            }
            return new ProcedureException(gql, statusException.status(), cause, message);
        } else {
            return new ProcedureException(
                    gql,
                    Status.Procedure.ProcedureCallFailed,
                    cause,
                    "Failed to invoke %s: %s",
                    typeAndName,
                    "Caused by: " + (rootCause != null ? rootCause : cause));
        }
    }

    public static ProcedureException functionError(String name, String msg) {
        ErrorGqlStatusObject gql = GqlHelper.get53N37(
                name,
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_53U00)
                        .withParam(GqlParams.StringParam.fun, name)
                        .withParam(GqlParams.StringParam.msgTitle, ProcedureException.class.getSimpleName())
                        .withParam(GqlParams.StringParam.msg, msg)
                        .build());
        return new ProcedureException(gql, Status.Procedure.ProcedureCallFailed, msg);
    }

    private static ErrorGqlStatusObject getInvocationFailedGqlStatus(
            Throwable cause, Throwable rootCause, String type, String name) {
        if (type.equals("procedure")) {
            if (cause instanceof ErrorGqlStatusObject errorGqlStatusObject) {
                return GqlHelper.get52N37(name, errorGqlStatusObject);
            } else if (rootCause instanceof ErrorGqlStatusObject errorGqlStatusObject) {
                return GqlHelper.get52N37(name, errorGqlStatusObject);
            } else {
                return GqlHelper.get52N37(name, GqlHelper.get52U00(name, cause));
            }
        } else {
            if (cause instanceof ErrorGqlStatusObject errorGqlStatusObject) {
                return GqlHelper.get53N37(name, errorGqlStatusObject);
            } else if (rootCause instanceof ErrorGqlStatusObject errorGqlStatusObject) {
                return GqlHelper.get53N37(name, errorGqlStatusObject);
            } else {
                return GqlHelper.get53N37(name, GqlHelper.get53U00(name, cause));
            }
        }
    }

    public static <EX extends Throwable & Status.HasStatus & ErrorGqlStatusObject>
            ProcedureException invocationFailedWithInnerError(EX error, String type, String name) {
        ErrorGqlStatusObject gql;
        if (type.equals("procedure")) {
            gql = GqlHelper.get52N37(name, error);
        } else {
            gql = GqlHelper.get53N37(name, error);
        }
        return new ProcedureException(gql, error.status(), error, error.getMessage(), error);
    }

    public static ProcedureException invalidReturnType(String methodName, String badReturnValue) {
        String msg = String.format(
                "Procedures must return a Stream of records, where a record is a concrete class%n"
                        + "that you define and not a %s.",
                badReturnValue);
        var gql = GqlHelper.getGql51N00_51N18(methodName);
        return new ProcedureException(gql, Status.Procedure.TypeError, msg);
    }

    public static ProcedureException invalidReturnTypeExtended(String methodName, Class<?> userClass) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N18)
                        .withParam(GqlParams.StringParam.procMethod, methodName)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.TypeError,
                "Procedures must return a Stream of records, where a record is a concrete class%n"
                        + "that you define, with public non-final fields defining the fields in the record.%n"
                        + "If you''d like your procedure to return `%s`, you could define a record class like:%n"
                        + "public class Output '{'%n"
                        + "    public %s out;%n"
                        + "'}'%n"
                        + "%n"
                        + "And then define your procedure as returning `Stream<Output>`.",
                userClass.getSimpleName(),
                userClass.getSimpleName());
    }

    public static ProcedureException databaseNotFound(String databaseName) {
        var gql = GqlHelper.getGql22000_22N51(databaseName);
        return new ProcedureException(gql, DatabaseNotFound, "Unable to find database with name " + databaseName);
    }

    public static ProcedureException unableToRetrieveStatusForDatabaseNotFound(String databaseName) {
        var gql = GqlHelper.getGql22000_22N51(databaseName);
        return new ProcedureException(
                gql,
                DatabaseNotFound,
                format(
                        "Unable to retrieve the status " + "for database with name %s because no database "
                                + "with this name exists!",
                        databaseName));
    }

    public static ProcedureException invalidNumberOfProcedureOrFunctionArguments(
            Number expectedNumberOfArgs,
            Number obtainedNumberOfArgs,
            String procedureFunction,
            String signature,
            String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I13)
                        .withParam(GqlParams.NumberParam.count1, expectedNumberOfArgs)
                        .withParam(GqlParams.NumberParam.count2, obtainedNumberOfArgs)
                        .withParam(GqlParams.StringParam.procFun, procedureFunction)
                        .withParam(GqlParams.StringParam.sig, signature)
                        .build())
                .build();

        return new ProcedureException(gql, Status.Procedure.ProcedureCallFailed, legacyMessage);
    }

    public static ProcedureException invalidCallSignature(
            String procedureFunction, String signature, String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I51)
                        .withParam(GqlParams.StringParam.procFun, procedureFunction)
                        .withParam(GqlParams.StringParam.sig, signature)
                        .build())
                .build();

        return new ProcedureException(gql, Status.Procedure.ProcedureCallFailed, legacyMessage);
    }

    public static ProcedureException cannotInjectField(String procField, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N20)
                        .withParam(GqlParams.StringParam.procField, procField)
                        .build())
                .build();

        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                cause,
                "Unable to inject component to field `%s`, please ensure it is public and non-final: %s",
                procField,
                cause.getMessage());
    }

    public static ProcedureException unableToCheckLicense(String procedureName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N09)
                .withParam(GqlParams.StringParam.proc, procedureName)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N60)
                        .build())
                .build();

        return new ProcedureException(gql, ProcedureCallFailed, "Unable to determine license acceptance status");
    }

    public static ProcedureException jmxError(ObjectName name, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N25)
                .withParam(GqlParams.StringParam.param, name.getCanonicalName())
                .build();
        return new ProcedureException(
                gql,
                Status.General.UnknownError,
                e,
                "JMX error while accessing `%s`, please report this. Message was: %s",
                name,
                e.getMessage());
    }

    public static ProcedureException notWriter(String name) {
        return new ProcedureException(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02)
                        .withParam(GqlParams.StringParam.proc, name)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N07)
                                .build())
                        .build(),
                Status.Cluster.NotALeader,
                "No write operations are allowed directly on this database. Writes must pass through the writer.");
    }

    public static ProcedureException mustInvokeProcedureOnSecondary(String dbName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N05)
                .withParam(GqlParams.StringParam.db, dbName)
                .build();
        return new ProcedureException(
                gql,
                Unknown,
                String.format(
                        "Can't invoke procedure on this server because it is not a read replica for database '%s'",
                        dbName));
    }

    public static ProcedureException checkConnectivityWrongNumberArguments(int count) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N06)
                        .withParam(GqlParams.NumberParam.count, count)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                "Unexpected number of parameters: should have 0-2 parameters, but was %d",
                count);
    }

    public static ProcedureException checkConnectivityinvalidPortArgument(
            String port, Set<ConnectorType> portSet, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N07)
                        .withParam(GqlParams.StringParam.port, port)
                        .withParam(
                                GqlParams.ListParam.portList, portSet.stream().toList())
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                e,
                "Unrecognised port name '%s'. Valid values are: %s",
                port,
                Arrays.toString(portSet.toArray()));
    }

    public static ProcedureException checkConnectivityInvalidServerId(String server, String rawServer, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N08)
                        .withParam(GqlParams.StringParam.server, server)
                        .build())
                .build();
        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                e,
                "Provided identifier '%s' is not a valid server name or id",
                rawServer);
    }

    public static ProcedureException quarantineChangeFailed(String procedureName, Throwable e, boolean generalMessage) {
        var message = generalMessage ? "Please refer to the server's debug log for more information." : e.getMessage();
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02)
                .withParam(GqlParams.StringParam.proc, procedureName)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N17)
                        .withParam(GqlParams.StringParam.msg, message)
                        .build())
                .build();
        return new ProcedureException(
                gql, ProcedureCallFailed, e, "Setting/removing the quarantine marker failed: " + message);
    }

    public static ProcedureException topologyProcedureGeneralException(
            String procedureName, Throwable e, boolean generalMessage) {
        var message = generalMessage ? "Please refer to the server's debug log for more information." : e.getMessage();
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02)
                .withParam(GqlParams.StringParam.proc, procedureName)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N11)
                        .withParam(GqlParams.StringParam.msg, message)
                        .build())
                .build();
        return new ProcedureException(gql, ProcedureCallFailed, e, "An unexpected error has occurred: " + message);
    }

    /**
     * This one does not set 52N11 as the cause.
     */
    public static ProcedureException generalProcedureExceptionNoCause(String procedure, Status status, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N02)
                .withParam(GqlParams.StringParam.proc, procedure)
                .build();
        return new ProcedureException(gql, status, e, e.getMessage());
    }

    public static ProcedureException invalidProcedureArgument(
            String invalidArgumentValue,
            String invalidArgumentProcParamName,
            String invalidArgumentProcName,
            String invalidArgumentProcParamFmt,
            Status statusCode,
            String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N36)
                        .withParam(GqlParams.StringParam.field, invalidArgumentValue)
                        .withParam(GqlParams.StringParam.procParam, invalidArgumentProcParamName)
                        .withParam(GqlParams.StringParam.proc, invalidArgumentProcName)
                        .withParam(GqlParams.StringParam.procParamFmt, invalidArgumentProcParamFmt)
                        .build())
                .build();
        return new ProcedureException(gql, statusCode, legacyMessage);
    }

    public static ProcedureException invalidFunctionArgument(String signature, String msg) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_53N33)
                .withParam(GqlParams.StringParam.sig, signature)
                .withParam(GqlParams.StringParam.msg, msg)
                .build();
        return new ProcedureException(gql, ProcedureCallFailed, msg + ".");
    }

    public static ProcedureException invalidEmptyStringFunctionArgument(
            String argumentName, String signature, String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_53N33)
                .withParam(GqlParams.StringParam.sig, signature)
                .withParam(GqlParams.StringParam.msg, argumentName + " is not allowed to be an empty string")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB6)
                        .withParam(GqlParams.StringParam.item, argumentName)
                        .build())
                .build();
        return new ProcedureException(gql, Status.Procedure.ProcedureCallFailed, legacyMessage);
    }

    public static ProcedureException graphPropertiesNotFound(String graphName) {
        var gql = GqlHelper.getGql42001_42N00(graphName);

        return new ProcedureException(
                gql,
                Status.Procedure.ProcedureCallFailed,
                "Graph properties not found for graph '%s'".formatted(graphName));
    }

    public static ProcedureException failedToReloadProcedures(Throwable cause, String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N24)
                .build();
        return new ProcedureException(gql, ProcedureCallFailed, cause, legacyMessage);
    }

    public static ProcedureException wrongParameter(
            String providedInvalidArgument,
            String argumentName,
            String procedureName,
            String expectedFormat,
            String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N16)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N22)
                        .withParam(GqlParams.StringParam.field, providedInvalidArgument)
                        .withParam(GqlParams.StringParam.procParam, argumentName)
                        .withParam(GqlParams.StringParam.proc, procedureName)
                        .withParam(GqlParams.StringParam.procParamFmt, expectedFormat)
                        .build())
                .build();
        return new ProcedureException(gql, ProcedureCallFailed, legacyMessage);
    }

    public static ProcedureException shouldBeExecutedAgainstSystemDb() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N28)
                .withParam(GqlParams.StringParam.db, "system")
                .build();
        return new ProcedureException(
                gql,
                ProcedureCallFailed,
                "This is an administration command and it should be executed against the system database");
    }

    public static ProcedureException shouldBeExecutedAgainstSystemDb(String procedure) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N28)
                .withParam(GqlParams.StringParam.db, "system")
                .build();
        return new ProcedureException(
                gql,
                ProcedureCallFailed,
                "This is an administration command and it should be executed against the system database: %s",
                procedure);
    }

    public static ProcedureException failedToCleanSystemGraph(Exception e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N21)
                .build();
        return new ProcedureException(gql, ProcedureCallFailed, e, "Failed to clean the system graph");
    }

    public static ProcedureException unsupportedType(String input, List<String> cypherTypes) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB8)
                .withParam(GqlParams.StringParam.input, input)
                .build();
        return new ProcedureException(
                gql,
                Status.Statement.TypeError,
                "Don't know how to map `%s` to the Neo4j Type System.%n"
                        + "Please refer to to the documentation for full details.%n"
                        + "For your reference, known types are: %s",
                input,
                cypherTypes);
    }

    public static ProcedureException argumentWithUnsupportedType(
            String arg, int pos, String method, String javaType, ProcedureException cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB8)
                .withParam(GqlParams.StringParam.input, javaType)
                .build();
        return new ProcedureException(
                gql,
                cause.status(),
                "Argument `%s` at position %d in `%s` with%n" + "type `%s` cannot be converted to a Neo4j type: %s",
                arg,
                pos,
                method,
                javaType,
                cause.getMessage());
    }

    public static ProcedureException fieldWithUnsupportedType(
            String field, String userClass, ProcedureException cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NB8)
                .withParam(GqlParams.StringParam.input, userClass)
                .build();
        return new ProcedureException(
                gql,
                cause.status(),
                cause,
                "Field `%s` in record `%s` cannot be converted to a Neo4j type: %s",
                field,
                userClass,
                cause.getMessage());
    }

    public static ProcedureException internalError(String msgTitle, String message, Status status, Throwable cause) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new ProcedureException(gql, status, cause, message);
    }

    public static ProcedureException internalError(String msgTitle, String message, Status status) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new ProcedureException(gql, status, message);
    }

    public static ProcedureException permissionDenied(String message) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new ProcedureException(gql, Security.Forbidden, message);
    }

    public static ProcedureException reconcilerFailed() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_52N40)
                .build();
        return new ProcedureException(gql, ProcedureCallFailed, gql.getMessage());
    }
}
