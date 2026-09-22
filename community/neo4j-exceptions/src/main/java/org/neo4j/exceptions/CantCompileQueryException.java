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
package org.neo4j.exceptions;

import java.util.List;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class CantCompileQueryException extends Neo4jException {

    private CantCompileQueryException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    private CantCompileQueryException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static CantCompileQueryException internalError(String msgTitle, String message) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new CantCompileQueryException(gql, message);
    }

    public static CantCompileQueryException internalError(String msgTitle, String message, Throwable cause) {
        var gql = GqlHelper.get50N00(msgTitle, message);
        return new CantCompileQueryException(gql, message, cause);
    }

    public static CantCompileQueryException unsupportedRuntimeInCommunityEdition(String runtime) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N27)
                        .withParam(GqlParams.StringParam.feat, "'%s'".formatted(runtime))
                        .withParam(GqlParams.StringParam.edition, "community edition")
                        .build())
                .build();
        return new CantCompileQueryException(
                gql, String.format("This version of Neo4j does not support the requested runtime: `%s`", runtime));
    }

    public static CantCompileQueryException actionUserUnsupportedInCommunityEdition(
            String feature, String action, String user, List<String> commands) {
        var verb = commands.size() == 1 ? "is" : "are";
        var commandString = String.join(", ", commands);
        var legacyMessage = "Failed to %s the specified user '%s': %s %s not available in community edition."
                .formatted(action, user, commandString, verb);

        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N27)
                .withParam(GqlParams.StringParam.feat, feature)
                .withParam(GqlParams.StringParam.edition, "community edition")
                .build();
        return new CantCompileQueryException(gql, legacyMessage);
    }

    public static CantCompileQueryException planUnsupportedInCommunityEdition(String planName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N27)
                .withParam(GqlParams.StringParam.feat, "'%s'".formatted(planName))
                .withParam(GqlParams.StringParam.edition, "community edition")
                .build();
        return new CantCompileQueryException(
                gql,
                "Plan is not a recognized database administration command in community edition: %s"
                        .formatted(planName));
    }

    public static CantCompileQueryException commandUnsupportedInCommunityEdition(String component) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N27)
                .withParam(GqlParams.StringParam.feat, "'%s'".formatted(component))
                .withParam(GqlParams.StringParam.edition, "community edition")
                .build();
        return new CantCompileQueryException(gql, GqlHelper.getCompleteMessage(gql));
    }

    public static CantCompileQueryException planNotRecognisedInAdminCommand(String unknownPlan) {
        var msg = String.format("Plan is not a recognized database administration command: %s", unknownPlan);
        var gql = GqlHelper.get50N00(CantCompileQueryException.class.getSimpleName(), msg);
        return new CantCompileQueryException(gql, msg);
    }

    public static CantCompileQueryException planNotSchemaCommand(String unknownPlan) {
        var msg = String.format("Plan is not a schema command: %s", unknownPlan);
        var gql = GqlHelper.get50N00(CantCompileQueryException.class.getSimpleName(), msg);
        return new CantCompileQueryException(gql, msg);
    }

    public static CantCompileQueryException aggregatingInPipelined(String functionName) {
        var msg = String.format(
                "Pipelined does not yet support the Aggregating function `%s`, use another runtime.", functionName);
        var gql = GqlHelper.getGql22000_22N48("pipelined", functionName);
        return new CantCompileQueryException(gql, msg);
    }

    public static CantCompileQueryException mergeNotYetSupported() {
        var msg = "This merge is not yet supported";
        var gql = GqlHelper.getGql22000_22N48("pipelined", GqlParams.StringParam.cmd.process("MERGE"));
        return new CantCompileQueryException(gql, msg);
    }

    public static CantCompileQueryException unsupportedPlanInRuntime(String planComponent, String runtime) {
        var msg = String.format(
                "%s does not yet support the plans including `%s`, use another runtime.", runtime, planComponent);
        var gql = GqlHelper.getGql22000_22N48(runtime, planComponent);
        return new CantCompileQueryException(gql, msg);
    }

    public static CantCompileQueryException unsupportedFallbackMiddlePlanInRuntime(
            String planComponent, String runtime) {
        var msg = String.format(
                "%s does not yet support using `%s` as a fallback middle plan, use another runtime.",
                runtime, planComponent);
        var gql = GqlHelper.getGql22000_22N48(runtime, planComponent);
        return new CantCompileQueryException(gql, msg);
    }

    public static CantCompileQueryException unsupportedMiddlePlanInRuntime(String planComponent, String runtime) {
        var msg = String.format(
                "%s does not yet support using `%s` as a middle plan, use another runtime.", runtime, planComponent);
        var gql = GqlHelper.getGql22000_22N48(runtime, planComponent);
        return new CantCompileQueryException(gql, msg);
    }

    public static CantCompileQueryException unsupportedInRuntime(String unsupported, String runtime) {
        var msg = String.format("%s does not yet support %s, use another runtime.", runtime, unsupported);
        var gql = GqlHelper.getGql22000_22N48(runtime, unsupported);
        return new CantCompileQueryException(gql, msg);
    }

    public static CantCompileQueryException unsupportedInSlotted(String unsupported) {
        var msg = String.format("Slotted runtime does not support %s", unsupported);
        var gql = GqlHelper.getGql22000_22N48("slotted", unsupported);
        return new CantCompileQueryException(gql, msg);
    }

    public static CantCompileQueryException unsupportedWritePlanInParallelRuntime(String planComponent) {
        var msg = String.format(
                "Updating plan %s can only be executed within a CALL {...} IN TRANSACTION subquery scope with parallel runtime.",
                planComponent);
        var gql = GqlHelper.getGql22000_22N48("parallel", planComponent);
        return new CantCompileQueryException(gql, msg);
    }

    @Override
    public Status status() {
        return Status.Statement.ExecutionFailed;
    }
}
