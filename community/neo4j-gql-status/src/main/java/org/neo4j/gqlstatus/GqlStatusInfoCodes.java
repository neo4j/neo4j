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
package org.neo4j.gqlstatus;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.CONFIG_SETTING;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.CYPHER_CONSTRUCT;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.CYPHER_VARIABLE;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.FIXED_TEXT;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.METADATA;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.NON_SENSITIVE_NUMBER;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.NO_PARAMETERS;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.PROCEDURES_FUNCTIONS;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.SCHEMA;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.TEMPORAL_SPATIAL;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.TOPOLOGY;
import static org.neo4j.gqlstatus.NonSensitiveStatusDescription.Reason.VALUE_TYPE;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum GqlStatusInfoCodes implements GqlStatusInfo {
    STATUS_00000(
            new GqlStatus("00000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "",
            NotificationClassification.UNKNOWN),
    STATUS_00001(
            new GqlStatus("00001"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "omitted result",
            NotificationClassification.UNKNOWN),
    STATUS_00N50(
            new GqlStatus("00N50"),
            "The database { %s } does not exist. Verify that the spelling is correct or create the database for the command to take effect.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "home database does not exist",
            NotificationClassification.UNRECOGNIZED),
    STATUS_00N70(
            new GqlStatus("00N70"),
            "The command { %s } has no effect. The role or privilege is already assigned.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "role or privilege already assigned",
            NotificationClassification.SECURITY),
    STATUS_00N71(
            new GqlStatus("00N71"),
            "The command { %s } has no effect. The role or privilege is not assigned.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "role or privilege not assigned",
            NotificationClassification.SECURITY),
    STATUS_00N72(
            new GqlStatus("00N72"),
            "The auth provider { %s } is not defined in the configuration. Verify that the spelling is correct or define { %s } in the configuration.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.auth, GqlParams.StringParam.auth},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "undefined auth provider",
            NotificationClassification.SECURITY),
    STATUS_00N80(
            new GqlStatus("00N80"),
            "The command 'ENABLE SERVER' has no effect. Server { %s } is already enabled. Verify that this is the intended server.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "server already enabled",
            NotificationClassification.TOPOLOGY),
    STATUS_00N81(
            new GqlStatus("00N81"),
            "The command 'CORDON SERVER' has no effect. The server { %s } is already cordoned. Verify that this is the intended server.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "server already cordoned",
            NotificationClassification.TOPOLOGY),
    STATUS_00N82(
            new GqlStatus("00N82"),
            "The command 'REALLOCATE DATABASES' has no effect. No databases were reallocated. No better allocation is currently possible.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "no databases reallocated",
            NotificationClassification.TOPOLOGY),
    STATUS_00N83(
            new GqlStatus("00N83"),
            "Cordoned servers existed when making an allocation decision. Server(s) { %s } are cordoned. This can impact allocation decisions.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.serverList},
            Map.of(GqlParams.ListParam.serverList, GqlParams.JoinStyle.ANDED),
            Condition.SUCCESSFUL_COMPLETION,
            "cordoned servers existed during allocation",
            NotificationClassification.TOPOLOGY),
    STATUS_00N84(
            new GqlStatus("00N84"),
            "The command 'ALTER DATABASE' has no effect. The requested topology matched the current topology. No allocations were changed.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "requested topology matched current topology",
            NotificationClassification.TOPOLOGY),
    STATUS_00NA0(
            new GqlStatus("00NA0"),
            "The command { %s } has no effect. The index or constraint specified by { %s } already exists.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd, GqlParams.StringParam.idxOrConstrPat},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "index or constraint already exists",
            NotificationClassification.SCHEMA),
    STATUS_00NA1(
            new GqlStatus("00NA1"),
            "The command { %s } has no effect. The specified index or constraint { %s } does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd, GqlParams.StringParam.idxOrConstr},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "index or constraint does not exist",
            NotificationClassification.SCHEMA),
    STATUS_00NA2(
            new GqlStatus("00NA2"),
            "The vector index was created without `vector.dimensions`. This is allowed, but providing dimensions when creating a vector index is recommended, as it ensures that only vectors of that size are indexed and makes dimension mismatches fail clearly at query time. For example, set `OPTIONS { indexConfig: { `vector.dimensions`: 1536 } }` when creating the index.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SUCCESSFUL_COMPLETION,
            "vector index dimensions not specified",
            NotificationClassification.SCHEMA),
    STATUS_01000(
            new GqlStatus("01000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.WARNING,
            "",
            NotificationClassification.UNKNOWN),
    STATUS_01004(
            new GqlStatus("01004"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.WARNING,
            "string data, right truncation",
            NotificationClassification.UNKNOWN),
    STATUS_01G03(
            new GqlStatus("01G03"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.WARNING,
            "graph does not exist",
            NotificationClassification.UNKNOWN),
    STATUS_01G04(
            new GqlStatus("01G04"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.WARNING,
            "graph type does not exist",
            NotificationClassification.UNKNOWN),
    STATUS_01G11(
            new GqlStatus("01G11"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.WARNING,
            "null value eliminated in set function",
            NotificationClassification.UNRECOGNIZED),
    STATUS_01N00(
            new GqlStatus("01N00"),
            "{ %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.item},
            emptyMap(),
            Condition.WARNING,
            "feature deprecated",
            NotificationClassification.DEPRECATION),
    STATUS_01N01(
            new GqlStatus("01N01"),
            "{ %s } is deprecated. It is replaced by { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.feat1, GqlParams.StringParam.feat2},
            emptyMap(),
            Condition.WARNING,
            "feature deprecated with replacement",
            NotificationClassification.DEPRECATION),
    STATUS_01N02(
            new GqlStatus("01N02"),
            "{ %s } is deprecated and will be removed without a replacement.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.feat},
            emptyMap(),
            Condition.WARNING,
            "feature deprecated without replacement",
            NotificationClassification.DEPRECATION),
    STATUS_01N30(
            new GqlStatus("01N30"),
            "Unable to create a plan with 'JOIN ON { %s }'. Try to change the join key(s) or restructure your query.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.variableList},
            Map.of(GqlParams.ListParam.variableList, GqlParams.JoinStyle.COMMAD),
            Condition.WARNING,
            "join hint unfulfillable",
            NotificationClassification.HINT),
    STATUS_01N31(
            new GqlStatus("01N31"),
            "Unable to create a plan with { %s } because the index does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idxDescr},
            emptyMap(),
            Condition.WARNING,
            "hinted index does not exist",
            NotificationClassification.HINT),
    STATUS_01N40(
            new GqlStatus("01N40"),
            "The query cannot be executed with { %s }; instead, { %s } is used. Cause: { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.preparserInput1, GqlParams.StringParam.preparserInput2, GqlParams.StringParam.msg
            },
            emptyMap(),
            Condition.WARNING,
            "unsupported runtime",
            NotificationClassification.UNSUPPORTED),
    STATUS_01N42(
            new GqlStatus("01N42"),
            "Unknown warning.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.WARNING,
            "unknown warning",
            NotificationClassification.UNKNOWN),
    STATUS_01N50(
            new GqlStatus("01N50"),
            "The label { %s } does not exist in database { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.label, GqlParams.StringParam.db},
            emptyMap(),
            Condition.WARNING,
            "label does not exist",
            NotificationClassification.UNRECOGNIZED),
    STATUS_01N51(
            new GqlStatus("01N51"),
            "The relationship type { %s } does not exist in database { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.relType, GqlParams.StringParam.db},
            emptyMap(),
            Condition.WARNING,
            "relationship type does not exist",
            NotificationClassification.UNRECOGNIZED),
    STATUS_01N52(
            new GqlStatus("01N52"),
            "The property { %s } does not exist in database { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.propKey, GqlParams.StringParam.db},
            emptyMap(),
            Condition.WARNING,
            "property key does not exist",
            NotificationClassification.UNRECOGNIZED),
    STATUS_01N60(
            new GqlStatus("01N60"),
            "The query plan cannot be cached and is not executable without 'EXPLAIN' due to the undefined parameter(s) { %s }. Provide the parameter(s).",
            new GqlParams.GqlParam[] {GqlParams.ListParam.paramList},
            Map.of(GqlParams.ListParam.paramList, GqlParams.JoinStyle.ANDED),
            Condition.WARNING,
            "parameter missing",
            NotificationClassification.GENERIC),
    STATUS_01N61(
            new GqlStatus("01N61"),
            "The expression { %s } cannot be satisfied because relationships must have exactly one type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.labelExpr},
            emptyMap(),
            Condition.WARNING,
            "unsatisfiable relationship type expression",
            NotificationClassification.GENERIC),
    STATUS_01N62(
            new GqlStatus("01N62"),
            "Execution of the procedure { %s } generated the warning { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.WARNING,
            "procedure or function execution warning",
            NotificationClassification.GENERIC),
    STATUS_01N63(
            new GqlStatus("01N63"),
            "{ %s } is repeated in { %s }, which leads to no results.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable, GqlParams.StringParam.pat},
            emptyMap(),
            Condition.WARNING,
            "repeated relationship pattern variable",
            NotificationClassification.GENERIC),
    STATUS_01N70(
            new GqlStatus("01N70"),
            "The command { %s } has no effect. Make sure nothing is misspelled. This notification will become an error in a future major version. Cause: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.WARNING,
            "inoperational revoke command",
            NotificationClassification.SECURITY),
    STATUS_01N71(
            new GqlStatus("01N71"),
            "Use the setting 'dbms.security.require_local_user' to enable external auth.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.WARNING,
            "external auth disabled",
            NotificationClassification.SECURITY),
    STATUS_01N72(
            new GqlStatus("01N72"),
            "Query uses an insecure protocol. Consider using 'https' instead.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.WARNING,
            "insecure URL protocol",
            NotificationClassification.SECURITY),
    STATUS_01N73(
            new GqlStatus("01N73"),
            "The specified privilege severely reduces the performance of queries run on sharded databases. Consider excluding sharded databases for now.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.WARNING,
            "sharded privilege performance",
            NotificationClassification.SECURITY),
    STATUS_01N74(
            new GqlStatus("01N74"),
            "Use the setting 'dbms.security.allow_oidc_credential_forwarding_enabled' to enable OIDC credential forwarding.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.WARNING,
            "OIDC credential forwarding disabled",
            NotificationClassification.SECURITY),

    STATUS_01N80(
            new GqlStatus("01N80"),
            "Server `{ %s }` at address `{ %s }` failed: { %s }",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.server, GqlParams.StringParam.serverAddress, GqlParams.StringParam.msg
            },
            emptyMap(),
            Condition.WARNING,
            "server failed",
            NotificationClassification.TOPOLOGY),
    STATUS_01N81(
            new GqlStatus("01N81"),
            "Server `{ %s }` at address `{ %s }` is still catching up.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server, GqlParams.StringParam.serverAddress},
            emptyMap(),
            Condition.WARNING,
            "server is catching up",
            NotificationClassification.TOPOLOGY),
    STATUS_01N82(
            new GqlStatus("01N82"),
            "Server `{ %s }` is not available.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            emptyMap(),
            Condition.WARNING,
            "server is not available",
            NotificationClassification.TOPOLOGY),
    STATUS_01N83(
            new GqlStatus("01N83"),
            "Client does not support type `{ %s }`. Please upgrade your client.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.valueType},
            emptyMap(),
            Condition.WARNING,
            "client does not support type",
            NotificationClassification.UNSUPPORTED),

    STATUS_02000(
            new GqlStatus("02000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.NO_DATA,
            "",
            NotificationClassification.UNKNOWN),
    STATUS_02N42(
            new GqlStatus("02N42"),
            "Unknown GQLSTATUS from old server.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.NO_DATA,
            "unknown subcondition",
            NotificationClassification.UNKNOWN),
    STATUS_03000(
            new GqlStatus("03000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INFORMATIONAL,
            "",
            NotificationClassification.UNKNOWN),
    STATUS_03N42(
            new GqlStatus("03N42"),
            "Unknown notification.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INFORMATIONAL,
            "unknown notification",
            NotificationClassification.UNKNOWN),
    STATUS_03N60(
            new GqlStatus("03N60"),
            "The variable { %s } in the subquery uses the same name as a variable from the outer query. Use '{ %s } ({ %s })' to import the one from the outer scope unless you want it to be a new variable.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.variable, GqlParams.StringParam.clause, GqlParams.StringParam.variable
            },
            emptyMap(),
            Condition.INFORMATIONAL,
            "subquery variable shadowing",
            NotificationClassification.GENERIC),
    STATUS_03N61(
            new GqlStatus("03N61"),
            "The use of `OPTIONAL` is redundant as `CALL { %s }` is a void procedure.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc},
            emptyMap(),
            Condition.INFORMATIONAL,
            "redundant optional procedure",
            NotificationClassification.GENERIC),
    STATUS_03N62(
            new GqlStatus("03N62"),
            "The use of `OPTIONAL` is redundant as `CALL` is a unit subquery.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INFORMATIONAL,
            "redundant optional subquery",
            NotificationClassification.GENERIC),
    STATUS_03N63(
            new GqlStatus("03N63"),
            "The identifier { %s } in the { %s } clause has the same name as a variable in scope. Regardless of what the variable evaluates to, it is the literal { %s } that will be used.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.ident, GqlParams.StringParam.clause, GqlParams.StringParam.ident
            },
            emptyMap(),
            Condition.INFORMATIONAL,
            "identifier shadowing variable",
            NotificationClassification.GENERIC),
    STATUS_03N85(
            new GqlStatus("03N85"),
            "Server `{ %s }` at address `{ %s }` has caught up.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server, GqlParams.StringParam.serverAddress},
            emptyMap(),
            Condition.INFORMATIONAL,
            "server has caught up",
            NotificationClassification.GENERIC),
    STATUS_03N90(
            new GqlStatus("03N90"),
            "The disconnected pattern { %s } builds a cartesian product. A cartesian product may produce a large amount of data and slow down query processing.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pat},
            emptyMap(),
            Condition.INFORMATIONAL,
            "cartesian product",
            NotificationClassification.PERFORMANCE),
    STATUS_03N91(
            new GqlStatus("03N91"),
            "The provided pattern { %s } is unbounded. Shortest path with an unbounded pattern may result in long execution times. Use an upper limit (e.g. '[*..5]') on the number of node hops in your pattern.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pat},
            emptyMap(),
            Condition.INFORMATIONAL,
            "unbounded variable length pattern",
            NotificationClassification.PERFORMANCE),
    STATUS_03N92(
            new GqlStatus("03N92"),
            "The query runs with exhaustive shortest path due to the existential predicate(s) { %s }. It may be possible to use 'WITH' to separate the 'MATCH' from the existential predicate(s).",
            new GqlParams.GqlParam[] {GqlParams.ListParam.predList},
            Map.of(GqlParams.ListParam.predList, GqlParams.JoinStyle.ANDED),
            Condition.INFORMATIONAL,
            "exhaustive shortest path",
            NotificationClassification.PERFORMANCE),
    STATUS_03N93(
            new GqlStatus("03N93"),
            "'LOAD CSV' in combination with 'MATCH' or 'MERGE' on a label that does not have an index may result in long execution times. Consider adding an index for label { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.label},
            emptyMap(),
            Condition.INFORMATIONAL,
            "no applicable index",
            NotificationClassification.PERFORMANCE),
    STATUS_03N94(
            new GqlStatus("03N94"),
            "The query execution plan contains the 'Eager' operator. 'LOAD CSV' in combination with 'Eager' can consume a lot of memory.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INFORMATIONAL,
            "eager operator",
            NotificationClassification.PERFORMANCE),
    STATUS_03N95(
            new GqlStatus("03N95"),
            "An index already exists on the relationship type or the label(s) { %s }. It is not possible to use indexes for dynamic properties. Consider using static properties.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.labelList},
            Map.of(GqlParams.ListParam.labelList, GqlParams.JoinStyle.ANDED),
            Condition.INFORMATIONAL,
            "dynamic property",
            NotificationClassification.PERFORMANCE),
    STATUS_03N96(
            new GqlStatus("03N96"),
            "Failed to generate code, falling back to interpreted { %s } engine. A stacktrace can be found in the debug.log. Cause: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cfgSetting, GqlParams.StringParam.cause},
            emptyMap(),
            Condition.INFORMATIONAL,
            "code generation failed",
            NotificationClassification.PERFORMANCE),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_08000(
            new GqlStatus("08000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_08007(
            new GqlStatus("08007"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "transaction resolution unknown",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_08N00(
            new GqlStatus("08N00"),
            "Unable to connect to database { %s }. Unable to get bolt address of the leader. Check the status of the database. Retrying your request at a later time may succeed.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "unable to connect to database",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY, CONFIG_SETTING})
    STATUS_08N01(
            new GqlStatus("08N01"),
            "Unable to write to database { %s } on this server. Server-side routing is disabled. Either connect to the database leader directly or enable server-side routing by setting '{ %s }=true'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db, GqlParams.StringParam.cfgSetting},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "unable to write to database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY, CONFIG_SETTING})
    STATUS_08N02(
            new GqlStatus("08N02"),
            "Unable to connect to database { %s }. Server-side routing is disabled. Either connect to { %s } directly, or enable server-side routing by setting '{ %s }=true'.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.db, GqlParams.StringParam.db, GqlParams.StringParam.cfgSetting
            },
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "unable to route to database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_08N03(
            new GqlStatus("08N03"),
            "Failed to write to graph { %s }. Check the defined access mode in both driver and database.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.graph},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "failed to write to graph",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_08N04(
            new GqlStatus("08N04"),
            "Routing with { %s } is not supported in embedded sessions. Connect to the database directly or try running the query using a Neo4j driver or the HTTP API.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.clause},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "unable to route use clause",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_08N05(
            new GqlStatus("08N05"),
            "Routing administration commands is not supported in embedded sessions. Connect to the system database directly or try running the query using a Neo4j driver or the HTTP API.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "unable to route administration command",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_08N06(
            new GqlStatus("08N06"),
            "General network protocol error.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "protocol error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_08N07(
            new GqlStatus("08N07"),
            "This member is not the leader.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "not the leader",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_08N08(
            new GqlStatus("08N08"),
            "This database is read only on this server.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "database is read only",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_08N09(
            new GqlStatus("08N09"),
            "The database { %s } is currently unavailable. Check the database status. Retry your request at a later time.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "database unavailable",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_08N10(
            new GqlStatus("08N10"),
            "Message { %s } cannot be handled by session in the { %s } state.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg, GqlParams.StringParam.boltServerState},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "invalid server state",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_08N11(
            new GqlStatus("08N11"),
            "The request is invalid and could not be processed by the server. See cause for further details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "request error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_08N12(
            new GqlStatus("08N12"),
            "Failed to parse the supplied bookmark. Verify it is correct or check the debug log for more information.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "failed to parse bookmark",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_08N13(
            new GqlStatus("08N13"),
            "The database { %s } is not up to the requested bookmark { %s }. The latest transaction ID is { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.db, GqlParams.StringParam.transactionId1, GqlParams.StringParam.transactionId2
            },
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "database not up to requested bookmark",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_08N14(
            new GqlStatus("08N14"),
            "Unable to provide a routing table for the database identifed by the alias { %s } because the request comes from another alias { %s } and alias chains are not permitted.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.alias1, GqlParams.StringParam.alias2},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "alias chains are not permitted",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_08N15(
            new GqlStatus("08N15"),
            "Policy definition of the routing policy { %s } could not be found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.routingPolicy},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "no such routing policy",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N16(
            new GqlStatus("08N16"),
            "Remote execution failed with message { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "general driver client error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N17(
            new GqlStatus("08N17"),
            "Remote execution failed with message { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "general driver transient error",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_08N18(
            new GqlStatus("08N18"),
            "Remote execution failed with message { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "general driver database error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_08N19(
            new GqlStatus("08N19"),
            "Communication with shard { %s } failed with message '{ %s }'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.graph, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "shard execution transient error",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_08N20(
            new GqlStatus("08N20"),
            "Communication with shard { %s } failed with message '{ %s }'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.graph, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "shard execution database error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_08N21(
            new GqlStatus("08N21"),
            "Communication with shard { %s } failed with message '{ %s }'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.graph, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.CONNECTION_EXCEPTION,
            "shard execution client error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22000(
            new GqlStatus("22000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22001(
            new GqlStatus("22001"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "string data, right truncation",
            ErrorClassification.UNKNOWN),
    STATUS_22003(
            new GqlStatus("22003"),
            "The numeric value { %s } is outside the required range.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value
            }, // StringParam since sometimes we will have ex. value > Long.MaxValue which would need to be parsed out
            // (and then back to String in the end)
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "numeric value out of range",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22004(
            new GqlStatus("22004"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "null value not allowed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22007(
            new GqlStatus("22007"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime format",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22008(
            new GqlStatus("22008"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "datetime field overflow",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22011(
            new GqlStatus("22011"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "substring error",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22012(
            new GqlStatus("22012"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "division by zero",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22015(
            new GqlStatus("22015"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "interval field overflow",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22018(
            new GqlStatus("22018"),
            "The character value { %s } is an invalid argument for the specified cast.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid character value for cast",
            ErrorClassification.UNKNOWN),
    STATUS_2201E(
            new GqlStatus("2201E"),
            "The value { %s } is an invalid argument for the specified natural logarithm.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid argument for natural logarithm",
            ErrorClassification.UNKNOWN),
    STATUS_2201F(
            new GqlStatus("2201F"),
            "The value { %s } is an invalid argument for the specified power function.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid argument for power function",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22027(
            new GqlStatus("22027"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "trim error",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_2202F(
            new GqlStatus("2202F"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "array data, right truncation",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G02(
            new GqlStatus("22G02"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "negative limit value",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G03(
            new GqlStatus("22G03"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid value type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G04(
            new GqlStatus("22G04"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "values not comparable",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G05(
            new GqlStatus("22G05"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime function field name",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G06(
            new GqlStatus("22G06"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid datetime function value",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G07(
            new GqlStatus("22G07"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid duration function field name",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0B(
            new GqlStatus("22G0B"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "list data, right truncation",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0C(
            new GqlStatus("22G0C"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "list element error",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0F(
            new GqlStatus("22G0F"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid number of paths or groups",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {TEMPORAL_SPATIAL})
    STATUS_22G0H(
            new GqlStatus("22G0H"),
            "The duration format { %s } is invalid.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.format},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid duration format",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {TEMPORAL_SPATIAL})
    STATUS_22G0I(
            new GqlStatus("22G0I"),
            "{ %s } is not a valid duration field.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.field},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid duration field",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0M(
            new GqlStatus("22G0M"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "multiple assignments to a graph element property",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0N(
            new GqlStatus("22G0N"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "number of node labels below supported minimum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0P(
            new GqlStatus("22G0P"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "number of node labels exceeds supported maximum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0Q(
            new GqlStatus("22G0Q"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "number of edge labels below supported minimum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0R(
            new GqlStatus("22G0R"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "number of edge labels exceeds supported maximum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0S(
            new GqlStatus("22G0S"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "number of node properties exceeds supported maximum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0T(
            new GqlStatus("22G0T"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "number of edge properties exceeds supported maximum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0U(
            new GqlStatus("22G0U"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "record fields do not match",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0V(
            new GqlStatus("22G0V"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "reference value, invalid base type",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0W(
            new GqlStatus("22G0W"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "reference value, invalid constrained type",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0X(
            new GqlStatus("22G0X"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "record data, field unassignable",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0Y(
            new GqlStatus("22G0Y"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "record data, field missing",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G0Z(
            new GqlStatus("22G0Z"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "malformed path",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G10(
            new GqlStatus("22G10"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "path data, right truncation",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G11(
            new GqlStatus("22G11"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "reference value, referent deleted",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G13(
            new GqlStatus("22G13"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid group variable value",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22G14(
            new GqlStatus("22G14"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "incompatible temporal instant unit groups",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N00(
            new GqlStatus("22N00"),
            "The provided value is unsupported and cannot be processed.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unsupported value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N01(
            new GqlStatus("22N01"),
            "Expected the value { %s } to be of type { %s }, but was of type { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.value, GqlParams.ListParam.valueTypeList, GqlParams.StringParam.valueType
            },
            Map.of(GqlParams.ListParam.valueTypeList, GqlParams.JoinStyle.ORED),
            Condition.DATA_EXCEPTION,
            "invalid type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT, TOPOLOGY, NON_SENSITIVE_NUMBER})
    STATUS_22N02(
            new GqlStatus("22N02"),
            "Expected { %s } to be a positive number but found { %s } instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.option, GqlParams.NumberParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "specified negative numeric value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N03(
            new GqlStatus("22N03"),
            "Expected { %s } to be of type { %s } and in the range { %s } to { %s } but found { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.component,
                GqlParams.StringParam.valueType,
                GqlParams.StringParam.lower,
                GqlParams.StringParam.upper,
                GqlParams.StringParam.value
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "specified numeric value out of range",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N04(
            new GqlStatus("22N04"),
            "Invalid input { %s } for { %s }. Expected { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input, GqlParams.StringParam.context, GqlParams.ListParam.inputList,
            },
            Map.of(GqlParams.ListParam.inputList, GqlParams.JoinStyle.ORED),
            Condition.DATA_EXCEPTION,
            "invalid input value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N05(
            new GqlStatus("22N05"),
            "Invalid input { %s } for { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input, GqlParams.StringParam.context},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "input failed validation",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_22N06(
            // See also 22NB6
            new GqlStatus("22N06"),
            "Invalid input. { %s } needs to be specified.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.inputList},
            Map.of(GqlParams.ListParam.inputList, GqlParams.JoinStyle.ANDED),
            Condition.DATA_EXCEPTION,
            "required input missing",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_22N07(
            new GqlStatus("22N07"),
            "Invalid pre-parser option(s): { %s }.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.optionList},
            Map.of(GqlParams.ListParam.optionList, GqlParams.JoinStyle.COMMAD),
            Condition.DATA_EXCEPTION,
            "invalid pre-parser option key",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_22N08(
            new GqlStatus("22N08"),
            "Invalid pre-parser option, cannot combine { %s } with { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.option1, GqlParams.StringParam.option2},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid pre-parser combination",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_22N09(
            new GqlStatus("22N09"),
            "Invalid pre-parser option, cannot specify multiple conflicting values for { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.option},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "conflicting pre-parser combination",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_22N10(
            new GqlStatus("22N10"),
            "Invalid pre-parser option, specified { %s } is not valid for option { %s }. Valid options are: { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input, GqlParams.StringParam.option, GqlParams.ListParam.optionList,
            },
            Map.of(GqlParams.ListParam.optionList, GqlParams.JoinStyle.ANDED),
            Condition.DATA_EXCEPTION,
            "invalid pre-parser option value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N11(
            new GqlStatus("22N11"),
            "Invalid argument: cannot process { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid argument",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N12(
            new GqlStatus("22N12"),
            "Invalid argument: cannot process { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime format",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N13(
            new GqlStatus("22N13"),
            "Specified time zones must include a date component.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid time zone",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TEMPORAL_SPATIAL})
    STATUS_22N14(
            new GqlStatus("22N14"),
            "Cannot select both { %s } and { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.temporal1, GqlParams.StringParam.temporal2},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid temporal value combination",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N15(
            new GqlStatus("22N15"),
            "Cannot read the specified { %s } component from { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.component, GqlParams.StringParam.temporal},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid temporal component",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE, TOPOLOGY})
    STATUS_22N16(
            new GqlStatus("22N16"),
            "Importing entity values to a graph with a USE clause is not supported. Attempted to import { %s } to { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr, GqlParams.StringParam.graph},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid import value",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TEMPORAL_SPATIAL})
    STATUS_22N18(
            new GqlStatus("22N18"),
            "A { %s } POINT must contain { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.crs, GqlParams.ListParam.mapKeyList,
            },
            Map.of(GqlParams.ListParam.mapKeyList, GqlParams.JoinStyle.ANDED),
            Condition.DATA_EXCEPTION,
            "incomplete spatial value",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N19(
            new GqlStatus("22N19"),
            "A POINT must contain either 'x' and 'y', or 'latitude' and 'longitude'.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid spatial value",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_22N20(
            new GqlStatus("22N20"),
            "Cannot create POINT with { %s }D coordinate reference system (CRS) and { %s } coordinates. Use the equivalent { %s }D coordinate reference system instead.",
            new GqlParams.GqlParam[] {
                GqlParams.NumberParam.dim1, GqlParams.NumberParam.value, GqlParams.NumberParam.dim2
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid spatial value dimensions",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TEMPORAL_SPATIAL})
    STATUS_22N21(
            new GqlStatus("22N21"),
            "Unsupported coordinate reference system (CRS): { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.crs},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unsupported coordinate reference system",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N22(
            new GqlStatus("22N22"),
            "Cannot specify both coordinate reference system (CRS) and spatial reference identifier (SRID).",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid spatial value combination",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N23(
            new GqlStatus("22N23"),
            "Cannot create WGS84 POINT with invalid coordinate: { %s }. The valid range for the latitude coordinate is [-90, 90].",
            new GqlParams.GqlParam[] {GqlParams.StringParam.coordinates},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid latitude value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N24(
            new GqlStatus("22N24"),
            "Cannot construct a { %s } from { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.valueType, GqlParams.StringParam.coordinates},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid coordinate arguments",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N25(
            new GqlStatus("22N25"),
            "Cannot construct a { %s } from { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.valueType, GqlParams.StringParam.temporal},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid temporal arguments",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N26(
            new GqlStatus("22N26"),
            "Unknown rounding mode. Valid values are: CEILING, FLOOR, UP, DOWN, HALF_EVEN, HALF_UP, HALF_DOWN, UNNECESSARY.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unsupported rounding mode",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N27(
            new GqlStatus("22N27"),
            "Invalid input { %s } for { %s }. Expected to be { %s }.{ %s }",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input,
                GqlParams.StringParam.context,
                GqlParams.ListParam.valueTypeList,
                GqlParams.StringParam.hint
            },
            Map.of(GqlParams.ListParam.valueTypeList, GqlParams.JoinStyle.ORED),
            Condition.DATA_EXCEPTION,
            "invalid entity type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_22N28(
            new GqlStatus("22N28"),
            "The result of the operation { %s } has caused an overflow.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.operation},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "overflow error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N29(
            new GqlStatus("22N29"),
            "Unknown coordinate reference system (CRS).",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unknown coordinate reference system",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N30(
            new GqlStatus("22N30"),
            "At least one temporal unit must be specified.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "missing temporal unit",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N31(
            new GqlStatus("22N31"),
            "The { %s } property { %s } in { %s } is invalid. 'MERGE' cannot be used with a graph element property value that is { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.entityType,
                GqlParams.StringParam.propKey,
                GqlParams.StringParam.pat,
                GqlParams.StringParam.value
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid properties in merge pattern",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N32(
            new GqlStatus("22N32"),
            "'ORDER BY' expressions must be deterministic.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "non-deterministic sort expression",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_22N33(
            new GqlStatus("22N33"),
            "Shortest path expressions must contain start and end nodes. Cannot find: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid shortest path expression",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_22N34(
            new GqlStatus("22N34"),
            "Cannot use { %s } function inside an aggregate function.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.funType},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid use of aggregate function",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N35(
            new GqlStatus("22N35"),
            "Cannot parse { %s } as a DATE. Calendar dates need to be specified using the format 'YYYY-MM', while ordinal dates need to be specified using the format 'YYYY-DDD'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid date format",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N36(
            new GqlStatus("22N36"),
            "Cannot parse { %s } as a { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input, GqlParams.StringParam.valueType},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid temporal format",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N37(
            new GqlStatus("22N37"),
            "Cannot coerce { %s } to { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value, GqlParams.StringParam.valueType},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid coercion",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_22N38(
            new GqlStatus("22N38"),
            "Invalid argument to the function { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid function argument",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N39(
            new GqlStatus("22N39"),
            "Value { %s } cannot be stored in properties.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unsupported property value type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TEMPORAL_SPATIAL, VALUE_TYPE})
    STATUS_22N40(
            new GqlStatus("22N40"),
            "Cannot assign { %s } of a { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.component, GqlParams.StringParam.valueType},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "non-assignable temporal component",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_22N41(
            new GqlStatus("22N41"),
            "The 'MERGE' clause did not find a matching node { %s } and cannot create a new node due to conflicts with existing uniqueness constraints.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "merge node uniqueness constraint violation",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_22N42(
            new GqlStatus("22N42"),
            "The 'MERGE' clause did not find a matching relationship { %s } and cannot create a new relationship due to conflicts with existing uniqueness constraints.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "merge relationship uniqueness constraint violation",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N43(
            new GqlStatus("22N43"),
            "Could not load external resource from { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.url},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unable to load external resource",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N44(
            new GqlStatus("22N44"),
            "Parallel runtime has been disabled, enable it or upgrade to a bigger Aura instance.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "parallel runtime disabled",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N46(
            new GqlStatus("22N46"),
            "Parallel runtime does not support updating queries or a change in the state of transactions. Use another runtime.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unsupported use of parallel runtime",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N47(
            new GqlStatus("22N47"),
            "No workers are configured for the parallel runtime. Set 'server.cypher.parallel.worker_limit' to a larger value.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid parallel runtime configuration",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_22N48(
            new GqlStatus("22N48"),
            "Cannot use the specified runtime { %s } due to { %s } not being supported. Use another runtime.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.runtime, GqlParams.StringParam.cause},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unable to use specified runtime",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N49(
            new GqlStatus("22N49"),
            "Cannot read a CSV field larger than the set buffer size. Ensure the field does not have an unterminated quote, or increase the buffer size via 'dbms.import.csv.buffer_size'.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "CSV buffer size overflow",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_22N51(
            new GqlStatus("22N51"),
            "A graph reference with the name { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "graph reference not found",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N52(
            new GqlStatus("22N52"),
            "'PROFILE' and 'EXPLAIN' cannot be combined.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid combination of PROFILE and EXPLAIN",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N53(
            new GqlStatus("22N53"),
            "Cannot 'PROFILE' query before results are materialized.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid use of PROFILE",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_22N54(
            new GqlStatus("22N54"),
            "Multiple conflicting entries specified for { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.mapKey},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid map",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA, FIXED_TEXT})
    STATUS_22N55(
            new GqlStatus("22N55"),
            "Map requires key { %s } but was missing from field { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.mapKey, GqlParams.StringParam.field},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "required key missing from map",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_22N56(
            new GqlStatus("22N56"),
            "Protocol message length limit exceeded (limit: { %s }).",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.boltMsgLenLimit},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "protocol message length limit overflow",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_22N57(
            new GqlStatus("22N57"),
            "Protocol type is invalid. Invalid number of struct components (received { %s } but expected { %s }).",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.count1, GqlParams.NumberParam.count2},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid protocol type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N58(
            new GqlStatus("22N58"),
            "Cannot read the specified { %s } component from { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.component, GqlParams.NumberParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid spatial component",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N59(
            new GqlStatus("22N59"),
            "The { %s } token with id { %s } does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.tokenType, GqlParams.NumberParam.tokenId},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "token does not exist",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_22N60(
            new GqlStatus("22N60"),
            "Encountered illegal { %s } element. Reason: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.item, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "illegal element",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N62(
            new GqlStatus("22N62"),
            "The relationship type { %s } does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.relType},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "relationship type does not exist",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N63(
            new GqlStatus("22N63"),
            "The property key { %s } does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.propKey},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "property key does not exist",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N64(
            new GqlStatus("22N64"),
            "The constraint { %s } does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constrDescrOrName},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "constraint does not exist",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N65(
            new GqlStatus("22N65"),
            "An equivalent constraint already exists: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constrDescrOrName},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "equivalent constraint already exists",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N66(
            new GqlStatus("22N66"),
            "A conflicting constraint already exists: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constrDescrOrName},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "conflicting constraint already exists",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N67(
            new GqlStatus("22N67"),
            "A constraint with the same name already exists: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constr},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "duplicated constraint name",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N68(
            new GqlStatus("22N68"),
            "Dependent constraints cannot be managed individually and must be managed together with its graph type.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "dependent constraint managed individually",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N69(
            new GqlStatus("22N69"),
            "The index { %s } does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idxDescrOrName},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "index does not exist",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N70(
            new GqlStatus("22N70"),
            "An equivalent index already exists: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idxDescrOrName},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "equivalent index already exists",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N71(
            new GqlStatus("22N71"),
            "An index with the same name already exists: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idx},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "duplicated index name",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N73(
            new GqlStatus("22N73"),
            "Constraint conflicts with already existing index { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idxDescrOrName},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "constraint conflicts with existing index",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N74(
            new GqlStatus("22N74"),
            "Index conflicts with already existing index owned by constraint { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constr},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "index conflicts with existing constraint",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N75(
            new GqlStatus("22N75"),
            "The constraint specified by { %s } includes a label, relationship type, or property key with name { %s } more than once.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constrDescrOrName, GqlParams.StringParam.token},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "constraint contains duplicated tokens",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N76(
            new GqlStatus("22N76"),
            "The index specified by { %s } includes a label, relationship type, or property key with name { %s } more than once.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idxDescrOrName, GqlParams.StringParam.token},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "index contains duplicated tokens",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22N77(
            new GqlStatus("22N77"),
            "{ %s } ({ %s }) with { %s } { %s } must have the following properties: { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.entityType,
                GqlParams.NumberParam.entityId,
                GqlParams.StringParam.tokenType,
                GqlParams.StringParam.token,
                GqlParams.ListParam.propKeyList
            },
            Map.of(GqlParams.ListParam.propKeyList, GqlParams.JoinStyle.COMMAD),
            Condition.DATA_EXCEPTION,
            "property presence verification failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA, VALUE_TYPE})
    STATUS_22N78(
            new GqlStatus("22N78"),
            "{ %s } ({ %s }) with { %s } { %s } must have the property { %s } with value type { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.entityType,
                GqlParams.NumberParam.entityId,
                GqlParams.StringParam.tokenType,
                GqlParams.StringParam.token,
                GqlParams.StringParam.propKey,
                GqlParams.StringParam.valueType
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "property type verification failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N79(
            new GqlStatus("22N79"),
            "Property uniqueness constraint violated: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.reasonList},
            Map.of(GqlParams.ListParam.reasonList, GqlParams.JoinStyle.COMMAD),
            Condition.DATA_EXCEPTION,
            "property uniqueness constraint violated",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N80(
            new GqlStatus("22N80"),
            "Index entry conflict: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "index entry conflict",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT, SCHEMA})
    STATUS_22N81(
            new GqlStatus("22N81"),
            "Invalid input: { %s } is not supported in { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.exprType, GqlParams.StringParam.context},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "expression type unsupported here",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N82(
            new GqlStatus("22N82"),
            "Input { %s } contains invalid characters for { %s }. Special characters may require that the input is quoted using backticks.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input, GqlParams.StringParam.context},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "input contains invalid characters",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_22N83(
            new GqlStatus("22N83"),
            "Expected name to contain at most { %s } components separated by '.'.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.upper},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "input consists of too many components",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_22N84(
            new GqlStatus("22N84"),
            "Expected the string to be no more than { %s } characters long.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.upper},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "string too long",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_22N85(
            new GqlStatus("22N85"),
            "Expected the string to be at least { %s } characters long.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.lower},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "string too short",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N86(
            new GqlStatus("22N86"),
            "Expected a nonzero number.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "numeric range 0 disallowed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N87(
            new GqlStatus("22N87"),
            "Expected a number that is zero or greater.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "numeric range 0 or greater allowed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N88(
            new GqlStatus("22N88"),
            "{ %s } is not a valid CIDR IP.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "not a valid CIDR IP",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N89(
            new GqlStatus("22N89"),
            "Expected the new password to be different from the old password.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "new password cannot be the same as the old password",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {VALUE_TYPE})
    STATUS_22N90(
            new GqlStatus("22N90"),
            "{ %s } is not supported in property type constraints.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.valueType},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "property type unsupported in constraint",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_22N91(
            new GqlStatus("22N91"),
            "Failed to alter the specified database alias { %s }. Altering remote alias to a local alias or vice versa is not supported. Drop and recreate the alias instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.alias},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "cannot convert alias local to remote or remote to local",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N92(
            new GqlStatus("22N92"),
            "This query requires a RETURN clause.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "missing RETURN",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N93(
            new GqlStatus("22N93"),
            "A required YIELD clause is missing.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "missing YIELD",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N94(
            new GqlStatus("22N94"),
            "'YIELD *' is not supported in this context. Explicitly specify which columns to yield.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid YIELD *",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N95(
            new GqlStatus("22N95"),
            "Invalid JSON input. Please check the format.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "parsing JSON exception",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22N96(
            new GqlStatus("22N96"),
            "Unable to map the JSON input. Please verify the structure.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "mapping JSON exception",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_22N97(
            new GqlStatus("22N97"),
            "Unexpected struct tag: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unexpected struct tag",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_22N98(
            new GqlStatus("22N98"),
            "Unable to deserialize request. Expected first field to be { %s }, but was '{ %s }'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.field, GqlParams.StringParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "wrong first field during deserialization",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_22N99(
            new GqlStatus("22N99"),
            "Unable to deserialize request. Expected { %s }, found { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.token, GqlParams.StringParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "wrong token during deserialization",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22NA0(
            new GqlStatus("22NA0"),
            "Failed to administer property rule.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid property-based access control rule",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA, CYPHER_CONSTRUCT})
    STATUS_22NA1(
            new GqlStatus("22NA1"),
            "The property { %s } must appear on the left hand side of the { %s } operator.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.propKey, GqlParams.StringParam.operation},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid property-based access control rule involving non-commutative expressions",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA2(
            new GqlStatus("22NA2"),
            "The expression: { %s } is not supported. Property rules can only contain one property.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid property-based access control rule involving multiple properties",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22NA3(
            new GqlStatus("22NA3"),
            "'NaN' is not supported for property-based access control.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid property-based access control rule involving NaN",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA4(
            new GqlStatus("22NA4"),
            "The property value access rule pattern { %s } always evaluates to 'NULL'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pred},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid property-based access control rule involving comparison with NULL",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA5(
            new GqlStatus("22NA5"),
            "The property value access rule pattern { %s } always evaluates to 'NULL'. Use 'IS NULL' instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pred},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid property-based access control rule involving IS NULL",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA6(
            new GqlStatus("22NA6"),
            "The property value access rule pattern { %s } always evaluates to 'NULL'. Use 'IS NOT NULL' instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pred},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid property-based access control rule involving IS NOT NULL",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA7(
            new GqlStatus("22NA7"),
            "The expression: { %s } is not supported. Only single, literal-based predicate expressions are allowed for property-based access control.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid property-based access control rule involving nontrivial predicates",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA8(
            new GqlStatus("22NA8"),
            "Underlying error: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cause},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "parsing JSON failure",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE, FIXED_TEXT})
    STATUS_22NA9(
            new GqlStatus("22NA9"),
            "Invalid input. Unexpected key { %s }, expected keys are { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.mapKey, GqlParams.ListParam.mapKeyList},
            Map.of(GqlParams.ListParam.mapKeyList, GqlParams.JoinStyle.ORED),
            Condition.DATA_EXCEPTION,
            "unexpected map entry",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NAA(
            new GqlStatus("22NAA"),
            "The expression { %s } is not supported. Lists containing { %s } values are not supported for property-based access control.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr, GqlParams.StringParam.exprType},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid list for property-based access control rule",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NAB(
            new GqlStatus("22NAB"),
            "The expression { %s } is not supported. All elements in a list must be literals of the same type for property-based access control.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "mixed type list for property-based access control rule",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NAC(
            new GqlStatus("22NAC"),
            "Characters after an ending quote in a CSV field are not supported. See { %s } at position { %s }. This is read as { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input, GqlParams.NumberParam.pos, GqlParams.StringParam.variable
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "characters after quote in CSV field",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NAD(
            new GqlStatus("22NAD"),
            "Missing end quote at position { %s } in { %s }.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.pos, GqlParams.StringParam.input},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "missing end quote in CSV field",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NAE(
            new GqlStatus("22NAE"),
            "Multi-line fields are illegal in this context. Verify that there is not a missing end quote in { %s } at position { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input, GqlParams.NumberParam.pos},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "multi-line field in illegal CSV context",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB0(
            new GqlStatus("22NB0"),
            "The property value access rule pattern { %s } always evaluates to 'NULL'. Use `WHERE` syntax in combination with `IS NULL` instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pred},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid property-based access control rule involving WHERE and IS NULL",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {VALUE_TYPE})
    STATUS_22NB1(
            new GqlStatus("22NB1"),
            "Type mismatch: expected to be { %s } but was { %s }.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.valueTypeList, GqlParams.StringParam.input},
            Map.of(GqlParams.ListParam.valueTypeList, GqlParams.JoinStyle.ORED),
            Condition.DATA_EXCEPTION,
            "type mismatch",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NB2(
            new GqlStatus("22NB2"),
            "Graph type { %s } constraint { %s } is incompatible with graph type { %s } constraint { %s } because they have different graph type dependence.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.graphTypeDependence1,
                GqlParams.StringParam.constrDescrOrName1,
                GqlParams.StringParam.graphTypeDependence2,
                GqlParams.StringParam.constrDescrOrName2
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "incompatible graph type dependence",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NB3(
            new GqlStatus("22NB3"),
            "{ %s } ({ %s }) with { %s } { %s } must have the { %s } { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.entityType,
                GqlParams.NumberParam.entityId,
                GqlParams.StringParam.tokenType1,
                GqlParams.StringParam.token1,
                GqlParams.StringParam.tokenType2,
                GqlParams.StringParam.token2
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "token presence verification failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NB4(
            new GqlStatus("22NB4"),
            "Relationship ({ %s }) with type { %s } requires its { %s } node ({ %s }) to have the label { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.NumberParam.entityId1,
                GqlParams.StringParam.relType,
                GqlParams.StringParam.endpointType,
                GqlParams.NumberParam.entityId2,
                GqlParams.StringParam.label
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "endpoint label presence verification failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB5(
            new GqlStatus("22NB5"),
            "Unknown time zone identifier { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unsupported time zone identifier",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_22NB6(
            // See also 22N06
            new GqlStatus("22NB6"),
            "Invalid input. { %s } is not allowed to be an empty string.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.item},
            Map.of(),
            Condition.DATA_EXCEPTION,
            "input empty",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NB7(
            new GqlStatus("22NB7"),
            "It is not supported to create element ids on composite databases. Create the element id for { %s } { %s } on the constituent instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.entityType, GqlParams.StringParam.entityId},
            Map.of(),
            Condition.DATA_EXCEPTION,
            "element id unsupported on composite database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {VALUE_TYPE})
    STATUS_22NB8(
            new GqlStatus("22NB8"),
            "{ %s } is not a recognized Neo4j type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid Neo4j type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {VALUE_TYPE})
    STATUS_22NB9(
            new GqlStatus("22NB9"),
            "Lists cannot have { %s } as an inner type in this context.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.typeDescription},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid inner list type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22NBA(
            new GqlStatus("22NBA"),
            "Property type constraints for vectors need to define both coordinate type and dimension.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "omitting mandatory field for property type constraints for vectors",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NBC(
            new GqlStatus("22NBC"),
            "Index belongs to constraint { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constrDescrOrName},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "index belongs to constraint",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_22NBD(
            new GqlStatus("22NBD"),
            "Unsupported struct tag: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "unsupported struct tag",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_22NBE(
            new GqlStatus("22NBE"),
            "Invalid vector dimensions. The number of vector dimensions must be between { %s } and { %s }, but is { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.NumberParam.count1, GqlParams.NumberParam.count2, GqlParams.NumberParam.count3
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid vector dimensions",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NBF(
            new GqlStatus("22NBF"),
            "Property value of type { %s } is too big (more than { %s } bytes): { %s }",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.typeDescription, GqlParams.NumberParam.bytes, GqlParams.StringParam.value
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "property value too big",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_22NBG(
            new GqlStatus("22NBG"),
            "Invalid vector coordinates. The vector coordinates must be finite.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid vector coordinates",
            ErrorClassification.CLIENT_ERROR),
    // Graph Type Errors
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NC1(
            new GqlStatus("22NC1"),
            "The graph type element includes a property key with name { %s } more than once.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.propKey},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "graph type element contains duplicated tokens",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NC2(
            new GqlStatus("22NC2"),
            "The node element type { %s } must contain one or more implied labels, or at least one property type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.label},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "node element type has no effect",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NC3(
            new GqlStatus("22NC3"),
            "The relationship element type { %s } must define a source, destination, or at least one property type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.relType},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "relationship element type has no effect",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NC4(
            new GqlStatus("22NC4"),
            "The label(s) { %s } are defined as both identifying and implied.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.labelList},
            Map.of(GqlParams.ListParam.labelList, GqlParams.JoinStyle.COMMAD),
            Condition.DATA_EXCEPTION,
            "a label cannot be both identifying and implied",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NC5(
            new GqlStatus("22NC5"),
            "The { %s } element type referenced by { %s } does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.entityType, GqlParams.StringParam.graphTypeReference},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "graph type element not found",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NC6(
            new GqlStatus("22NC6"),
            "The independent constraint { %s } is using the same label { %s } as a node element type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constrDescrOrName, GqlParams.StringParam.label},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "independent constraint and node element type have the same label",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NC7(
            new GqlStatus("22NC7"),
            "The independent constraint { %s } is using the same relationship type { %s } as a relationship element type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constrDescrOrName, GqlParams.StringParam.relType},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "independent constraint and relationship element type have the same relationship type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NC8(
            new GqlStatus("22NC8"),
            "The graph type includes a label, a relationship type, or an alias with the name { %s } more than once.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.token},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "graph type contains duplicated tokens",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NC9(
            new GqlStatus("22NC9"),
            "A { %s } element type property { %s } constraint cannot be specified inline of a { %s } element type.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.entityType, GqlParams.StringParam.context, GqlParams.StringParam.entityType
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "invalid element type constraints in graph type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NCA(
            new GqlStatus("22NCA"),
            "A node element type identified by the label { %s } already exists in the graph type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.label},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "node element type already exists",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NCB(
            new GqlStatus("22NCB"),
            "A relationship element type identified by the relationship type { %s } already exists in the graph type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.relType},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "relationship element type already exists",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NCC(
            new GqlStatus("22NCC"),
            "The node element type { %s } identified by the label { %s } is different to the one specified in the query { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.graphTypeElement1,
                GqlParams.StringParam.label,
                GqlParams.StringParam.graphTypeElement2
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "node element type specified incorrectly",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NCD(
            new GqlStatus("22NCD"),
            "The relationship element type { %s } identified by the relationship type { %s } is different to the one specified in the query { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.graphTypeElement1,
                GqlParams.StringParam.relType,
                GqlParams.StringParam.graphTypeElement2
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "relationship element type specified incorrectly",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NCE(
            new GqlStatus("22NCE"),
            "The node element type identified by the label { %s } is referenced in the graph type element { %s } and cannot be dropped.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.label, GqlParams.StringParam.graphTypeReference},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "node element type in use",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_22NCF(
            new GqlStatus("22NCF"),
            "Graph type constraint definitions are not supported in the `ALTER GRAPH TYPE { %s }` operation.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.graphTypeOperation},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "graph type constraint not supported",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22NCG(
            new GqlStatus("22NCG"),
            "Expected the index { %s } to be a { %s } index but was a { %s } index.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.idx, GqlParams.StringParam.idxType1, GqlParams.StringParam.idxType2
            },
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "wrong index type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22ND1(
            new GqlStatus("22ND1"),
            "Invalid input: { %s } is not allowed for roles that are granted to an AUTH RULE.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.query},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "operation not allowed for roles that are granted to an AUTH RULE",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22ND2(
            new GqlStatus("22ND2"),
            "Invalid input: { %s } is not allowed for roles with DENY privileges.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.query},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "operation not allowed for roles with DENY privileges",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_22ND3(
            new GqlStatus("22ND3"),
            "The property { %s } is not an additional property for vector search with filters on the vector index { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.propKey, GqlParams.StringParam.idx},
            emptyMap(),
            Condition.DATA_EXCEPTION,
            "wrong property for vector search with filters",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25000(
            new GqlStatus("25000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25G01(
            new GqlStatus("25G01"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "active GQL-transaction",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25G02(
            new GqlStatus("25G02"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "catalog and data statement mixing not supported",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25G03(
            new GqlStatus("25G03"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "read-only GQL-transaction",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25G04(
            new GqlStatus("25G04"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "accessing multiple graphs not supported",
            ErrorClassification.UNKNOWN),
    STATUS_25N01(
            new GqlStatus("25N01"),
            "Failed to execute the query { %s } due to conflicting statement types (read query, write query, schema modification, or administration command). To execute queries in the same transaction, they must be either of the same type, or be a combination of schema modifications and read commands.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.query},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "invalid combination of statement types",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25N02(
            new GqlStatus("25N02"),
            "Unable to complete transaction. See debug log for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "unable to complete transaction",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25N03(
            new GqlStatus("25N03"),
            "Transaction is being used concurrently by another request.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "concurrent access violation",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_25N04(
            new GqlStatus("25N04"),
            "Transaction { %s } does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.transactionId},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "specified transaction does not exist",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25N05(
            new GqlStatus("25N05"),
            "Transaction has been closed.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "transaction closed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25N06(
            new GqlStatus("25N06"),
            "Failed to start transaction. See debug log for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "transaction start failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25N07(
            new GqlStatus("25N07"),
            "Failed to start constituent transaction. See debug log for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "constituent transaction start failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25N08(
            new GqlStatus("25N08"),
            "The lease for the transaction is no longer valid.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "invalid transaction lease",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25N09(
            new GqlStatus("25N09"),
            "The transaction failed due to an internal error.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "internal transaction failure",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25N11(
            new GqlStatus("25N11"),
            "There was a conflict detected between the transaction state and applied updates. Please retry the transaction.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "conflicting transaction state",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_25N12(
            new GqlStatus("25N12"),
            "Index { %s } was dropped in this transaction and cannot be used.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idx},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "index was dropped",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_25N13(
            new GqlStatus("25N13"),
            "A { %s } was accessed after being deleted in this transaction. Verify the transaction statements.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.entityType},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "cannot access entity after removal",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_25N14(
            new GqlStatus("25N14"),
            "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Reason: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "transaction termination client error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_25N15(
            new GqlStatus("25N15"),
            "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Reason: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "transaction termination database error",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT, SCHEMA, NON_SENSITIVE_NUMBER})
    STATUS_25N16(
            new GqlStatus("25N16"),
            "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Reason: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "transaction termination transient error",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_25N17(
            new GqlStatus("25N17"),
            "The attempted operation requires an implicit transaction.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_STATE,
            "implicit transaction required",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_2D000(
            new GqlStatus("2D000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_TERMINATION,
            "",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_2DN01(
            new GqlStatus("2DN01"),
            "Failed to commit transaction.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_TERMINATION,
            "commit failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_2DN02(
            new GqlStatus("2DN02"),
            "Failed to commit constituent transaction. See debug log for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_TERMINATION,
            "constituent commit failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_2DN03(
            new GqlStatus("2DN03"),
            "Failed to terminate transaction. See debug log for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_TERMINATION,
            "transaction termination failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_2DN04(
            new GqlStatus("2DN04"),
            "Failed to terminate constituent transaction. See debug log for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_TERMINATION,
            "constituent transaction termination failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_2DN05(
            new GqlStatus("2DN05"),
            "There was an error on applying the transaction. See logs for more information.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_TERMINATION,
            "failed to apply transaction",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_2DN06(
            new GqlStatus("2DN06"),
            "There was an error on appending the transaction. See logs for more information.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_TERMINATION,
            "failed to append transaction",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_2DN07(
            new GqlStatus("2DN07"),
            "Unable to commit transaction because it still have non-closed inner transactions.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.INVALID_TRANSACTION_TERMINATION,
            "inner transactions still open",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_40000(
            new GqlStatus("40000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.TRANSACTION_ROLLBACK,
            "",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_40003(
            new GqlStatus("40003"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.TRANSACTION_ROLLBACK,
            "statement completion unknown",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_40N01(
            new GqlStatus("40N01"),
            "Failed to rollback transaction. See debug log for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.TRANSACTION_ROLLBACK,
            "rollback failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_40N02(
            new GqlStatus("40N02"),
            "Failed to rollback constituent transaction. See debug log for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.TRANSACTION_ROLLBACK,
            "constituent rollback failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42000(
            new GqlStatus("42000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42001(
            new GqlStatus("42001"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid syntax",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42004(
            new GqlStatus("42004"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "use of visually confusable identifiers",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42006(
            new GqlStatus("42006"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge labels below supported minimum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42007(
            new GqlStatus("42007"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge labels exceeds supported maximum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42008(
            new GqlStatus("42008"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge properties exceeds supported maximum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42009(
            new GqlStatus("42009"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node labels below supported minimum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42010(
            new GqlStatus("42010"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node labels exceeds supported maximum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42011(
            new GqlStatus("42011"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node properties exceeds supported maximum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42012(
            new GqlStatus("42012"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node type key labels below supported minimum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42013(
            new GqlStatus("42013"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node type key labels exceeds supported maximum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42014(
            new GqlStatus("42014"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge type key labels below supported minimum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42015(
            new GqlStatus("42015"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge type key labels exceeds supported maximum",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I00(
            new GqlStatus("42I00"),
            "'CASE' expressions must have the same number of 'WHEN' and 'THEN' operands.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid case expression",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42I01(
            new GqlStatus("42I01"),
            "Invalid use of { %s } inside 'FOREACH'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid FOREACH",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I02(
            new GqlStatus("42I02"),
            "Failed to parse comment. A comment starting with '/*' must also have a closing '*/'.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid comment",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I03(
            new GqlStatus("42I03"),
            "A Cypher query has to contain at least one clause.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "empty request",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I04(
            new GqlStatus("42I04"),
            "{ %s } cannot be used in a { %s } clause.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr, GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid expression",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I05(
            new GqlStatus("42I05"),
            "The FIELDTERMINATOR specified for LOAD CSV can only be one character wide.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid FIELDTERMINATOR",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I06(
            new GqlStatus("42I06"),
            "Invalid input { %s }, expected: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input, GqlParams.ListParam.valueList},
            Map.of(GqlParams.ListParam.valueList, GqlParams.JoinStyle.ORED),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid input",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I07(
            new GqlStatus("42I07"),
            "The given { %s } literal { %s } is invalid.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.valueType, GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid integer literal",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42I08(
            new GqlStatus("42I08"),
            "The lower bound of the variable length relationship used in the { %s } function must be 0 or 1.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid lower bound",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I09(
            new GqlStatus("42I09"),
            "Expected MAP to contain the same number of keys and values, but got keys { %s } and values { %s }.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.mapKeyList, GqlParams.ListParam.valueList},
            Map.of(
                    GqlParams.ListParam.mapKeyList,
                    GqlParams.JoinStyle.ANDED,
                    GqlParams.ListParam.valueList,
                    GqlParams.JoinStyle.COMMAD),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid map",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_42I10(
            new GqlStatus("42I10"),
            "Mixing label expression symbols (`|`, `&`, `!`, and `%`) with colon (`:`) between labels is not allowed. This expression could be expressed as { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.syntax},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid label expression",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_42I11(
            new GqlStatus("42I11"),
            "A { %s } name cannot be empty or contain any null-bytes: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.tokenType, GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid name",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I12(
            new GqlStatus("42I12"),
            "Quantified path patterns cannot be nested.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid nesting of quantified path patterns",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS, NON_SENSITIVE_NUMBER})
    STATUS_42I13(
            new GqlStatus("42I13"),
            "The procedure or function call does not provide the required number of arguments; expected { %s } but got { %s }. The procedure or function { %s } has the signature: { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.NumberParam.count1,
                GqlParams.NumberParam.count2,
                GqlParams.StringParam.procFun,
                GqlParams.StringParam.sig
            },
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid number of procedure or function arguments",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42I14(
            new GqlStatus("42I14"),
            "Exactly one relationship type must be specified for { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid number of relationship types",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_42I15(
            new GqlStatus("42I15"),
            "Expected exactly one statement per query but got: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.count},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid number of statements",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TEMPORAL_SPATIAL})
    STATUS_42I16(
            new GqlStatus("42I16"),
            "Map with keys { %s } is not a valid POINT. Use either Cartesian or geographic coordinates.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.mapKeyList},
            Map.of(GqlParams.ListParam.mapKeyList, GqlParams.JoinStyle.ANDED),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid point",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I17(
            new GqlStatus("42I17"),
            "A quantifier must not have a lower bound greater than the upper bound.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid quantifier",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42I18(
            new GqlStatus("42I18"),
            "The aggregation column contains implicit grouping expressions referenced by the variables { %s }. Implicit grouping expressions are variables not explicitly declared as grouping keys.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.variableList},
            Map.of(GqlParams.ListParam.variableList, GqlParams.JoinStyle.COMMAD),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid reference to implicitly grouped expressions",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I19(
            new GqlStatus("42I19"),
            "Failed to parse string literal. The query must contain an even number of non-escaped quotes.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid string literal",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42I20(
            new GqlStatus("42I20"),
            "Label expressions and relationship type expressions cannot contain { %s }. To express a label disjunction use { %s } instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input, GqlParams.StringParam.labelExpr},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid symbol in expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I21(
            new GqlStatus("42I21"),
            "Not allowed to reference { %s } from within a parenthesized/quantified path pattern like { %s } in the same graph pattern.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.variableList, GqlParams.StringParam.pat},
            Map.of(GqlParams.ListParam.variableList, GqlParams.JoinStyle.COMMAD),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid reference to variable out of scope",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I22(
            new GqlStatus("42I22"),
            "The right hand side of a UNION clause must be a single query.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of UNION",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42I23(
            new GqlStatus("42I23"),
            "The { %s } function cannot contain a quantified path pattern.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid quantified path pattern in shortest path",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I24(
            new GqlStatus("42I24"),
            "Aggregate expression { %s } is not allowed in this context.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of aggregate function",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I25(
            new GqlStatus("42I25"),
            "'CALL { ... } IN TRANSACTIONS' is not supported after a write clause.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of CALL IN TRANSACTIONS",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I26(
            new GqlStatus("42I26"),
            "'DELETE ...' does not support removing labels from a node. Use 'REMOVE ...' instead.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid DELETE",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42I27(
            new GqlStatus("42I27"),
            "`DISTINCT` cannot be used with the { %s } function.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of DISTINCT with non-aggregate function",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I28(
            new GqlStatus("42I28"),
            "Importing WITH can consist only of direct references to outside variables. { %s } is not allowed.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of importing WITH",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I29(
            new GqlStatus("42I29"),
            "The IS keyword cannot be used together with multiple labels in { %s }. Rewrite the expression as { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input, GqlParams.StringParam.replacement},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of IS",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42I30(
            new GqlStatus("42I30"),
            "Label expressions cannot be used in { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of label expressions",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42I32(
            new GqlStatus("42I32"),
            "Node and relationship pattern predicates cannot be used in { %s }. They can only be used in a `MATCH` clause or inside a pattern comprehension.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.context},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of node and relationship pattern predicate",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I33(
            new GqlStatus("42I33"),
            "Closed Dynamic Union types cannot be appended with 'NOT NULL', specify 'NOT NULL' on inner types instead.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of NOT NULL",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I34(
            new GqlStatus("42I34"),
            "A pattern expression can only be used to test the existence of a pattern. Use a pattern comprehension instead.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of pattern expression",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I35(
            new GqlStatus("42I35"),
            "Relationship type expressions can only be used in 'MATCH ...'.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of relationship type expression",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I36(
            new GqlStatus("42I36"),
            "'REPORT STATUS' can only be used when specifying 'ON ERROR CONTINUE' or 'ON ERROR BREAK'.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of REPORT STATUS",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I37(
            new GqlStatus("42I37"),
            "'RETURN *' is not allowed when there are no variables in scope.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of RETURN *",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42I38(
            new GqlStatus("42I38"),
            "'{ %s }...' can only be used at the end of a query or subquery.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid position of clause",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42I39(
            new GqlStatus("42I39"),
            "Mixing the { %s } function with path selectors, explicit match modes or "
                    + "explicit path modes is not allowed.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of shortest path function",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I40(
            new GqlStatus("42I40"),
            "UNION and UNION ALL cannot be combined.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of UNION and UNION ALL",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_42I41(
            new GqlStatus("42I41"),
            "Variable length relationships cannot be used in { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of variable length relationship",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I42(
            new GqlStatus("42I42"),
            "Cannot use YIELD on a call to a void procedure.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of YIELD",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I43(
            new GqlStatus("42I43"),
            "'YIELD *' can only be used with a standalone procedure call.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of YIELD *",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I44(
            new GqlStatus("42I44"),
            "Cannot use a join hint for a single node pattern.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid joint hint",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_42I45(
            new GqlStatus("42I45"),
            "Multiple path patterns cannot be used in the same clause in combination with a selective path selector.{ %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.action},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of multiple path patterns",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I46(
            new GqlStatus("42I46"),
            "Node pattern pairs are only supported for quantified path patterns.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of a node pattern pair",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I47(
            new GqlStatus("42I47"),
            "Parser Error: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "parser error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I48(
            new GqlStatus("42I48"),
            "Subqueries are not allowed in a MERGE clause.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of a subquery in MERGE",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I49(
            new GqlStatus("42I49"),
            "Unknown inequality operator '!='. The operator for inequality in Cypher is '<>'.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid inequality operator",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA, NON_SENSITIVE_NUMBER})
    STATUS_42I50(
            new GqlStatus("42I50"),
            "Invalid input { %s }... A { %s } name cannot be longer than { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input, GqlParams.StringParam.tokenType, GqlParams.NumberParam.value
            },
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "token name too long",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42I51(
            new GqlStatus("42I51"),
            "The procedure or function { %s } must have the signature: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procFun, GqlParams.StringParam.sig},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid call signature",
            ErrorClassification.CLIENT_ERROR),
    // Used for syntax errors for features removed in Neo4j 5.0,
    // which should have a helpful error message in Cypher 5 but the more general 42I06 in Cypher 25.
    // The full old message will be inserted as the msg parameter.
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT, TOPOLOGY, FIXED_TEXT})
    STATUS_42I52(
            new GqlStatus("42I52"),
            "{ %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no longer valid syntax",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {VALUE_TYPE})
    STATUS_42I53(
            new GqlStatus("42I53"),
            "Unknown coordinate type: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported coordinate type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_42I54(
            new GqlStatus("42I54"),
            "`{ %s }` not allowed in `INSERT`. Use `CREATE` or { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cause, GqlParams.StringParam.replacement},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of `INSERT`",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA, CYPHER_CONSTRUCT})
    STATUS_42I55(
            new GqlStatus("42I55"),
            "Dynamic { %s } using `$any()` are not allowed in `{ %s }`.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.entityType, GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of dynamic label or type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42I56(
            new GqlStatus("42I56"),
            "Only directed relationships are supported in `{ %s }`.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid relationship direction",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42I57(
            new GqlStatus("42I57"),
            "{ %s } cannot contain a query ending with { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.exprType, GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid query ending",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I58(
            new GqlStatus("42I58"),
            "Entity, { %s }, cannot be created and referenced in the same clause.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid entity reference",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42I59(
            new GqlStatus("42I59"),
            "Dynamic label and types are only allowed in { %s } clauses.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.clauseList},
            Map.of(GqlParams.ListParam.clauseList, GqlParams.JoinStyle.ANDED),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "dynamic entity type not allowed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I60(
            new GqlStatus("42I60"),
            "Each part of the glob (a block of text up until a dot) must either be fully escaped or not escaped at all.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid glob escaping",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I61(
            new GqlStatus("42I61"),
            "Missing function name for the LOOKUP INDEX.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing LOOKUP INDEX function name",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I62(
            new GqlStatus("42I62"),
            "Unknown distance metric: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported distance metric",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42I63(
            new GqlStatus("42I63"),
            "`ORDER BY`, `{ %s }` and `LIMIT` can only be used in this order in `RETURN`.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "wrong subclause order",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I64(
            new GqlStatus("42I64"),
            "{ %s } currently not supported after `NEXT` { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.context, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported operation",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {VALUE_TYPE})
    STATUS_42I65(
            new GqlStatus("42I65"),
            "An invalid character is used in the pattern. Verify that all characters are supported by `{ %s }`.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.valueType},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid pattern character",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I66(
            new GqlStatus("42I66"),
            "Pattern parsing failed. Make sure that an even number of escapes are used in the pattern.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "pattern parsing failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_42I67(
            new GqlStatus("42I67"),
            "The query is parsable in `CYPHER { %s }`, but it is run in `CYPHER { %s }`. Consider changing the database default Cypher version using `ALTER DATABASE SET DEFAULT LANGUAGE` or prefix the query with `CYPHER { %s }`.",
            new GqlParams.GqlParam[] {
                GqlParams.NumberParam.version2, GqlParams.NumberParam.version1, GqlParams.NumberParam.version2
            },
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported language feature",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I68(
            new GqlStatus("42I68"),
            "Pattern, `{ %s }`, does not match input, `{ %s }`. Verify that the pattern is valid for constructing `{ %s }`.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input1, GqlParams.StringParam.input2, GqlParams.StringParam.valueType
            },
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "mismatched pattern",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42I69(
            new GqlStatus("42I69"),
            "{ %s } must reference a variable from the same MATCH statement.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid search variable reference",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I70(
            new GqlStatus("42I70"),
            "In order to have a search clause, a MATCH statement can only have one bound variable.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "search clause with multiple bound variables",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I71(
            new GqlStatus("42I71"),
            "In order to have a search clause, a MATCH statement can only have predicates on the bound node or relationship.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "search clause with invalid predicates",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42I72(
            new GqlStatus("42I72"),
            "In order to have a search clause, a MATCH statement can only have a single node or relationship pattern and no selectors.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "search clause with too complex pattern",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I73(
            new GqlStatus("42I73"),
            "The vector search filter predicate { %s } must consist of one or more property predicates joined by AND, and the combined property predicates for each property must specify either an exact value (e.g. x.prop = 1), a half-bounded range (e.g. x.prop >= 1), or a bounded range (e.g. x.prop > 1 AND x.prop < 100). Note that this is not an exhaustive list of valid predicates, see documentation for all rules.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid predicate for vector search with filters",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42I74(
            new GqlStatus("42I74"),
            "The variable { %s } in a vector search filter property predicate must be the same as the search clause binding variable { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable1, GqlParams.StringParam.variable2},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "wrong variable for vector search with filters",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE, SCHEMA})
    STATUS_42I75(
            new GqlStatus("42I75"),
            "The expression { %s } in the search clause may not depend on the search clause binding variable { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr, GqlParams.StringParam.variable},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "self-referencing in vector search",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA, NON_SENSITIVE_NUMBER})
    STATUS_42I76(
            new GqlStatus("42I76"),
            "The provided index or constraint { %s } { %s } ({ %s } bytes) exceeded limit of { %s } bytes.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.item,
                GqlParams.StringParam.input,
                GqlParams.NumberParam.bytes1,
                GqlParams.NumberParam.bytes2
            },
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "index or constraint value too long",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42I77(
            new GqlStatus("42I77"),
            "Local callable { %s } is already defined.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "local callable already defined",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42N00(
            new GqlStatus("42N00"),
            "A graph reference with the name { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "graph reference not found",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42N01(
            new GqlStatus("42N01"),
            "The constituent graph { %s } was not found in the in composite database { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.graph, GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such constituent graph exists in composite database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N02(
            new GqlStatus("42N02"),
            "Writing in read access mode not allowed.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "writing in read access mode",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N03(
            new GqlStatus("42N03"),
            "Writing to multiple graphs in the same transaction is not allowed. Use CALL IN TRANSACTION or create separate transactions in your application.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "writing to multiple graphs",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42N04(
            new GqlStatus("42N04"),
            "Failed to access database identified by { %s } while connected to session database { %s }. Connect to { %s } directly.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db1, GqlParams.StringParam.db2, GqlParams.StringParam.db3},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported access of composite database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42N05(
            new GqlStatus("42N05"),
            "Failed to access database identified by { %s } while connected to composite session database { %s }. Connect to { %s } directly or create an alias in the composite database.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db1, GqlParams.StringParam.db2, GqlParams.StringParam.db3},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported access of standard database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N06(
            new GqlStatus("42N06"),
            "{ %s } is not supported on composite databases.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.action},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported action on composite database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42N07(
            new GqlStatus("42N07"),
            "The variable { %s } is shadowing a variable with the same name from the outer scope and needs to be renamed.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable shadowing",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N08(
            new GqlStatus("42N08"),
            "The procedure { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procFun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such procedure",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N09(
            new GqlStatus("42N09"),
            "A user with the name { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.user},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such user",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT, TOPOLOGY})
    STATUS_42N0A(
            new GqlStatus("42N0A"),
            "{ %s } is not allowed with a shard target. Target the sharded database { %s } instead of { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.action, GqlParams.StringParam.db1, GqlParams.StringParam.db2
            },
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid shard target",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42N0B(
            new GqlStatus("42N0B"),
            "The database identified by { %s } is sharded. Drop the database { %s } before recreating.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db1, GqlParams.StringParam.db2},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot replace sharded database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT, TOPOLOGY})
    STATUS_42N0C(
            new GqlStatus("42N0C"),
            "{ %s } is not allowed with a { %s } target.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.action, GqlParams.StringParam.typeDescription},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid database target",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS, FIXED_TEXT})
    STATUS_42N0D(
            new GqlStatus("42N0D"),
            "The function { %s } cannot be called from the current context. It can only be used { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun, GqlParams.StringParam.context},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot call function from this context",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N0E(
            new GqlStatus("42N0E"),
            "The function { %s } cannot be called without metadata.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot call function without metadata",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS, FIXED_TEXT})
    STATUS_42N0F(
            new GqlStatus("42N0F"),
            "The function { %s } cannot be called without metadata for realm: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun, GqlParams.StringParam.auth},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot call function without metadata for realm",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N10(
            new GqlStatus("42N10"),
            "A role with the name { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.role},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such role",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42N11(
            new GqlStatus("42N11"),
            "A graph reference with the name { %s } already exists.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.dbList},
            Map.of(GqlParams.ListParam.dbList, GqlParams.JoinStyle.ORED),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "graph reference already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N12(
            new GqlStatus("42N12"),
            "A user with the name { %s } already exists.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.user},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "user already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N13(
            new GqlStatus("42N13"),
            "A role with the name { %s } already exists.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.role},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "role already exists",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N14(
            new GqlStatus("42N14"),
            "{ %s } cannot be used together with { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.clause, GqlParams.StringParam.cmd},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of command",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_42N15(
            new GqlStatus("42N15"),
            "{ %s } is a reserved keyword and cannot be used in this place.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.syntax},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of reserved keyword",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_42N16(
            new GqlStatus("42N16"),
            "Only single property { %s } are supported.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idxType},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported index or constraint",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N17(
            new GqlStatus("42N17"),
            "{ %s } is not allowed on the system database.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported request",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N18(
            new GqlStatus("42N18"),
            "The database is in read-only mode.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "read-only database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N19(
            new GqlStatus("42N19"),
            "Duplicate { %s } clause.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.syntax},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "duplicate clause",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N20(
            new GqlStatus("42N20"),
            "The list range operator '[ ]' cannot be empty.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "empty list range operator",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N21(
            new GqlStatus("42N21"),
            "Expression in { %s } must be aliased (use AS).",
            new GqlParams.GqlParam[] {GqlParams.StringParam.context},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unaliased return item",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N22(
            new GqlStatus("42N22"),
            "A COLLECT subquery must end with a single return column.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "single return column required",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N23(
            new GqlStatus("42N23"),
            "The aggregating function must be included in the { %s } clause for use in 'ORDER BY ...'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing reference to aggregation function",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N24(
            new GqlStatus("42N24"),
            "A WITH clause is required between { %s } and { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input1, GqlParams.StringParam.input2},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing WITH",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N25(
            new GqlStatus("42N25"),
            "Procedure call inside a query does not support naming results implicitly. Use YIELD instead.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing YIELD",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42N26(
            new GqlStatus("42N26"),
            "Multiple join hints for the same variable { %s } are not supported.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "multiple join hints on same variable",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N28(
            new GqlStatus("42N28"),
            "Only statically inferrable patterns and variables are allowed in { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "patterns or variables not statically inferrable",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42N29(
            new GqlStatus("42N29"),
            "Pattern expressions are not allowed to introduce new variables: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unbound variables in pattern expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N31(
            new GqlStatus("42N31"),
            "Expected { %s } to be { %s } in the range { %s } to { %s } but found { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.component,
                GqlParams.StringParam.valueType,
                GqlParams.NumberParam.lower,
                GqlParams.NumberParam.upper,
                GqlParams.StringParam.value
            },
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "specified number out of range",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N32(
            new GqlStatus("42N32"),
            "Parameter maps cannot be used in { %s } patterns. Use a literal map instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.keyword},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of parameter map",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N34(
            new GqlStatus("42N34"),
            "Path cannot be bound in a quantified path pattern.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "path bound in quantified path pattern",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N35(
            new GqlStatus("42N35"),
            "The path selector { %s } is not supported within quantified or parenthesized path patterns.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.selector},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported path selector in path pattern",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N36(
            new GqlStatus("42N36"),
            "Procedure call is missing parentheses.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "procedure call without parentheses",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N37(
            new GqlStatus("42N37"),
            "Relationship pattern predicates cannot be use in variable length relationships.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of relationship pattern predicates in variable length relationships",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N38(
            new GqlStatus("42N38"),
            "Return items must have unique names.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "duplicate return item name",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_42N39(
            new GqlStatus("42N39"),
            "All { %s } must have the same return column names. Use `AS` to ensure columns have the same name.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.context},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "incompatible return column names",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_42N3A(
            new GqlStatus("42N3A"),
            "All { %s } need to either return rows or update the graph.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.context},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "incompatible conditional query",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_42N3B(
            new GqlStatus("42N3B"),
            "All { %s } must return the same number of columns.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.context},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "incompatible number of return columns",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_42N3C(
            new GqlStatus("42N3C"),
            "Not possible to enclose { %s } in 'CALL { ... }'. Use 'CALL () { ... }' instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of CALL { ... }",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N40(
            new GqlStatus("42N40"),
            "The { %s } function must contain one relationship pattern.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "single relationship pattern required",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N41(
            new GqlStatus("42N41"),
            "The reduce function requires a '| expression' after the accumulator.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing |-expression",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N42(
            new GqlStatus("42N42"),
            "Sub-path assignment is not supported.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported sub-path binding",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE, CYPHER_CONSTRUCT})
    STATUS_42N44(
            new GqlStatus("42N44"),
            "It is not possible to access the variable { %s } declared before the { %s } clause when using `DISTINCT` or an aggregation.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable, GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "inaccessible variable",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N45(
            new GqlStatus("42N45"),
            "Unexpected end of input, expected 'CYPHER', 'EXPLAIN', 'PROFILE' or a query.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unexpected end of input",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {VALUE_TYPE})
    STATUS_42N46( // Duplicate of 42N52, use that instead
            new GqlStatus("42N46"),
            "{ %s } is not a recognized Cypher type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unexpected type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N47(
            new GqlStatus("42N47"),
            "'CALL { ... } IN TRANSACTIONS' is not supported in '... UNION ...'.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of UNION and CALL IN TRANSACTIONS",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N48(
            new GqlStatus("42N48"),
            "The function { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such function",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N49(
            new GqlStatus("42N49"),
            "Unknown Normal Form: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported normal form",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N50(
            new GqlStatus("42N50"),
            "The procedure return column { %s } is not defined for this procedure. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "procedure return column not defined",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N51(
            new GqlStatus("42N51"),
            "Invalid parameter { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.param},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid parameter",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {VALUE_TYPE})
    STATUS_42N52(
            new GqlStatus("42N52"),
            "{ %s } is not a recognized Cypher type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid value type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N53(
            new GqlStatus("42N53"),
            "The quantified path pattern may yield an infinite number of rows under match mode 'REPEATABLE ELEMENTS'. Add an upper bound to the quantified path pattern.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsafe usage of repeatable elements",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N54(
            new GqlStatus("42N54"),
            "The match mode { %s } is not supported.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.matchMode},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported match mode",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N55(
            new GqlStatus("42N55"),
            "The path selector { %s } is not supported.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.selector},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported path selector",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N56(
            new GqlStatus("42N56"),
            "Properties are not supported in the { %s } function.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported use of properties",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N57(
            new GqlStatus("42N57"),
            "{ %s } cannot contain any updating clauses.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of data-modifications in expressions",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N58(
            new GqlStatus("42N58"),
            "Nested 'CALL { ... } IN TRANSACTIONS is not supported.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported use of nesting",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42N59(
            new GqlStatus("42N59"),
            "Variable { %s } already declared.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable already defined",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N60(
            new GqlStatus("42N60"),
            "REPEATABLE ELEMENTS with { %s } path mode is not supported.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pathMode},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported combination of match mode and path mode",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N61(
            new GqlStatus("42N61"),
            "Mixing { %s } in the same graph pattern is not supported. Split the pattern into separate MATCH clauses instead.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.pathModes},
            Map.of(GqlParams.ListParam.pathModes, GqlParams.JoinStyle.ANDED),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported mixing of different path modes",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42N62(
            new GqlStatus("42N62"),
            "Variable { %s } not defined.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable not defined",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N63(
            new GqlStatus("42N63"),
            "All inner types in a Closed Dynamic Union must be nullable, or be appended with 'NOT NULL'.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "inner type with different nullability",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N64(
            new GqlStatus("42N64"),
            "A quantified or parenthesized path pattern must have at least one node or relationship pattern.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "at least one node or relationship required",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N65(
            new GqlStatus("42N65"),
            "The { %s } function requires bound node variables when it is not part of a 'MATCH ...'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "node variable not bound",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N66(
            new GqlStatus("42N66"),
            "Bound relationships are not allowed in calls to the { %s } function.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "relationship variable already bound",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N67(
            new GqlStatus("42N67"),
            "Duplicate parameter { %s } in local callable signature.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procParam},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "duplicate parameter",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_42N68(
            new GqlStatus("42N68"),
            "Variables cannot be defined more than once in a { %s } clause.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "duplicate variable definition",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS, CYPHER_CONSTRUCT})
    STATUS_42N69(
            new GqlStatus("42N69"),
            "The { %s } function is only allowed as a top-level element and not inside an { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun, GqlParams.StringParam.expr},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "function not allowed inside expression",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N70(
            new GqlStatus("42N70"),
            "The function { %s } requires a WHERE clause.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "function without required WHERE clause",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N71(
            new GqlStatus("42N71"),
            "A query must conclude with a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call without a YIELD clause.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "incomplete query",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N72(
            new GqlStatus("42N72"),
            "Calling graph functions is only supported on composite databases. Use the name directly or connect to a composite database with the desired constituents.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "graph function only supported on composite databases",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N73(
            new GqlStatus("42N73"),
            "The USE clause must be the first clause of a query or an operand to '... UNION ...' . In a CALL sub-query, it can also be the second clause if the first clause is an importing WITH.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid placement of USE clause",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42N74(
            new GqlStatus("42N74"),
            "Failed to access { %s } and { %s }. Child USE clauses must target the same graph as their parent query. Run in separate (sub)queries instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db1, GqlParams.StringParam.db2},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid nested USE clause",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_42N75(
            new GqlStatus("42N75"),
            "A call to the graph function { %s } is only allowed as the top-level argument of a USE clause.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of graph function",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE, SCHEMA})
    STATUS_42N76(
            new GqlStatus("42N76"),
            "The hint(s) { %s } cannot be fulfilled.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.hintList},
            Map.of(GqlParams.ListParam.hintList, GqlParams.JoinStyle.ANDED),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unfulfillable hints",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE, SCHEMA})
    STATUS_42N77(
            new GqlStatus("42N77"),
            "The hint { %s } cannot be fulfilled. The query does not contain a compatible predicate for { %s } on { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.hint, GqlParams.StringParam.entityType, GqlParams.StringParam.variable
            },
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing hint predicate",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE, CYPHER_CONSTRUCT})
    STATUS_42N78(
            new GqlStatus("42N78"),
            "Node { %s } has already been bound and cannot be modified by the { %s } clause.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable, GqlParams.StringParam.clause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable already bound",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N79(
            new GqlStatus("42N79"),
            "The USE clause is not required for administration commands. Retry the query without the USE clause, and it will be routed automatically.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid USE clause",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42N81(
            new GqlStatus("42N81"),
            "Expected { %s }, but got { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.param, GqlParams.ListParam.paramList},
            Map.of(GqlParams.ListParam.paramList, GqlParams.JoinStyle.ANDED),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing request parameter",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42N82(
            new GqlStatus("42N82"),
            "The database identified by { %s } has one or more aliases. Drop the aliases { %s } before dropping the database.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db, GqlParams.ListParam.aliasList},
            Map.of(GqlParams.ListParam.aliasList, GqlParams.JoinStyle.ANDED),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot drop database with aliases",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N83(
            new GqlStatus("42N83"),
            "Cannot impersonate a user while password change required.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "impersonation disallowed while password change required",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N84(
            new GqlStatus("42N84"),
            "WHERE clause without YIELD clause. Use 'TERMINATE TRANSACTION ... YIELD ... WHERE ...'.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "TERMINATE TRANSACTION misses YIELD clause",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N85(
            new GqlStatus("42N85"),
            "Allowed and denied database options are mutually exclusive.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot specify both allowed and denied databases",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N86(
            new GqlStatus("42N86"),
            "{ %s } failed. Parameterized database and graph names do not support wildcards.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.syntax},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "wildcard in parameter",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42N87(
            new GqlStatus("42N87"),
            "The database or alias name { %s } conflicts with the name { %s } of an existing database or alias.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db1, GqlParams.StringParam.db2},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "database or alias with similar name exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N89(
            new GqlStatus("42N89"),
            "Failed evaluating the given driver settings. { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cause},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid driver settings map",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42N90(
            new GqlStatus("42N90"),
            "Composite databases cannot be altered (database: { %s }).",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot alter immutable composite database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_42N91(
            // Never used, this error scenario is unreachable
            new GqlStatus("42N91"),
            "Cannot index nested properties (property: { %s }).",
            new GqlParams.GqlParam[] {GqlParams.StringParam.propKey},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot index nested property",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N92(
            new GqlStatus("42N92"),
            "Cannot combine old and new auth syntax for the same auth provider.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot combine old and new auth provider syntax",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N93(
            // Never used, 22N06 was used instead
            new GqlStatus("42N93"),
            "No auth given for user.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing auth clause",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N94(
            new GqlStatus("42N94"),
            "'ALTER USER' requires at least one clause.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "incomplete ALTER USER command",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N95(
            new GqlStatus("42N95"),
            "The combination of provider and id is already in use.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "provider-id combination already exists",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N96(
            new GqlStatus("42N96"),
            "User has no auth provider. Add at least one auth provider for the user or consider suspending them.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid user configuration",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT, FIXED_TEXT})
    STATUS_42N97(
            new GqlStatus("42N97"),
            "Clause { %s } is mandatory for auth provider { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.clause, GqlParams.StringParam.auth},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing mandatory auth clause",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N98(
            new GqlStatus("42N98"),
            "Cannot modify the user record of the current user.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot modify own user",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42N99(
            new GqlStatus("42N99"),
            "Cannot delete the user record of the current user.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot delete own user",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NA0(
            new GqlStatus("42NA0"),
            "Query contains operations that must be executed on the constituent.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "operations must be executed on constituent",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NA1(
            new GqlStatus("42NA1"),
            "Graph access operations are not supported on composite databases.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "graph access operations on composite database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NA2(
            new GqlStatus("42NA2"),
            "Database operations are not supported on composite databases.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "database operations on composite database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NA3(
            new GqlStatus("42NA3"),
            "Schema operations are not supported on composite databases.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "schema operations on composite database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NA4(
            new GqlStatus("42NA4"),
            "Transaction operations are not supported on composite databases.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "transaction operations on composite database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NA5(
            new GqlStatus("42NA5"),
            "Accessing multiple graphs in the same query is only supported on composite databases. Connect to a composite database with the desired constituents.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "accessing multiple graphs only supported on composite databases",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NA6(
            new GqlStatus("42NA6"),
            "Aliases are not allowed to target composite databases.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid alias target",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42NA7(
            new GqlStatus("42NA7"),
            "No database is corresponding to { %s }. Verify that the elementId is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "referenced database not found",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA8(
            new GqlStatus("42NA8"),
            "Invalid reference in command { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid reference in command",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NA9(
            new GqlStatus("42NA9"),
            "The system database supports a restricted set of Cypher clauses. The supported clauses include procedure calls (if the procedure is allowed), a subset of show and terminate commands, and combinations of the two. 'YIELD' and 'RETURN' are also permitted when combined with procedure calls, show, or terminate commands.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "system database rules",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_42NAA(
            new GqlStatus("42NAA"),
            "Incorrectly formatted graph reference { %s }. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "incorrectly formatted graph reference",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NAB(
            new GqlStatus("42NAB"),
            "WHERE is not supported in a standalone call. Use `CALL ... YIELD ... WHERE ... RETURN ...` instead.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "not supported standalone call",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NAC(
            new GqlStatus("42NAC"),
            "The backup metadata script contains an invalid or missing CREATE DATABASE statement. The metadata script must contain exactly one CREATE DATABASE statement and it must use the parameter $database.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid CREATE DATABASE statement",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42NAD(
            new GqlStatus("42NAD"),
            "An auth rule with the name { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.authRule},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such auth rule",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_VARIABLE})
    STATUS_42NAE(
            new GqlStatus("42NAE"),
            "An auth rule with the name { %s } already exists.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.authRule},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "auth rule already exists",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NAF(
            new GqlStatus("42NAF"),
            "USE clause is not supported in local procedure definitions.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "not supported local procedure definition",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NAG(
            new GqlStatus("42NAG"),
            "USE clause is not supported in local function definitions.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "not supported local function definition",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NFC(
            new GqlStatus("42NFC"),
            "Authentication and/or authorization could not be validated. See security logs for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "auth info validation error",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NFD(
            new GqlStatus("42NFD"),
            "Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "credentials expired",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NFE(
            new GqlStatus("42NFE"),
            "Authentication and/or authorization info expired.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "auth info expired",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_42NFF(
            new GqlStatus("42NFF"),
            "Access denied, see the security logs for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "permission/access denied",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N00(
            new GqlStatus("50N00"),
            "Internal exception raised { %s }: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msgTitle, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "internal error",
            ErrorClassification.UNKNOWN),
    STATUS_50N01(
            new GqlStatus("50N01"),
            "Remote execution by { %s } raised { %s }: { %s }",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.server, GqlParams.StringParam.msgTitle, GqlParams.StringParam.msg
            },
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "remote execution error",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_50N05(
            new GqlStatus("50N05"),
            "Deadlock detected while trying to acquire locks. See log for more details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "deadlock detected",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_50N06(
            new GqlStatus("50N06"),
            "Remote execution failed. See cause for more details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "remote execution client error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_50N07(
            new GqlStatus("50N07"),
            "Execution failed. See cause and debug log for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "execution failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_50N09(
            new GqlStatus("50N09"),
            "The server transitioned into a server state that is not valid in the current context: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.boltServerState},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "invalid server state transition",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_50N10(
            new GqlStatus("50N10"),
            "Unable to drop { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idxDescrOrName},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "index drop failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_50N11(
            new GqlStatus("50N11"),
            "Unable to create { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constrDescrOrName},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "constraint creation failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_50N12(
            new GqlStatus("50N12"),
            "Unable to drop { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constrDescrOrName},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "constraint drop failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_50N13(
            new GqlStatus("50N13"),
            "Unable to validate constraint { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.constrDescrOrName},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "constraint validation error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_50N14(
            new GqlStatus("50N14"),
            "A constraint imposed by the database was violated.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "constraint violation",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_50N15(
            new GqlStatus("50N15"),
            "The system attempted to execute an unsupported operation on index { %s }. See debug.log for more information.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idx},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "unsupported index operation",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_50N16(
            new GqlStatus("50N16"),
            "Remote execution failed. See cause for more details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "remote execution transient error",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_50N17(
            new GqlStatus("50N17"),
            "Remote execution failed. See cause for more details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "remote execution database error",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_50N18(
            new GqlStatus("50N18"),
            "Communication with shard { %s } failed. See cause for more details.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.graph},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "shard execution transient error",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_50N19(
            new GqlStatus("50N19"),
            "Communication with shard { %s } failed. See cause for more details.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.graph},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "shard execution database error",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_50N1A(
            new GqlStatus("50N1A"),
            "Failed to retrieve all shard replica locations of { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.graph},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "failed to retrieve all shard replica locations",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_50N20(
            new GqlStatus("50N20"),
            "Communication with shard { %s } failed. See cause for more details.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.graph},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "shard execution client error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_50N21(
            new GqlStatus("50N21"),
            "The { %s } was not found for { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.schemaDescrType, GqlParams.StringParam.schemaDescr},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "no such schema descriptor",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_50N22(
            new GqlStatus("50N22"),
            "Failed to parse { %s } as a graph name. Graph name parts that contain unsupported characters for unescaped identifiers require backtick escaping. Graph name parts with special characters may require additional escaping of those characters.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "invalid graph name",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER, TEMPORAL_SPATIAL})
    STATUS_50N23(
            new GqlStatus("50N23"),
            "Transaction retry aborted after { %s } attempts. Retry timed out with a maximum retry duration of { %s } { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.NumberParam.count, GqlParams.NumberParam.timeAmount, GqlParams.StringParam.timeUnit
            },
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "transaction retry aborted",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_50N24(
            new GqlStatus("50N24"),
            "Unexpected exception while getting transaction state.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "sharded properties transaction handling database error",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_50N25(
            new GqlStatus("50N25"),
            "Unexpected exception while getting transaction state.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "sharded properties transaction handling client error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_50N26(
            new GqlStatus("50N26"),
            "The backup metadata script contains invalid syntax.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "invalid backup metadata script",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_50N42(
            new GqlStatus("50N42"),
            "Unexpected error has occurred. See debug log for details.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "unexpected error",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N00(
            new GqlStatus("51N00"),
            "Failed to register procedure/function.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "procedure registration error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N01(
            new GqlStatus("51N01"),
            "The field { %s } in the class { %s } is annotated as a '@Context' field, but it is declared as static. '@Context' fields must be public, non-final and non-static.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procField, GqlParams.StringParam.procClass},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class field annotation should be public, non-final, and non-static",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N02(
            new GqlStatus("51N02"),
            "Unable to set up injection for procedure { %s }. The field { %s } has type { %s } which is not a supported injectable component.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.procClass, GqlParams.StringParam.procField, GqlParams.StringParam.procFieldType
            },
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unsupported injectable component type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N03(
            new GqlStatus("51N03"),
            "Unable to set up injection for { %s }, failed to access field { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procClass, GqlParams.StringParam.procField},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unable to access field",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N04(
            new GqlStatus("51N04"),
            "The field { %s } on { %s } must be annotated as a '@Context' field in order to store its state.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procField, GqlParams.StringParam.procClass},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "missing class field annotation",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N05(
            new GqlStatus("51N05"),
            "The field { %s } on { %s } must be declared non-final and public.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procField, GqlParams.StringParam.procClass},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class field should be public and non-final",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS, NON_SENSITIVE_NUMBER})
    STATUS_51N06(
            new GqlStatus("51N06"),
            "The argument at position { %s } in { %s } requires a '@Name' annotation and a non-empty name.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.pos, GqlParams.StringParam.procMethod},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "missing argument name",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N07(
            new GqlStatus("51N07"),
            "The { %s } contains a non-default argument after a default argument. Non-default arguments are not allowed to be positioned after default arguments.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procFun},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid ordering of default arguments",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N08(
            new GqlStatus("51N08"),
            "The class { %s } must contain exactly one '@UserAggregationResult' method and exactly one '@UserAggregationUpdate' method.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procClass},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "exactly one @UserAggregationResult method and one @UserAggregationUpdate method required",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N09(
            new GqlStatus("51N09"),
            "The '@UserAggregationUpdate' method { %s } of { %s } must be public and have the return type 'void'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procMethod, GqlParams.StringParam.procClass},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "@UserAggregationUpdate method must be public and void",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N10(
            new GqlStatus("51N10"),
            "The method { %s } of { %s } must be public.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procMethod, GqlParams.StringParam.procClass},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "aggregation method not public",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N11(
            new GqlStatus("51N11"),
            "The class { %s } must be public.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procClass},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class not public",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N12(
            new GqlStatus("51N12"),
            "The procedure { %s } has zero return columns and must be defined as void.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class not void",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N13(
            new GqlStatus("51N13"),
            "Unable to register the procedure or function { %s } because the name is already in use.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procFun},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "procedure or function name already in use",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N14(
            new GqlStatus("51N14"),
            "The procedure { %s } has a duplicate { %s } field, { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.proc, GqlParams.StringParam.procFieldType, GqlParams.StringParam.procField
            },
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "duplicate field name",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {VALUE_TYPE})
    STATUS_51N15(
            new GqlStatus("51N15"),
            "Type mismatch for map key. Required 'STRING', but found { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.valueType},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid map key type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {VALUE_TYPE, PROCEDURES_FUNCTIONS})
    STATUS_51N16(
            new GqlStatus("51N16"),
            "Type mismatch for the default value. Required { %s }, but found { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.valueType, GqlParams.StringParam.input},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid default value type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N17(
            new GqlStatus("51N17"),
            "Procedures and functions cannot be defined in the root namespace, or use a reserved namespace. Use the package name instead (e.g., org.example.com.{ %s }).",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procFun},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid procedure or function name",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N18(
            new GqlStatus("51N18"),
            "The method { %s } has an invalid return type. Procedures must return a stream of records, where each record is of a defined concrete class.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procMethod},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid method return type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_51N20(
            new GqlStatus("51N20"),
            "The field { %s } is not injectable. Ensure the field is marked as public and non-final.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procField},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot inject field",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N21(
            new GqlStatus("51N21"),
            "The procedure registration failed because the procedure registry was busy. Try again.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "procedure registry is busy",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N22(
            new GqlStatus("51N22"),
            "Finding the shortest path for the given pattern requires an exhaustive search. To enable exhaustive searches, set 'cypher.forbid_exhaustive_shortestpath' to false.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "exhaustive shortest path search disabled",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N23(
            new GqlStatus("51N23"),
            "Cannot find the shortest path when the start and end nodes are the same. To enable this behavior, set 'dbms.cypher.forbid_shortestpath_common_nodes' to false.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cyclic shortest path search disabled",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N24(
            new GqlStatus("51N24"),
            "Could not find a query plan within given time and space limits.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "insufficient resources for plan search",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N25(
            new GqlStatus("51N25"),
            "Cannot compile query due to excessive updates to indexes and constraints.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "database is busy",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_51N26(
            new GqlStatus("51N26"),
            "{ %s } is not available. This implementation of Cypher does not support { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.item, GqlParams.StringParam.feat},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported in this version",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N27(
            new GqlStatus("51N27"),
            "{ %s } is not supported in { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.feat, GqlParams.StringParam.edition},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported in this edition",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N28(
            new GqlStatus("51N28"),
            "This Cypher command must be executed against the database { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported by this database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_51N29(
            new GqlStatus("51N29"),
            "The command { %s } must be executed on the current 'LEADER' server.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported by this server",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_51N2A(
            new GqlStatus("51N2A"),
            "The command { %s } is not available with auth disabled.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported with auth disabled",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_51N30(
            new GqlStatus("51N30"),
            "{ %s } is not supported in { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.item, GqlParams.StringParam.context},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported with this configuration",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT, VALUE_TYPE, SCHEMA, FIXED_TEXT})
    STATUS_51N31(
            new GqlStatus("51N31"),
            "{ %s } is not supported in { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.feat, GqlParams.StringParam.context},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N32(
            new GqlStatus("51N32"),
            "Server is in panic.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "server panic",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N33(
            new GqlStatus("51N33"),
            "This member failed to replicate transaction, try again.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "replication error",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N34(
            new GqlStatus("51N34"),
            "Failed to write to the database due to a cluster leader change. Retrying your request at a later time may succeed.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "write transaction failed due to leader change",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N35(
            new GqlStatus("51N35"),
            "The location of { %s } has changed while the transaction was running.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "database location changed",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N36(
            new GqlStatus("51N36"),
            "There is not enough memory to perform the current task.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "out of memory",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N37(
            new GqlStatus("51N37"),
            "There is not enough stack size to perform the current task.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "stack overflow",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N38(
            new GqlStatus("51N38"),
            "There are insufficient threads available for executing the current task.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "failed to acquire execution thread",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N39(
            new GqlStatus("51N39"),
            "Expected set of files not found on disk. Please restore from backup.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "raft log corrupted",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N40(
            new GqlStatus("51N40"),
            "Database { %s } failed to start. Try restarting it.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unable to start database",
            ErrorClassification.DATABASE_ERROR),
    STATUS_51N41(
            new GqlStatus("51N41"),
            "Server or database admin operation not possible. Reason: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "admin operation failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N42(
            new GqlStatus("51N42"),
            "Unable to check if allocator { %s } is available.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.alloc},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "allocator availability check failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N43(
            new GqlStatus("51N43"),
            "Cannot deallocate server(s) { %s }.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.serverList},
            Map.of(GqlParams.ListParam.serverList, GqlParams.JoinStyle.ANDED),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot deallocate servers",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N44(
            new GqlStatus("51N44"),
            "Cannot drop server { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot drop server",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N45(
            new GqlStatus("51N45"),
            "Cannot cordon server { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot cordon server",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N46(
            new GqlStatus("51N46"),
            "Cannot alter server { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot alter server",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N47(
            new GqlStatus("51N47"),
            "Cannot rename server { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot rename server",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N48(
            new GqlStatus("51N48"),
            "Cannot enable server { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot enable server",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N49(
            new GqlStatus("51N49"),
            "Cannot alter database { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot alter database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N50(
            new GqlStatus("51N50"),
            "Cannot recreate database { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot recreate database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N51(
            new GqlStatus("51N51"),
            "Cannot create database { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot create database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT, NON_SENSITIVE_NUMBER})
    STATUS_51N52(
            new GqlStatus("51N52"),
            "Cannot { %s } database topology. Number of primaries { %s } needs to be at least 1 and may not exceed { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.action, GqlParams.NumberParam.count, GqlParams.NumberParam.upper
            },
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "number primaries out of range",
            ErrorClassification.CLIENT_ERROR) // message sounds a little off for me for these two. maybe "numbe rof
    // primaries/secondaries ({ %s }) instead?
    ,
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N53(
            new GqlStatus("51N53"),
            "Cannot { %s } database topology. Number of secondaries { %s } needs to be at least 0 and may not exceed { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.action, GqlParams.NumberParam.count, GqlParams.NumberParam.upper
            },
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "number secondaries out of range",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N54(
            new GqlStatus("51N54"),
            "Failed to calculate reallocation for databases. { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot reallocate",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY, CONFIG_SETTING})
    STATUS_51N55(
            new GqlStatus("51N55"),
            "Failed to create the database { %s }. The limit of databases is reached. Either increase the limit using the config setting { %s } or drop a database.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db, GqlParams.StringParam.cfgSetting},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot create additional database",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY, NON_SENSITIVE_NUMBER})
    STATUS_51N56(
            new GqlStatus("51N56"),
            "The number of { %s } seeding servers { %s } is larger than the desired number of { %s } allocations { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.serverType,
                GqlParams.NumberParam.count1,
                GqlParams.StringParam.allocType,
                GqlParams.NumberParam.count2
            },
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "topology out of range",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_51N57(
            new GqlStatus("51N57"),
            "Unexpected error while picking allocations. { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "generic topology modification error",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT, NON_SENSITIVE_NUMBER})
    STATUS_51N58(
            new GqlStatus("51N58"),
            "Invalid database shard topology. The number of { %s } { %s } needs to be at least 1 and may not exceed { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.context, GqlParams.NumberParam.count, GqlParams.NumberParam.upper
            },
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid shard topology",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N59(
            new GqlStatus("51N59"),
            "The DBMS is unable to handle the request, please retry later or contact the system operator. More information is present in the logs.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "internal resource exhaustion",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N60(
            new GqlStatus("51N60"),
            "The DBMS is unable to determine the enterprise license acceptance status.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unable to check enterprise license acceptance",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_51N61(
            new GqlStatus("51N61"),
            "Index { %s } population failed.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idx},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "index population failed",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_51N62(
            new GqlStatus("51N62"),
            "Unable to use index { %s } because it is in a failed state. See logs for more information.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idx},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "index is in a failed state",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_51N63(
            new GqlStatus("51N63"),
            "Index { %s } is not ready yet. Wait until it finishes populating and retry the transaction.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idx},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "index is still populating",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N64(
            new GqlStatus("51N64"),
            "The index dropped while sampling.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "index dropped while sampling",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA, NON_SENSITIVE_NUMBER})
    STATUS_51N65(
            new GqlStatus("51N65"),
            "Vector index { %s } has a configured dimensionality of { %s }, but the provided vector has a dimension { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idx, GqlParams.NumberParam.dim1, GqlParams.NumberParam.dim2
            },
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "vector index dimensionality mismatch",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N66(
            new GqlStatus("51N66"),
            "Insufficient resources to complete the request.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "resource exhaustion",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS, FIXED_TEXT})
    STATUS_51N67(
            new GqlStatus("51N67"),
            "Unexpected CDC selector { %s } at { %s }, expected selector to be a { %s } selector.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.selectorType1, GqlParams.StringParam.input, GqlParams.StringParam.selectorType2
            },
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid CDC selector type",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N68(
            new GqlStatus("51N68"),
            "Change Data Capture is not currently enabled for this database.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "CDC is disabled for this database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CYPHER_CONSTRUCT})
    STATUS_51N69(
            new GqlStatus("51N69"),
            "It is not possible to perform { %s } on the system database.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.operation},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "system database is immutable",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N70(
            new GqlStatus("51N70"),
            "Cannot get routing table for { %s } because Bolt is not enabled. Please update your configuration such that 'server.bolt.enabled' is set to true.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "bolt is not enabled",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT, SCHEMA})
    STATUS_51N71(
            new GqlStatus("51N71"),
            "Feature: { %s } is not available in a sharded database.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.feat},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unsupported operation on a sharded database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CONFIG_SETTING})
    STATUS_51N72(
            new GqlStatus("51N72"),
            "Failed to allocate memory in a memory pool. See { %s } in the neo4j.conf file.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cfgSetting},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "memory pool out of memory",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CONFIG_SETTING})
    STATUS_51N73(
            new GqlStatus("51N73"),
            "The transaction uses more memory than it is allowed. The maximum allowed size for a transaction can be configured with { %s } in the neo4j.conf file.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cfgSetting},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "transaction memory limit reached",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {CONFIG_SETTING})
    STATUS_51N74(
            new GqlStatus("51N74"),
            "Failed to start a new transaction. The limit of concurrent transactions is reached. Increase the number of concurrent transactions using { %s } in the neo4j.conf file.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cfgSetting},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "maximum number of transactions reached",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {SCHEMA})
    STATUS_51N75(
            new GqlStatus("51N75"),
            "Unable to find entity with id { %s } since it is not up to date. Retrying your request at a later time may succeed.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.entityId},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "shard execution error",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N76(
            new GqlStatus("51N76"),
            "The upgrade to a new Neo4j version failed.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "upgrade failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_51N77(
            new GqlStatus("51N77"),
            "{ %s } is not supported in { %s } store format.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.feat, GqlParams.StringParam.storeFormat},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported in this store format",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N78(
            new GqlStatus("51N78"),
            "Routing is not permitted via this connector. Switch the connection URI scheme to bolt:// or connect to a connector with routing support.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "routing unavailable",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_51N79(
            new GqlStatus("51N79"),
            "Access to database { %s } is not permitted via this connector.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "database unavailable",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_51N7A(
            new GqlStatus("51N7A"),
            "No admin user candidate is found. Use `neo4j-admin dbms set-default-admin` to select a valid admin.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "no admin user candidate",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS, NON_SENSITIVE_NUMBER, TEMPORAL_SPATIAL})
    STATUS_52N01(
            new GqlStatus("52N01"),
            "Execution of the procedure { %s } timed out after { %s } { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.proc, GqlParams.NumberParam.timeAmount, GqlParams.StringParam.timeUnit
            },
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "procedure execution timeout",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_52N02(
            new GqlStatus("52N02"),
            "Execution of the procedure { %s } failed due to a client error.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "procedure execution client error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_52N03(
            new GqlStatus("52N03"),
            "Execution of the procedure { %s } failed due to an invalid specified execution mode { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc, GqlParams.StringParam.procExeMode},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid procedure execution mode",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_52N05(
            new GqlStatus("52N05"),
            "Cannot invoke procedure on this member because it is not a secondary for the database { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "must invoke procedure on secondary",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_52N06(
            new GqlStatus("52N06"),
            "Unexpected number of arguments (expected 0-2 but received { %s }).",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.count},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid number of arguments to checkConnectivity",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_52N07(
            new GqlStatus("52N07"),
            "Unrecognised port name { %s } (valid values are: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.port, GqlParams.ListParam.portList},
            Map.of(GqlParams.ListParam.portList, GqlParams.JoinStyle.ANDED),
            Condition.PROCEDURE_EXCEPTION,
            "invalid port argument to checkConnectivity",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_52N08(
            new GqlStatus("52N08"),
            "Unable to parse server id { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid server id argument to checkConnectivity",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_52N09(
            new GqlStatus("52N09"),
            "Execution of the procedure { %s } failed due to a database error.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "procedure execution database error",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N10(
            new GqlStatus("52N10"),
            "An address key is included in the query string provided to the GetRoutingTableProcedure, but its value could not be parsed.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid address key",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N11(
            new GqlStatus("52N11"),
            "An unexpected error has occurred: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "generic topology procedure error",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_52N12(
            new GqlStatus("52N12"),
            "The previous default database { %s } is still running.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "cannot change default database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_52N13(
            new GqlStatus("52N13"),
            "New default database { %s } does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "new default database does not exist",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N14(
            new GqlStatus("52N14"),
            "System database cannot be set as default.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "system cannot be default database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_52N15(
            new GqlStatus("52N15"),
            "Provided allocator { %s } is not available or was not initialized. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.alloc},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "no such allocator",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N16(
            new GqlStatus("52N16"),
            "Invalid arguments to procedure.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid procedure argument list",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N17(
            new GqlStatus("52N17"),
            "Setting/removing the quarantine marker failed: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "quarantine change failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NON_SENSITIVE_NUMBER})
    STATUS_52N18(
            new GqlStatus("52N18"),
            "The number of seeding servers { %s } is larger than the defined number of allocations { %s }.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.countSeeders, GqlParams.NumberParam.countAllocs},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "too many seeders",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_52N19(
            new GqlStatus("52N19"),
            "The specified seeding server with id { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "no such seeder",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N20(
            new GqlStatus("52N20"),
            "The recreation of a database is not supported when seed updating is not enabled.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "seed updating not enabled",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N21(
            new GqlStatus("52N21"),
            "Failed to clean the system graph.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "failed to clean the system graph",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N22(
            new GqlStatus("52N22"),
            "Invalid argument { %s } for { %s } on procedure { %s }. The expected format of { %s } is { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.field,
                GqlParams.StringParam.procParam,
                GqlParams.StringParam.proc,
                GqlParams.StringParam.procParam,
                GqlParams.StringParam.procParamFmt
            },
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid procedure argument",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_52N23(
            new GqlStatus("52N23"),
            "The following namespaces are not reloadable: { %s }",
            new GqlParams.GqlParam[] {GqlParams.ListParam.namespaceList},
            Map.of(GqlParams.ListParam.namespaceList, GqlParams.JoinStyle.ANDED),
            Condition.PROCEDURE_EXCEPTION,
            "non-reloadable namespace",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N24(
            new GqlStatus("52N24"),
            "Failed to reload procedures. See logs for more information.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "failed to reload procedures",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {METADATA})
    STATUS_52N25(
            new GqlStatus("52N25"),
            "JMX error while accessing { %s }. See logs for more information.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.param},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "JMX error",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_52N26(
            new GqlStatus("52N26"),
            "{ %s } is not a valid change identifier.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.param},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid change identifier",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N27(
            new GqlStatus("52N27"),
            "The commit timestamp for the provided transaction ID does not match the one in the transaction log.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid commit timestamp",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_52N28(
            new GqlStatus("52N28"),
            "{ %s } is not a valid change identifier. Transaction ID does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.transactionId},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid change identifier and transaction id",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N29(
            new GqlStatus("52N29"),
            "Given ChangeIdentifier describes a transaction that occurred before any enrichment records exist.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "outdated change identifier",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N30(
            new GqlStatus("52N30"),
            "Given ChangeIdentifier describes a transaction that hasn't yet occurred.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "future change identifier",
            ErrorClassification.TRANSIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY})
    STATUS_52N31(
            new GqlStatus("52N31"),
            "Change identifier { %s } does not belong to this database.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.param},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "wrong database",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {TOPOLOGY, NON_SENSITIVE_NUMBER})
    STATUS_52N32(
            new GqlStatus("52N32"),
            "Change identifier { %s } has an invalid sequence number { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.param1, GqlParams.StringParam.param2},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid sequence number",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N33(
            new GqlStatus("52N33"),
            "Failed to invoke procedure/function { %s } caused by: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.sig, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "procedure invocation failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_52N34(
            new GqlStatus("52N34"),
            "{ %s } is restricted and accesses database internals. Procedure restriction is controlled by the dbms.security.procedures.unrestricted setting. Only un-restrict procedures you can trust with access to database internals.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "procedure restricted",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_52N35(
            new GqlStatus("52N35"),
            "Failed to compile procedure defined in { %s }: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.procClass, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "procedure compilation failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_52N36(
            new GqlStatus("52N36"),
            "Invalid argument { %s } for { %s } on procedure { %s }. The expected format of { %s } is outlined in the { %s } API documentation.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.field,
                GqlParams.StringParam.procParam,
                GqlParams.StringParam.proc,
                GqlParams.StringParam.procParam,
                GqlParams.StringParam.procParamFmt
            },
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid procedure argument with API documentation hint",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_52N37(
            new GqlStatus("52N37"),
            "Execution of the procedure { %s } failed.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "procedure execution error",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N38(
            new GqlStatus("52N38"),
            "Cannot find a start position in the logs.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "cdc start position not found",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N39(
            new GqlStatus("52N39"),
            "The log scanner is no longer active.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "cdc scanner inactive",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_52N40(
            new GqlStatus("52N40"),
            "Reconciliation failed during writing the topology graph, transaction may not be committed.",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "reconciler execution error",
            ErrorClassification.DATABASE_ERROR),
    @NonSensitiveStatusDescription(reasons = {FIXED_TEXT})
    STATUS_52N41(
            new GqlStatus("52N41"),
            "The key value for { %s } in the query string cannot be parsed when getting a routing table.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.field},
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "invalid routing key",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52U00(
            new GqlStatus("52U00"),
            "Execution of the procedure { %s } failed due to { %s }: { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.proc, GqlParams.StringParam.msgTitle, GqlParams.StringParam.msg
            },
            emptyMap(),
            Condition.PROCEDURE_EXCEPTION,
            "custom procedure execution error cause",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS, FIXED_TEXT})
    STATUS_53N33(
            new GqlStatus("53N33"),
            "Failed to invoke function { %s } caused by: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.sig, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.FUNCTION_EXCEPTION,
            "function invocation failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_53N34(
            new GqlStatus("53N34"),
            "{ %s } is restricted and accesses database internals. User-defined function restriction is controlled by the dbms.security.procedures.unrestricted setting. Only un-restrict user-defined functions you can trust with access to database internals.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.FUNCTION_EXCEPTION,
            "function restricted",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_53N35(
            new GqlStatus("53N35"),
            "Failed to compile function defined in { %s }: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.funClass, GqlParams.StringParam.msg},
            emptyMap(),
            Condition.FUNCTION_EXCEPTION,
            "function compilation failed",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {PROCEDURES_FUNCTIONS})
    STATUS_53N37(
            new GqlStatus("53N37"),
            "Execution of the function { %s } failed.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.fun},
            emptyMap(),
            Condition.FUNCTION_EXCEPTION,
            "function execution error",
            ErrorClassification.UNKNOWN),
    STATUS_53U00(
            new GqlStatus("53U00"),
            "Execution of the function { %s } failed due to { %s }: { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.fun, GqlParams.StringParam.msgTitle, GqlParams.StringParam.msg
            },
            emptyMap(),
            Condition.FUNCTION_EXCEPTION,
            "custom function execution error cause",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_G1000(
            new GqlStatus("G1000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DEPENDENT_OBJECT_ERROR,
            "",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_G1001(
            new GqlStatus("G1001"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DEPENDENT_OBJECT_ERROR,
            "edges still exist",
            ErrorClassification.CLIENT_ERROR),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_G1002(
            new GqlStatus("G1002"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DEPENDENT_OBJECT_ERROR,
            "endpoint node is deleted",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_G1003(
            new GqlStatus("G1003"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.DEPENDENT_OBJECT_ERROR,
            "endpoint node not in current working graph",
            ErrorClassification.UNKNOWN),
    @NonSensitiveStatusDescription(reasons = {NO_PARAMETERS})
    STATUS_G2000(
            new GqlStatus("G2000"),
            "",
            new GqlParams.GqlParam[] {},
            emptyMap(),
            Condition.GRAPH_TYPE_VIOLATION,
            "",
            ErrorClassification.UNKNOWN),
    ;

    private final GqlStatus gqlStatus;
    private final GqlParams.GqlParam[] statusParameterKeys;
    private final String subCondition;
    private final Condition condition;
    private final GqlClassification classification;
    private final String template;
    private final int[] offsets;
    private final Map<GqlParams.ListParam, GqlParams.JoinStyle> joinStyles;

    GqlStatusInfoCodes(
            GqlStatus gqlStatus,
            String template,
            GqlParams.GqlParam[] statusParameterKeys,
            Map<GqlParams.ListParam, GqlParams.JoinStyle> joinStyles,
            Condition condition,
            String subCondition,
            GqlClassification classification) {
        this.gqlStatus = gqlStatus;
        this.statusParameterKeys = statusParameterKeys;
        this.joinStyles = joinStyles;
        this.condition = condition;
        this.subCondition = subCondition;
        this.classification = classification;
        this.template = template;
        this.offsets = getOffSets();
    }

    // for testing
    public Map<GqlParams.ListParam, GqlParams.JoinStyle> getJoinStyles() {
        return joinStyles;
    }

    @Override
    public GqlStatus getGqlStatus() {
        return gqlStatus;
    }

    @Override
    public String getStatusString() {
        return gqlStatus.gqlStatusString();
    }

    public String getTemplate() {
        return template;
    }

    public int[] getOffSets() {
        return getOffsets(template, GqlParams.substitution);
    }

    public int[] getOffsets(String template, String substitution) {
        int offset = 0;
        ArrayList<Integer> offsets = new ArrayList<>();
        while ((offset = template.indexOf(substitution, offset)) != -1) {
            offsets.add(offset);
            offset++;
        }
        return offsets.stream().mapToInt(o -> o).toArray();
    }

    @Override
    public String getMessage(Object[] params) {
        return SimpleMessageFormatter.format(
                statusParameterKeys,
                joinStyles,
                template,
                GqlParams.substitution,
                offsets,
                populateMissingParams(params));
    }

    @Override
    public String getMessage(Map<GqlParams.GqlParam, Object> params) {
        return SimpleMessageFormatter.format(
                statusParameterKeys, joinStyles, template, GqlParams.substitution, offsets, orderedParams(params));
    }

    @Override
    public String getSubCondition() {
        return subCondition;
    }

    @Override
    public GqlClassification getClassification() {
        return classification;
    }

    @Override
    public Condition getCondition() {
        return condition;
    }

    @Override
    public boolean hasNonSensitiveStatusDescription() {
        try {
            Field field = this.getDeclaringClass().getField(this.name());
            return field.isAnnotationPresent(NonSensitiveStatusDescription.class);
        } catch (NoSuchFieldException e) {
            // Fallback if the field somehow isn't found - should not happen
            return false;
        }
    }

    @Override
    public Map<String, Object> parameterMap(Object[] params) {
        final var keys = statusParameterKeys;

        // Almost all codes have zero or one parameters, so here is an un-necessary micro optimisation for that case.
        if (keys.length == 0 || params == null) {
            return emptyMap();
        } else if (keys.length == 1 && params.length > 0) {
            return singletonMap(keys[0].name(), params[0]);
        } else {
            final var result = HashMap.<String, Object>newHashMap(keys.length);
            for (int i = 0; i < keys.length && i < params.length; i++) {
                final var key = keys[i];
                result.put(key.name(), params[i]);
            }
            return result;
        }
    }

    @Override
    public Map<String, Object> parameterMap(Map<GqlParams.GqlParam, Object> params) {
        final var keys = statusParameterKeys;

        // Almost all codes have zero or one parameters, so here is an un-necessary micro optimisation for that case.
        if (keys.length == 0 || params == null) {
            return emptyMap();
        } else if (keys.length == 1 && params.containsKey(keys[0])) {
            return singletonMap(keys[0].name(), params.get(keys[0]));
        } else {
            final var result = HashMap.<String, Object>newHashMap(statusParameterKeys.length);
            for (int i = 0; i < statusParameterKeys.length; i++) {
                final var key = statusParameterKeys[i];
                result.put(key.name(), params.get(key));
            }
            return result;
        }
    }

    @Override
    public int parameterCount() {
        return statusParameterKeys.length;
    }

    // Visible for testing
    int messageFormatParameterCount() {
        return offsets.length;
    }

    @Override
    public List<GqlParams.GqlParam> getStatusParameterKeys() {
        return Collections.unmodifiableList(Arrays.asList(statusParameterKeys));
    }

    // In case of missing parameters (should not happen) we substitute with the param name to make message more readable
    private Object[] populateMissingParams(Object[] params) {
        if (params == null) params = new Object[0];
        if (params.length >= statusParameterKeys.length) {
            return params;
        } else {
            final var result = Arrays.copyOf(params, statusParameterKeys.length);
            for (int i = params.length; i < result.length; ++i) result[i] = statusParameterKeys[i].toParamFormat();
            return result;
        }
    }

    private Object[] orderedParams(Map<GqlParams.GqlParam, Object> paramMap) {
        if (paramMap == null) paramMap = emptyMap();
        final var params = new Object[statusParameterKeys.length];
        for (int i = 0; i < statusParameterKeys.length; ++i) {
            final var key = statusParameterKeys[i];
            params[i] = readableValue(key, paramMap.get(key));
        }
        return params;
    }

    // In case of missing parameters (should not happen) we substitute with the param name to make message more
    // readable.
    private static Object readableValue(GqlParams.GqlParam param, Object value) {
        return value != null ? value : param.toParamFormat();
    }
}
