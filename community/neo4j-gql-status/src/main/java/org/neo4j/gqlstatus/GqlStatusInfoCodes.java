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
import static org.neo4j.gqlstatus.NonSensitiveReason.CONFIG_SETTING;
import static org.neo4j.gqlstatus.NonSensitiveReason.CYPHER_CONSTRUCT;
import static org.neo4j.gqlstatus.NonSensitiveReason.CYPHER_VARIABLE;
import static org.neo4j.gqlstatus.NonSensitiveReason.FIXED_TEXT;
import static org.neo4j.gqlstatus.NonSensitiveReason.METADATA;
import static org.neo4j.gqlstatus.NonSensitiveReason.NON_SENSITIVE_NUMBER;
import static org.neo4j.gqlstatus.NonSensitiveReason.PROCEDURES_FUNCTIONS;
import static org.neo4j.gqlstatus.NonSensitiveReason.SCHEMA;
import static org.neo4j.gqlstatus.NonSensitiveReason.TEMPORAL_SPATIAL;
import static org.neo4j.gqlstatus.NonSensitiveReason.TOPOLOGY;
import static org.neo4j.gqlstatus.NonSensitiveReason.VALUE_TYPE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum GqlStatusInfoCodes implements GqlStatusInfo {
    STATUS_00000("", "", NotificationClassification.UNKNOWN),
    STATUS_00001("", "omitted result", NotificationClassification.UNKNOWN),
    STATUS_00N50(
            "The database { %s } does not exist. Verify that the spelling is correct or create the database for the command to take effect.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.db},
            "home database does not exist",
            NotificationClassification.UNRECOGNIZED),
    STATUS_00N70(
            "The command { %s } has no effect. The role or privilege is already assigned.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd},
            "role or privilege already assigned",
            NotificationClassification.SECURITY),
    STATUS_00N71(
            "The command { %s } has no effect. The role or privilege is not assigned.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd},
            "role or privilege not assigned",
            NotificationClassification.SECURITY),
    STATUS_00N72(
            "The auth provider { %s } is not defined in the configuration. Verify that the spelling is correct or define { %s } in the configuration.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.auth, GqlParams.StringParam.auth},
            "undefined auth provider",
            NotificationClassification.SECURITY),
    STATUS_00N80(
            "The command 'ENABLE SERVER' has no effect. Server { %s } is already enabled. Verify that this is the intended server.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            "server already enabled",
            NotificationClassification.TOPOLOGY),
    STATUS_00N81(
            "The command 'CORDON SERVER' has no effect. The server { %s } is already cordoned. Verify that this is the intended server.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            "server already cordoned",
            NotificationClassification.TOPOLOGY),
    STATUS_00N82(
            "The command 'REALLOCATE DATABASES' has no effect. No databases were reallocated. No better allocation is currently possible.",
            "no databases reallocated",
            NotificationClassification.TOPOLOGY),
    STATUS_00N83(
            "Cordoned servers existed when making an allocation decision. Server(s) { %s } are cordoned. This can impact allocation decisions.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.serverList},
            Map.of(GqlParams.ListParam.serverList, GqlParams.JoinStyle.ANDED),
            "cordoned servers existed during allocation",
            NotificationClassification.TOPOLOGY),
    STATUS_00N84(
            "The command 'ALTER DATABASE' has no effect. The requested topology matched the current topology. No allocations were changed.",
            "requested topology matched current topology",
            NotificationClassification.TOPOLOGY),
    STATUS_00NA0(
            "The command { %s } has no effect. The index or constraint specified by { %s } already exists.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd, GqlParams.StringParam.idxOrConstrPat},
            "index or constraint already exists",
            NotificationClassification.SCHEMA),
    STATUS_00NA1(
            "The command { %s } has no effect. The specified index or constraint { %s } does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd, GqlParams.StringParam.idxOrConstr},
            "index or constraint does not exist",
            NotificationClassification.SCHEMA),
    STATUS_00NA2(
            "When creating a vector index, `vector.dimensions` should be specified. Omitting it is allowed, but specifying dimensions ensures that only vectors of that size are indexed and makes dimension mismatches fail clearly at query time. For example, set `OPTIONS { indexConfig: { `vector.dimensions`: 1536 } }` when creating the index.",
            new GqlParams.GqlParam[] {},
            "vector index dimensions not specified",
            NotificationClassification.SCHEMA),
    STATUS_01000("", "", NotificationClassification.UNKNOWN),
    STATUS_01004("", "string data, right truncation", NotificationClassification.UNKNOWN),
    STATUS_01G03("", "graph does not exist", NotificationClassification.UNKNOWN),
    STATUS_01G04("", "graph type does not exist", NotificationClassification.UNKNOWN),
    STATUS_01G11("", "null value eliminated in set function", NotificationClassification.UNRECOGNIZED),
    STATUS_01N00(
            "{ %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.item},
            "feature deprecated",
            NotificationClassification.DEPRECATION),
    STATUS_01N01(
            "{ %s } is deprecated. It is replaced by { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.feat1, GqlParams.StringParam.feat2},
            "feature deprecated with replacement",
            NotificationClassification.DEPRECATION),
    STATUS_01N02(
            "{ %s } is deprecated and will be removed without a replacement.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.feat},
            "feature deprecated without replacement",
            NotificationClassification.DEPRECATION),
    STATUS_01N30(
            "Unable to create a plan with 'JOIN ON { %s }'. Try to change the join key(s) or restructure your query.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.variableList},
            Map.of(GqlParams.ListParam.variableList, GqlParams.JoinStyle.COMMAD),
            "join hint unfulfillable",
            NotificationClassification.HINT),
    STATUS_01N31(
            "Unable to create a plan with { %s } because the index does not exist.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.idxDescr},
            "hinted index does not exist",
            NotificationClassification.HINT),
    STATUS_01N40(
            "The query cannot be executed with { %s }; instead, { %s } is used. Cause: { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.preparserInput1, GqlParams.StringParam.preparserInput2, GqlParams.StringParam.msg
            },
            "unsupported runtime",
            NotificationClassification.UNSUPPORTED),
    STATUS_01N42("Unknown warning.", "unknown warning", NotificationClassification.UNKNOWN),
    STATUS_01N50(
            "The label { %s } does not exist in database { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.label, GqlParams.StringParam.db},
            "label does not exist",
            NotificationClassification.UNRECOGNIZED),
    STATUS_01N51(
            "The relationship type { %s } does not exist in database { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.relType, GqlParams.StringParam.db},
            "relationship type does not exist",
            NotificationClassification.UNRECOGNIZED),
    STATUS_01N52(
            "The property { %s } does not exist in database { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.propKey, GqlParams.StringParam.db},
            "property key does not exist",
            NotificationClassification.UNRECOGNIZED),
    STATUS_01N60(
            "The query plan cannot be cached and is not executable without 'EXPLAIN' due to the undefined parameter(s) { %s }. Provide the parameter(s).",
            new GqlParams.GqlParam[] {GqlParams.ListParam.paramList},
            Map.of(GqlParams.ListParam.paramList, GqlParams.JoinStyle.ANDED),
            "parameter missing",
            NotificationClassification.GENERIC),
    STATUS_01N61(
            "The expression { %s } cannot be satisfied because relationships must have exactly one type.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.labelExpr},
            "unsatisfiable relationship type expression",
            NotificationClassification.GENERIC),
    STATUS_01N62(
            "Execution of the procedure { %s } generated the warning { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc, GqlParams.StringParam.msg},
            "procedure or function execution warning",
            NotificationClassification.GENERIC),
    STATUS_01N63(
            "{ %s } is repeated in { %s }, which leads to no results.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.variable, GqlParams.StringParam.pat},
            "repeated relationship pattern variable",
            NotificationClassification.GENERIC),
    STATUS_01N70(
            "The command { %s } has no effect. Make sure nothing is misspelled. This notification will become an error in a future major version. Cause: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd, GqlParams.StringParam.msg},
            "inoperational revoke command",
            NotificationClassification.SECURITY),
    STATUS_01N71(
            "Use the setting 'dbms.security.require_local_user' to enable external auth.",
            "external auth disabled",
            NotificationClassification.SECURITY),
    STATUS_01N72(
            "Query uses an insecure protocol. Consider using 'https' instead.",
            "insecure URL protocol",
            NotificationClassification.SECURITY),
    STATUS_01N73(
            "The specified privilege severely reduces the performance of queries run on sharded databases. Consider excluding sharded databases for now.",
            "sharded privilege performance",
            NotificationClassification.SECURITY),
    STATUS_01N74(
            "Use the setting 'dbms.security.allow_oidc_credential_forwarding_enabled' to enable OIDC credential forwarding.",
            "OIDC credential forwarding disabled",
            NotificationClassification.SECURITY),

    STATUS_01N80(
            "Server `{ %s }` at address `{ %s }` failed: { %s }",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.server, GqlParams.StringParam.serverAddress, GqlParams.StringParam.msg
            },
            "server failed",
            NotificationClassification.TOPOLOGY),
    STATUS_01N81(
            "Server `{ %s }` at address `{ %s }` is still catching up.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server, GqlParams.StringParam.serverAddress},
            "server is catching up",
            NotificationClassification.TOPOLOGY),
    STATUS_01N82(
            "Server `{ %s }` is not available.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server},
            "server is not available",
            NotificationClassification.TOPOLOGY),
    STATUS_01N83(
            "Client does not support type `{ %s }`. Please upgrade your client.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.valueType},
            "client does not support type",
            NotificationClassification.UNSUPPORTED),
    STATUS_01N84(
            "The Cypher planner version { %s } is no longer supported. The default planner version is used instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value},
            "unsupported planner version",
            NotificationClassification.UNSUPPORTED),

    STATUS_02000("", "", NotificationClassification.UNKNOWN),
    STATUS_02N42("Unknown GQLSTATUS from old server.", "unknown subcondition", NotificationClassification.UNKNOWN),
    STATUS_03000("", "", NotificationClassification.UNKNOWN),
    STATUS_03N42("Unknown notification.", "unknown notification", NotificationClassification.UNKNOWN),
    STATUS_03N60(
            "The variable { %s } in the subquery uses the same name as a variable from the outer query. Use '{ %s } ({ %s })' to import the one from the outer scope unless you want it to be a new variable.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.variable, GqlParams.StringParam.clause, GqlParams.StringParam.variable
            },
            "subquery variable shadowing",
            NotificationClassification.GENERIC),
    STATUS_03N61(
            "The use of `OPTIONAL` is redundant as `CALL { %s }` is a void procedure.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.proc},
            "redundant optional procedure",
            NotificationClassification.GENERIC),
    STATUS_03N62(
            "The use of `OPTIONAL` is redundant as `CALL` is a unit subquery.",
            "redundant optional subquery",
            NotificationClassification.GENERIC),
    STATUS_03N63(
            "The identifier { %s } in the { %s } clause has the same name as a variable in scope. Regardless of what the variable evaluates to, it is the literal { %s } that will be used.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.ident, GqlParams.StringParam.clause, GqlParams.StringParam.ident
            },
            "identifier shadowing variable",
            NotificationClassification.GENERIC),
    STATUS_03N64(
            "Local { %s } { %s } shadows a built-in or external { %s } with the same name.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.item, GqlParams.StringParam.ident, GqlParams.StringParam.item
            },
            "callable shadowing",
            NotificationClassification.GENERIC),
    STATUS_03N85(
            "Server `{ %s }` at address `{ %s }` has caught up.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.server, GqlParams.StringParam.serverAddress},
            "server has caught up",
            NotificationClassification.GENERIC),
    STATUS_03N90(
            "The disconnected pattern { %s } builds a cartesian product. A cartesian product may produce a large amount of data and slow down query processing.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pat},
            "cartesian product",
            NotificationClassification.PERFORMANCE),
    STATUS_03N91(
            "The provided pattern { %s } is unbounded. Shortest path with an unbounded pattern may result in long execution times. Use an upper limit (e.g. '[*..5]') on the number of node hops in your pattern.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pat},
            "unbounded variable length pattern",
            NotificationClassification.PERFORMANCE),
    STATUS_03N92(
            "The query runs with exhaustive shortest path due to the existential predicate(s) { %s }. It may be possible to use 'WITH' to separate the 'MATCH' from the existential predicate(s).",
            new GqlParams.GqlParam[] {GqlParams.ListParam.predList},
            Map.of(GqlParams.ListParam.predList, GqlParams.JoinStyle.ANDED),
            "exhaustive shortest path",
            NotificationClassification.PERFORMANCE),
    STATUS_03N93(
            "'LOAD CSV' in combination with 'MATCH' or 'MERGE' on a label that does not have an index may result in long execution times. Consider adding an index for label { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.label},
            "no applicable index",
            NotificationClassification.PERFORMANCE),
    STATUS_03N94(
            "The query execution plan contains the 'Eager' operator. 'LOAD CSV' in combination with 'Eager' can consume a lot of memory.",
            "eager operator",
            NotificationClassification.PERFORMANCE),
    STATUS_03N95(
            "An index already exists on the relationship type or the label(s) { %s }. It is not possible to use indexes for dynamic properties. Consider using static properties.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.labelList},
            Map.of(GqlParams.ListParam.labelList, GqlParams.JoinStyle.ANDED),
            "dynamic property",
            NotificationClassification.PERFORMANCE),
    STATUS_03N96(
            "Failed to generate code, falling back to interpreted { %s } engine. A stacktrace can be found in the debug.log. Cause: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cfgSetting, GqlParams.StringParam.cause},
            "code generation failed",
            NotificationClassification.PERFORMANCE),
    STATUS_03N97(
            "The query execution plan contains a post-processing step that materialize intermediate results. This may transfer large amounts of data from the remote source and increase memory usage. Consider rewriting the query so that aggregation, `ORDER BY`, `DISTINCT`, and `LIMIT` can be pushed down to the remote source.",
            "virtual graph post-processing",
            NotificationClassification.PERFORMANCE),
    STATUS_08000("", "", ErrorClassification.UNKNOWN),
    STATUS_08007("", "transaction resolution unknown", ErrorClassification.UNKNOWN),
    STATUS_08N00(
            "Unable to connect to database { %s }. Unable to get bolt address of the leader. Check the status of the database. Retrying your request at a later time may succeed.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "unable to connect to database",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_08N01(
            "Unable to write to database { %s } on this server. Server-side routing is disabled. Either connect to the database leader directly or enable server-side routing by setting '{ %s }=true'.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.cfgSetting, List.of(CONFIG_SETTING))
            },
            "unable to write to database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N02(
            "Unable to connect to database { %s }. Server-side routing is disabled. Either connect to { %s } directly, or enable server-side routing by setting '{ %s }=true'.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.cfgSetting, List.of(CONFIG_SETTING))
            },
            "unable to route to database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N03(
            "Failed to write to graph { %s }. Check the defined access mode in both driver and database.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.graph, List.of(TOPOLOGY))},
            "failed to write to graph",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N04(
            "Routing with { %s } is not supported in embedded sessions. Connect to the database directly or try running the query using a Neo4j driver or the HTTP API.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT))
            },
            "unable to route use clause",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N05(
            "Routing administration commands is not supported in embedded sessions. Connect to the system database directly or try running the query using a Neo4j driver or the HTTP API.",
            "unable to route administration command",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N06("General network protocol error.", "protocol error", ErrorClassification.CLIENT_ERROR),
    STATUS_08N07("This member is not the leader.", "not the leader", ErrorClassification.CLIENT_ERROR),
    STATUS_08N08(
            "This database is read only on this server.", "database is read only", ErrorClassification.CLIENT_ERROR),
    STATUS_08N09(
            "The database { %s } is currently unavailable. Check the database status. Retry your request at a later time.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "database unavailable",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_08N10(
            "Message { %s } cannot be handled by session in the { %s } state.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.msg, List.of(METADATA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.boltServerState, List.of(METADATA))
            },
            "invalid server state",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N11(
            "The request is invalid and could not be processed by the server. See cause for further details.",
            "request error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N12(
            "Failed to parse the supplied bookmark. Verify it is correct or check the debug log for more information.",
            "failed to parse bookmark",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N13(
            "The database { %s } is not up to the requested bookmark { %s }. The latest transaction ID is { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.transactionId1, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.transactionId2, List.of(TOPOLOGY))
            },
            "database not up to requested bookmark",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_08N14(
            "Unable to provide a routing table for the database identifed by the alias { %s } because the request comes from another alias { %s } and alias chains are not permitted.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.alias1, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.alias2, List.of(TOPOLOGY))
            },
            "alias chains are not permitted",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N15(
            "Policy definition of the routing policy { %s } could not be found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.routingPolicy, List.of(METADATA))},
            "no such routing policy",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N16(
            "Remote execution failed with message { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            "general driver client error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_08N17(
            "Remote execution failed with message { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            "general driver transient error",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_08N18(
            "Remote execution failed with message { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            "general driver database error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_08N19(
            "Communication with shard { %s } failed with message '{ %s }'.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.graph, List.of(TOPOLOGY)), GqlParams.StringParam.msg
            },
            "shard execution transient error",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_08N20(
            "Communication with shard { %s } failed with message '{ %s }'.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.graph, List.of(TOPOLOGY)), GqlParams.StringParam.msg
            },
            "shard execution database error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_08N21(
            "Communication with shard { %s } failed with message '{ %s }'.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.graph, List.of(TOPOLOGY)), GqlParams.StringParam.msg
            },
            "shard execution client error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22000("", "", ErrorClassification.CLIENT_ERROR),
    STATUS_22001("", "string data, right truncation", ErrorClassification.UNKNOWN),
    STATUS_22003(
            "The numeric value { %s } is outside the required range.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value
            }, // StringParam since sometimes we will have ex. value > Long.MaxValue which would need to be parsed out
            // (and then back to String in the end)
            "numeric value out of range",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22004("", "null value not allowed", ErrorClassification.CLIENT_ERROR),
    STATUS_22007("", "invalid date, time, or datetime format", ErrorClassification.CLIENT_ERROR),
    STATUS_22008("", "datetime field overflow", ErrorClassification.UNKNOWN),
    STATUS_22011("", "substring error", ErrorClassification.UNKNOWN),
    STATUS_22012("", "division by zero", ErrorClassification.CLIENT_ERROR),
    STATUS_22015("", "interval field overflow", ErrorClassification.CLIENT_ERROR),
    STATUS_22018(
            "The character value { %s } is an invalid argument for the specified cast.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.value},
            "invalid character value for cast",
            ErrorClassification.UNKNOWN),
    STATUS_2201E(
            "The value { %s } is an invalid argument for the specified natural logarithm.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.value},
            "invalid argument for natural logarithm",
            ErrorClassification.UNKNOWN),
    STATUS_2201F(
            "The value { %s } is an invalid argument for the specified power function.",
            new GqlParams.GqlParam[] {GqlParams.NumberParam.value},
            "invalid argument for power function",
            ErrorClassification.UNKNOWN),
    STATUS_22027("", "trim error", ErrorClassification.UNKNOWN),
    STATUS_2202F("", "array data, right truncation", ErrorClassification.UNKNOWN),
    STATUS_22G02("", "negative limit value", ErrorClassification.UNKNOWN),
    STATUS_22G03("", "invalid value type", ErrorClassification.CLIENT_ERROR),
    STATUS_22G04("", "values not comparable", ErrorClassification.CLIENT_ERROR),
    STATUS_22G05("", "invalid date, time, or datetime function field name", ErrorClassification.CLIENT_ERROR),
    STATUS_22G06("", "invalid date, time, or datetime function field value", ErrorClassification.UNKNOWN),
    STATUS_22G07("", "invalid duration function field name", ErrorClassification.UNKNOWN),
    STATUS_22G08("", "invalid duration function field value", ErrorClassification.UNKNOWN),
    STATUS_22G0B("", "list data, right truncation", ErrorClassification.UNKNOWN),
    STATUS_22G0C("", "list element error", ErrorClassification.UNKNOWN),
    STATUS_22G0F("", "invalid number of paths or groups", ErrorClassification.UNKNOWN),
    STATUS_22G0H(
            "The duration format { %s } is invalid.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.format, List.of(TEMPORAL_SPATIAL))
            },
            "invalid duration format",
            ErrorClassification.UNKNOWN),
    STATUS_22G0I(
            "{ %s } is not a valid duration field.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.field, List.of(TEMPORAL_SPATIAL))},
            "invalid duration field",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22G0M("", "multiple assignments to a graph element property", ErrorClassification.UNKNOWN),
    STATUS_22G0N("", "number of node labels below supported minimum", ErrorClassification.UNKNOWN),
    STATUS_22G0P("", "number of node labels exceeds supported maximum", ErrorClassification.UNKNOWN),
    STATUS_22G0Q("", "number of edge labels below supported minimum", ErrorClassification.UNKNOWN),
    STATUS_22G0R("", "number of edge labels exceeds supported maximum", ErrorClassification.UNKNOWN),
    STATUS_22G0S("", "number of node properties exceeds supported maximum", ErrorClassification.UNKNOWN),
    STATUS_22G0T("", "number of edge properties exceeds supported maximum", ErrorClassification.UNKNOWN),
    STATUS_22G0U("", "record fields do not match", ErrorClassification.UNKNOWN),
    STATUS_22G0V("", "reference value, invalid base type", ErrorClassification.UNKNOWN),
    STATUS_22G0W("", "reference value, invalid constrained type", ErrorClassification.UNKNOWN),
    STATUS_22G0X("", "record data, field unassignable", ErrorClassification.UNKNOWN),
    STATUS_22G0Y("", "record data, field missing", ErrorClassification.UNKNOWN),
    STATUS_22G0Z("", "malformed path", ErrorClassification.UNKNOWN),
    STATUS_22G10("", "path data, right truncation", ErrorClassification.UNKNOWN),
    STATUS_22G11("", "reference value, referent deleted", ErrorClassification.UNKNOWN),
    STATUS_22G13("", "invalid group variable value", ErrorClassification.UNKNOWN),
    STATUS_22G14("", "incompatible temporal instant unit groups", ErrorClassification.UNKNOWN),
    STATUS_22N00(
            "The provided value is unsupported and cannot be processed.",
            "unsupported value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N01(
            "Expected the value { %s } to be of type { %s }, but was of type { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.value,
                new NonSensitiveGqlParam(GqlParams.ListParam.valueTypeList, List.of(VALUE_TYPE, METADATA, FIXED_TEXT)),
                new NonSensitiveGqlParam(
                        GqlParams.StringParam.valueType, List.of(VALUE_TYPE, METADATA, PROCEDURES_FUNCTIONS))
            },
            Map.of(GqlParams.ListParam.valueTypeList, GqlParams.JoinStyle.ORED),
            "invalid type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N02(
            "Expected { %s } to be a positive number but found { %s } instead.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.option, List.of(FIXED_TEXT, TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.value, List.of(NON_SENSITIVE_NUMBER))
            },
            "specified negative numeric value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N03(
            "Expected { %s } to be of type { %s } and in the range { %s } to { %s } but found { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.component,
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE)),
                // lower and upper are usually numbers but sometimes string representations of other types such as
                // durations
                new NonSensitiveGqlParam(GqlParams.StringParam.lower, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.StringParam.upper, List.of(NON_SENSITIVE_NUMBER)),
                GqlParams.StringParam.value
            },
            "specified numeric value out of range",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N04(
            "Invalid input { %s } for { %s }. Expected { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input,
                GqlParams.StringParam.context,
                new NonSensitiveGqlParam(
                        GqlParams.ListParam.inputList,
                        List.of(
                                FIXED_TEXT,
                                VALUE_TYPE,
                                TEMPORAL_SPATIAL,
                                NON_SENSITIVE_NUMBER,
                                CONFIG_SETTING,
                                SCHEMA,
                                PROCEDURES_FUNCTIONS,
                                CYPHER_CONSTRUCT)),
            },
            Map.of(GqlParams.ListParam.inputList, GqlParams.JoinStyle.ORED),
            "invalid input value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N05(
            "Invalid input { %s } for { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input, GqlParams.StringParam.context},
            "input failed validation",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N06(
            // See also 22NB6
            "Invalid input. { %s } needs to be specified.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.ListParam.inputList, List.of(FIXED_TEXT))},
            Map.of(GqlParams.ListParam.inputList, GqlParams.JoinStyle.ANDED),
            "required input missing",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N07(
            "Invalid pre-parser option(s): { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.optionList, List.of(CYPHER_CONSTRUCT))
            },
            Map.of(GqlParams.ListParam.optionList, GqlParams.JoinStyle.COMMAD),
            "invalid pre-parser option key",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N08(
            "Invalid pre-parser option, cannot combine { %s } with { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.option1, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.option2, List.of(CYPHER_CONSTRUCT))
            },
            "invalid pre-parser combination",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N09(
            "Invalid pre-parser option, cannot specify multiple conflicting values for { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.option, List.of(CYPHER_CONSTRUCT))
            },
            "conflicting pre-parser combination",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N10(
            "Invalid pre-parser option, specified { %s } is not valid for option { %s }. Valid options are: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.option, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.ListParam.optionList, List.of(CYPHER_CONSTRUCT))
            },
            Map.of(GqlParams.ListParam.optionList, GqlParams.JoinStyle.ANDED),
            "invalid pre-parser option value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N11(
            "Invalid argument: cannot process { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            "invalid argument",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N12(
            "Invalid argument: cannot process { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            "invalid date, time, or datetime format",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N13(
            "Specified time zones must include a date component.",
            "invalid time zone",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N14(
            "Cannot select both { %s } and { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.temporal1, List.of(TEMPORAL_SPATIAL)),
                new NonSensitiveGqlParam(GqlParams.StringParam.temporal2, List.of(TEMPORAL_SPATIAL))
            },
            "invalid temporal value combination",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N15(
            "Cannot read the specified { %s } component from { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.component, List.of(TEMPORAL_SPATIAL)),
                GqlParams.StringParam.temporal
            },
            "invalid temporal component",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N16(
            "Importing entity values to a graph with a USE clause is not supported. Attempted to import { %s } to { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.expr, List.of(CYPHER_VARIABLE)),
                new NonSensitiveGqlParam(GqlParams.StringParam.graph, List.of(TOPOLOGY))
            },
            "invalid import value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N18(
            "A { %s } POINT must contain { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.crs, List.of(TEMPORAL_SPATIAL)),
                new NonSensitiveGqlParam(GqlParams.ListParam.mapKeyList, List.of(TEMPORAL_SPATIAL))
            },
            Map.of(GqlParams.ListParam.mapKeyList, GqlParams.JoinStyle.ANDED),
            "incomplete spatial value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N19(
            "A POINT must contain either 'x' and 'y', or 'latitude' and 'longitude'.",
            "invalid spatial value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N20(
            "Cannot create POINT with { %s }D coordinate reference system (CRS) and { %s } coordinates. Use the equivalent { %s }D coordinate reference system instead.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.dim1, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.value, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.dim2, List.of(NON_SENSITIVE_NUMBER))
            },
            "invalid spatial value dimensions",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N21(
            "Unsupported coordinate reference system (CRS): { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.crs, List.of(TEMPORAL_SPATIAL)),
            },
            "unsupported coordinate reference system",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N22(
            "Cannot specify both coordinate reference system (CRS) and spatial reference identifier (SRID).",
            "invalid spatial value combination",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N23(
            "Cannot create WGS84 POINT with invalid coordinate: { %s }. The valid range for the latitude coordinate is [-90, 90].",
            new GqlParams.GqlParam[] {GqlParams.StringParam.coordinates},
            "invalid latitude value",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N24(
            "Cannot construct a { %s } from { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE)),
                GqlParams.StringParam.coordinates
            },
            "invalid coordinate arguments",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N25(
            "Cannot construct a { %s } from { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE)),
                GqlParams.StringParam.temporal
            },
            "invalid temporal arguments",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N26(
            "Unknown rounding mode. Valid values are: CEILING, FLOOR, UP, DOWN, HALF_EVEN, HALF_UP, HALF_DOWN, UNNECESSARY.",
            "unsupported rounding mode",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N27(
            "Invalid input { %s } for { %s }. Expected to be { %s }.{ %s }",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input,
                new NonSensitiveGqlParam(
                        GqlParams.StringParam.context,
                        List.of(
                                FIXED_TEXT,
                                TEMPORAL_SPATIAL,
                                CYPHER_VARIABLE,
                                CYPHER_CONSTRUCT,
                                PROCEDURES_FUNCTIONS,
                                CONFIG_SETTING,
                                VALUE_TYPE)),
                new NonSensitiveGqlParam(GqlParams.ListParam.valueTypeList, List.of(VALUE_TYPE, CONFIG_SETTING)),
                new NonSensitiveGqlParam(GqlParams.StringParam.hint, List.of(FIXED_TEXT))
            },
            Map.of(GqlParams.ListParam.valueTypeList, GqlParams.JoinStyle.ORED),
            "invalid entity type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N28(
            "The result of the operation { %s } has caused an overflow.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.operation, List.of(CYPHER_CONSTRUCT))
            },
            "overflow error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N29(
            "Unknown coordinate reference system (CRS).",
            "unknown coordinate reference system",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N30(
            "At least one temporal unit must be specified.", "missing temporal unit", ErrorClassification.CLIENT_ERROR),
    STATUS_22N31(
            "The { %s } property { %s } in { %s } is invalid. 'MERGE' cannot be used with a graph element property value that is { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.propKey, List.of(SCHEMA)),
                GqlParams.StringParam.pat,
                new NonSensitiveGqlParam(GqlParams.StringParam.value, List.of(CYPHER_CONSTRUCT)),
            },
            "invalid properties in merge pattern",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N32(
            "'ORDER BY' expressions must be deterministic.",
            "non-deterministic sort expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N33(
            "Shortest path expressions must contain start and end nodes. Cannot find: { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "invalid shortest path expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N34(
            "Cannot use { %s } function inside an aggregate function.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.funType, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid use of aggregate function",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N35(
            "Cannot parse { %s } as a DATE. Calendar dates need to be specified using the format 'YYYY-MM', while ordinal dates need to be specified using the format 'YYYY-DDD'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            "invalid date format",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N36(
            "Cannot parse { %s } as a { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input,
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE))
            },
            "invalid temporal format",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N37(
            "Cannot coerce { %s } to { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.value,
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE))
            },
            "invalid coercion",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N38(
            "Invalid argument to the function { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.value, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid function argument",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N39(
            "Value { %s } cannot be stored in properties.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value},
            "unsupported property value type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N40(
            "Cannot assign { %s } of a { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.component, List.of(TEMPORAL_SPATIAL)),
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE))
            },
            "non-assignable temporal component",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N41(
            "The 'MERGE' clause did not find a matching node { %s } and cannot create a new node due to conflicts with existing uniqueness constraints.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "merge node uniqueness constraint violation",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N42(
            "The 'MERGE' clause did not find a matching relationship { %s } and cannot create a new relationship due to conflicts with existing uniqueness constraints.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "merge relationship uniqueness constraint violation",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N43(
            "Could not load external resource from { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.url},
            "unable to load external resource",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N44(
            "Parallel runtime has been disabled, enable it or upgrade to a bigger Aura instance.",
            "parallel runtime disabled",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N46(
            "Parallel runtime does not support updating queries or a change in the state of transactions. Use another runtime.",
            "unsupported use of parallel runtime",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N47(
            "No workers are configured for the parallel runtime. Set 'server.cypher.parallel.worker_limit' to a larger value.",
            "invalid parallel runtime configuration",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N48(
            "Cannot use the specified runtime { %s } due to { %s } not being supported. Use another runtime.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.runtime, List.of(FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.cause, List.of(FIXED_TEXT))
            },
            "unable to use specified runtime",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N49(
            "Cannot read a CSV field larger than the set buffer size. Ensure the field does not have an unterminated quote, or increase the buffer size via 'dbms.import.csv.buffer_size'.",
            "CSV buffer size overflow",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N51(
            "A graph reference with the name { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "graph reference not found",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N52(
            "'PROFILE' and 'EXPLAIN' cannot be combined.",
            "invalid combination of PROFILE and EXPLAIN",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N53(
            "Cannot 'PROFILE' query before results are materialized.",
            "invalid use of PROFILE",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N54(
            "Multiple conflicting entries specified for { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.mapKey, List.of(METADATA))},
            "invalid map",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N55(
            "Map requires key { %s } but was missing from field { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.mapKey, List.of(FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.field, List.of(FIXED_TEXT))
            },
            "required key missing from map",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N56(
            "Protocol message length limit exceeded (limit: { %s }).",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.boltMsgLenLimit, List.of(NON_SENSITIVE_NUMBER))
            },
            "protocol message length limit overflow",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N57(
            "Protocol type is invalid. Invalid number of struct components (received { %s } but expected { %s }).",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.count1, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.count2, List.of(NON_SENSITIVE_NUMBER))
            },
            "invalid protocol type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N58(
            "Cannot read the specified { %s } component from { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.component, GqlParams.NumberParam.value},
            "invalid spatial component",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N59(
            "The { %s } token with id { %s } does not exist.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.tokenType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.tokenId, List.of(SCHEMA))
            },
            "token does not exist",
            ErrorClassification.DATABASE_ERROR),
    STATUS_22N60(
            "Encountered illegal { %s } element. Reason: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.item, List.of(METADATA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.msg, List.of(METADATA))
            },
            "illegal element",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N62(
            "The relationship type { %s } does not exist.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.relType, List.of(SCHEMA))},
            "relationship type does not exist",
            ErrorClassification.DATABASE_ERROR),
    STATUS_22N63(
            "The property key { %s } does not exist.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.propKey, List.of(SCHEMA))},
            "property key does not exist",
            ErrorClassification.DATABASE_ERROR),
    STATUS_22N64(
            "The constraint { %s } does not exist.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName, List.of(SCHEMA))
            },
            "constraint does not exist",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N65(
            "An equivalent constraint already exists: { %s }",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName, List.of(SCHEMA))
            },
            "equivalent constraint already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N66(
            "A conflicting constraint already exists: { %s }",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName, List.of(SCHEMA))
            },
            "conflicting constraint already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N67(
            "A constraint with the same name already exists: { %s }",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.constr, List.of(SCHEMA))},
            "duplicated constraint name",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N68(
            "Dependent constraints cannot be managed individually and must be managed together with its graph type.",
            "dependent constraint managed individually",
            ErrorClassification.DATABASE_ERROR),
    STATUS_22N69(
            "The index { %s } does not exist.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idxDescrOrName, List.of(SCHEMA))},
            "index does not exist",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N70(
            "An equivalent index already exists: { %s }",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idxDescrOrName, List.of(SCHEMA))},
            "equivalent index already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N71(
            "An index with the same name already exists: { %s }",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idx, List.of(SCHEMA))},
            "duplicated index name",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N73(
            "Constraint conflicts with already existing index { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idxDescrOrName, List.of(SCHEMA))},
            "constraint conflicts with existing index",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N74(
            "Index conflicts with already existing index owned by constraint { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.constr, List.of(SCHEMA))},
            "index conflicts with existing constraint",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N75(
            "The constraint specified by { %s } includes a label, relationship type, or property key with name { %s } more than once.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.token, List.of(SCHEMA))
            },
            "constraint contains duplicated tokens",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N76(
            "The index specified by { %s } includes a label, relationship type, or property key with name { %s } more than once.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.idxDescrOrName, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.token, List.of(SCHEMA))
            },
            "index contains duplicated tokens",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N77(
            "{ %s } ({ %s }) with { %s } { %s } must have the following properties: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.entityId, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.tokenType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.token, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.ListParam.propKeyList, List.of(SCHEMA))
            },
            Map.of(GqlParams.ListParam.propKeyList, GqlParams.JoinStyle.COMMAD),
            "property presence verification failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N78(
            "{ %s } ({ %s }) with { %s } { %s } must have the property { %s } with value type { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.entityId, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.tokenType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.token, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.propKey, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE))
            },
            "property type verification failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N79(
            "Property uniqueness constraint violated: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.reasonList},
            Map.of(GqlParams.ListParam.reasonList, GqlParams.JoinStyle.COMMAD),
            "property uniqueness constraint violated",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N80(
            "Index entry conflict: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.value},
            "index entry conflict",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N81(
            "Invalid input: { %s } is not supported in { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.exprType, List.of(FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(FIXED_TEXT, SCHEMA))
            },
            "expression type unsupported here",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N82(
            "Input { %s } contains invalid characters for { %s }. Special characters may require that the input is quoted using backticks.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input, GqlParams.StringParam.context},
            "input contains invalid characters",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N83(
            "Expected name to contain at most { %s } components separated by '.'.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.upper, List.of(NON_SENSITIVE_NUMBER))
            },
            "input consists of too many components",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N84(
            "Expected the string to be no more than { %s } characters long.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.upper, List.of(NON_SENSITIVE_NUMBER))
            },
            "string too long",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N85(
            "Expected the string to be at least { %s } characters long.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.lower, List.of(NON_SENSITIVE_NUMBER))
            },
            "string too short",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N86("Expected a nonzero number.", "numeric range 0 disallowed", ErrorClassification.CLIENT_ERROR),
    STATUS_22N87(
            "Expected a number that is zero or greater.",
            "numeric range 0 or greater allowed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N88(
            "{ %s } is not a valid CIDR IP.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            "not a valid CIDR IP",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N89(
            "Expected the new password to be different from the old password.",
            "new password cannot be the same as the old password",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N90(
            "{ %s } is not supported in property type constraints.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE))},
            "property type unsupported in constraint",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N91(
            "Failed to alter the specified database alias { %s }. Altering remote alias to a local alias or vice versa is not supported. Drop and recreate the alias instead.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.alias, List.of(TOPOLOGY))},
            "cannot convert alias local to remote or remote to local",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N92("This query requires a RETURN clause.", "missing RETURN", ErrorClassification.CLIENT_ERROR),
    STATUS_22N93("A required YIELD clause is missing.", "missing YIELD", ErrorClassification.CLIENT_ERROR),
    STATUS_22N94(
            "'YIELD *' is not supported in this context. Explicitly specify which columns to yield.",
            "invalid YIELD *",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N95(
            "Invalid JSON input. Please check the format.", "parsing JSON exception", ErrorClassification.CLIENT_ERROR),
    STATUS_22N96(
            "Unable to map the JSON input. Please verify the structure.",
            "mapping JSON exception",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N97(
            "Unexpected struct tag: { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.value, List.of(METADATA))},
            "unexpected struct tag",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N98(
            "Unable to deserialize request. Expected first field to be { %s }, but was '{ %s }'.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.field, List.of(METADATA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.value, List.of(METADATA))
            },
            "wrong first field during deserialization",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22N99(
            "Unable to deserialize request. Expected { %s }, found { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.token, List.of(METADATA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.value, List.of(METADATA))
            },
            "wrong token during deserialization",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA0(
            "Failed to administer property rule.",
            "invalid property-based access control rule",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA1(
            "The property { %s } must appear on the left hand side of the { %s } operator.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.propKey, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.operation, List.of(CYPHER_CONSTRUCT))
            },
            "invalid property-based access control rule involving non-commutative expressions",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA2(
            "The expression: { %s } is not supported. Property rules can only contain one property.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            "invalid property-based access control rule involving multiple properties",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA3(
            "'NaN' is not supported for property-based access control.",
            "invalid property-based access control rule involving NaN",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA4(
            "The property value access rule pattern { %s } always evaluates to 'NULL'.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pred},
            "invalid property-based access control rule involving comparison with NULL",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA5(
            "The property value access rule pattern { %s } always evaluates to 'NULL'. Use 'IS NULL' instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pred},
            "invalid property-based access control rule involving IS NULL",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA6(
            "The property value access rule pattern { %s } always evaluates to 'NULL'. Use 'IS NOT NULL' instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pred},
            "invalid property-based access control rule involving IS NOT NULL",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA7(
            "The expression: { %s } is not supported. Only single, literal-based predicate expressions are allowed for property-based access control.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            "invalid property-based access control rule involving nontrivial predicates",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA8(
            "Underlying error: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cause},
            "parsing JSON failure",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NA9(
            "Invalid input. Unexpected key { %s }, expected keys are { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.mapKey, List.of(CYPHER_VARIABLE)),
                new NonSensitiveGqlParam(GqlParams.ListParam.mapKeyList, List.of(FIXED_TEXT))
            },
            Map.of(GqlParams.ListParam.mapKeyList, GqlParams.JoinStyle.ORED),
            "unexpected map entry",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NAA(
            "The expression { %s } is not supported. Lists containing { %s } values are not supported for property-based access control.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.expr,
                new NonSensitiveGqlParam(GqlParams.StringParam.exprType, List.of(CYPHER_CONSTRUCT)),
            },
            "invalid list for property-based access control rule",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NAB(
            "The expression { %s } is not supported. All elements in a list must be literals of the same type for property-based access control.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            "mixed type list for property-based access control rule",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NAC(
            "Characters after an ending quote in a CSV field are not supported. See { %s } at position { %s }. This is read as { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input,
                new NonSensitiveGqlParam(GqlParams.NumberParam.pos, List.of(NON_SENSITIVE_NUMBER)),
                GqlParams.StringParam.variable
            },
            "characters after quote in CSV field",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NAD(
            "Missing end quote at position { %s } in { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.pos, List.of(NON_SENSITIVE_NUMBER)),
                GqlParams.StringParam.input
            },
            "missing end quote in CSV field",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NAE(
            "Multi-line fields are illegal in this context. Verify that there is not a missing end quote in { %s } at position { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input,
                new NonSensitiveGqlParam(GqlParams.NumberParam.pos, List.of(NON_SENSITIVE_NUMBER)),
            },
            "multi-line field in illegal CSV context",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB0(
            "The property value access rule pattern { %s } always evaluates to 'NULL'. Use `WHERE` syntax in combination with `IS NULL` instead.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.pred},
            "invalid property-based access control rule involving WHERE and IS NULL",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB1(
            "Type mismatch: expected to be { %s } but was { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.valueTypeList, List.of(VALUE_TYPE)),
                new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(VALUE_TYPE))
            },
            Map.of(GqlParams.ListParam.valueTypeList, GqlParams.JoinStyle.ORED),
            "type mismatch",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB2(
            "Graph type { %s } constraint { %s } is incompatible with graph type { %s } constraint { %s } because they have different graph type dependence.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.graphTypeDependence1, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName1, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.graphTypeDependence2, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName2, List.of(SCHEMA))
            },
            "incompatible graph type dependence",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB3(
            "{ %s } ({ %s }) with { %s } { %s } must have the { %s } { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.entityId, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.tokenType1, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.token1, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.tokenType2, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.token2, List.of(SCHEMA))
            },
            "token presence verification failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB4(
            "Relationship ({ %s }) with type { %s } requires its { %s } node ({ %s }) to have the label { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.entityId1, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.relType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.endpointType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.entityId2, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.label, List.of(SCHEMA))
            },
            "endpoint label presence verification failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB5(
            "Unknown time zone identifier { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            "unsupported time zone identifier",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB6(
            // See also 22N06
            "Invalid input. { %s } is not allowed to be an empty string.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.item, List.of(FIXED_TEXT))},
            "input empty",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB7(
            "It is not supported to create element ids on composite databases. Create the element id for { %s } { %s } on the constituent instead.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.entityId, List.of(SCHEMA))
            },
            "element id unsupported on composite database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB8(
            "{ %s } is not a recognized Neo4j type.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(VALUE_TYPE))},
            "invalid Neo4j type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NB9(
            "Lists cannot have { %s } as an inner type in this context.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.typeDescription, List.of(VALUE_TYPE))
            },
            "invalid inner list type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NBA(
            "Property type constraints for vectors need to define both coordinate type and dimension.",
            "omitting mandatory field for property type constraints for vectors",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NBC(
            "Index belongs to constraint { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName, List.of(SCHEMA))
            },
            "index belongs to constraint",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NBD(
            "Unsupported struct tag: { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.value, List.of(METADATA))},
            "unsupported struct tag",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NBE(
            "Invalid vector dimensions. The number of vector dimensions must be between { %s } and { %s }, but is { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.count1, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.count2, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.count3, List.of(NON_SENSITIVE_NUMBER))
            },
            "invalid vector dimensions",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NBF(
            "Property value of type { %s } is too big (more than { %s } bytes): { %s }",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.typeDescription, List.of(VALUE_TYPE)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.bytes, List.of(NON_SENSITIVE_NUMBER)),
                GqlParams.StringParam.value
            },
            "property value too big",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NBG(
            "Invalid vector coordinates. The vector coordinates must be finite.",
            "invalid vector coordinates",
            ErrorClassification.CLIENT_ERROR),
    // Graph Type Errors
    STATUS_22NC1(
            "The graph type element includes a property key with name { %s } more than once.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.propKey, List.of(SCHEMA))},
            "graph type element contains duplicated tokens",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NC2(
            "The node element type { %s } must contain one or more implied labels, or at least one property type.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.label, List.of(SCHEMA))},
            "node element type has no effect",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NC3(
            "The relationship element type { %s } must define a source, destination, or at least one property type.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.relType, List.of(SCHEMA))},
            "relationship element type has no effect",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NC4(
            "The label(s) { %s } are defined as both identifying and implied.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.ListParam.labelList, List.of(SCHEMA))},
            Map.of(GqlParams.ListParam.labelList, GqlParams.JoinStyle.COMMAD),
            "a label cannot be both identifying and implied",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NC5(
            "The { %s } element type referenced by { %s } does not exist.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.graphTypeReference, List.of(SCHEMA))
            },
            "graph type element not found",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NC6(
            "The independent constraint { %s } is using the same label { %s } as a node element type.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.label, List.of(SCHEMA))
            },
            "independent constraint and node element type have the same label",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NC7(
            "The independent constraint { %s } is using the same relationship type { %s } as a relationship element type.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.relType, List.of(SCHEMA))
            },
            "independent constraint and relationship element type have the same relationship type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NC8(
            "The graph type includes a label, a relationship type, or an alias with the name { %s } more than once.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.token, List.of(SCHEMA))},
            "graph type contains duplicated tokens",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NC9(
            "A { %s } element type property { %s } constraint cannot be specified inline of a { %s } element type.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA))
            },
            "invalid element type constraints in graph type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NCA(
            "A node element type identified by the label { %s } already exists in the graph type.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.label, List.of(SCHEMA))},
            "node element type already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NCB(
            "A relationship element type identified by the relationship type { %s } already exists in the graph type.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.relType, List.of(SCHEMA))},
            "relationship element type already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NCC(
            "The node element type { %s } identified by the label { %s } is different to the one specified in the query { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.graphTypeElement1, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.label, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.graphTypeElement2, List.of(SCHEMA))
            },
            "node element type specified incorrectly",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NCD(
            "The relationship element type { %s } identified by the relationship type { %s } is different to the one specified in the query { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.graphTypeElement1, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.relType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.graphTypeElement2, List.of(SCHEMA))
            },
            "relationship element type specified incorrectly",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NCE(
            "The node element type identified by the label { %s } is referenced in the graph type element { %s } and cannot be dropped.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.label, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.graphTypeReference, List.of(SCHEMA))
            },
            "node element type in use",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NCF(
            "Graph type constraint definitions are not supported in the `ALTER GRAPH TYPE { %s }` operation.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.graphTypeOperation, List.of(CYPHER_CONSTRUCT))
            },
            "graph type constraint not supported",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22NCG(
            "Expected the index { %s } to be a { %s } index but was a { %s } index.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.idx, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.idxType1, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.idxType2, List.of(SCHEMA))
            },
            "wrong index type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22ND1(
            "Invalid input: { %s } is not allowed for roles that are granted to an AUTH RULE.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.query},
            "operation not allowed for roles that are granted to an AUTH RULE",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22ND2(
            "Invalid input: { %s } is not allowed for roles with DENY privileges.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.query},
            "operation not allowed for roles with DENY privileges",
            ErrorClassification.CLIENT_ERROR),
    STATUS_22ND3(
            "The property { %s } is not an additional property for vector search with filters on the vector index { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.propKey, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.idx, List.of(SCHEMA))
            },
            "wrong property for vector search with filters",
            ErrorClassification.CLIENT_ERROR),
    STATUS_25000("", "", ErrorClassification.DATABASE_ERROR),
    STATUS_25G01("", "active GQL-transaction", ErrorClassification.UNKNOWN),
    STATUS_25G02("", "catalog and data statement mixing not supported", ErrorClassification.CLIENT_ERROR),
    STATUS_25G03("", "read-only GQL-transaction", ErrorClassification.UNKNOWN),
    STATUS_25G04("", "accessing multiple graphs not supported", ErrorClassification.UNKNOWN),
    STATUS_25N01(
            "Failed to execute the query { %s } due to conflicting statement types (read query, write query, schema modification, or administration command). To execute queries in the same transaction, they must be either of the same type, or be a combination of schema modifications and read commands.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.query},
            "invalid combination of statement types",
            ErrorClassification.CLIENT_ERROR),
    STATUS_25N02(
            "Unable to complete transaction. See debug log for details.",
            "unable to complete transaction",
            ErrorClassification.DATABASE_ERROR),
    STATUS_25N03(
            "Transaction is being used concurrently by another request.",
            "concurrent access violation",
            ErrorClassification.CLIENT_ERROR),
    STATUS_25N04(
            "Transaction { %s } does not exist.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.transactionId, List.of(TOPOLOGY))},
            "specified transaction does not exist",
            ErrorClassification.CLIENT_ERROR),
    STATUS_25N05("Transaction has been closed.", "transaction closed", ErrorClassification.CLIENT_ERROR),
    STATUS_25N06(
            "Failed to start transaction. See debug log for details.",
            "transaction start failed",
            ErrorClassification.DATABASE_ERROR),
    STATUS_25N07(
            "Failed to start constituent transaction. See debug log for details.",
            "constituent transaction start failed",
            ErrorClassification.DATABASE_ERROR),
    STATUS_25N08(
            "The lease for the transaction is no longer valid.",
            "invalid transaction lease",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_25N09(
            "The transaction failed due to an internal error.",
            "internal transaction failure",
            ErrorClassification.DATABASE_ERROR),
    STATUS_25N11(
            "There was a conflict detected between the transaction state and applied updates. Please retry the transaction.",
            "conflicting transaction state",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_25N12(
            "Index { %s } was dropped in this transaction and cannot be used.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idx, List.of(SCHEMA))},
            "index was dropped",
            ErrorClassification.CLIENT_ERROR),
    STATUS_25N13(
            "A { %s } was accessed after being deleted in this transaction. Verify the transaction statements.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA))},
            "cannot access entity after removal",
            ErrorClassification.CLIENT_ERROR),
    STATUS_25N14(
            "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Reason: { %s }",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.msg, List.of(FIXED_TEXT))},
            "transaction termination client error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_25N15(
            "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Reason: { %s }",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.msg, List.of(FIXED_TEXT))},
            "transaction termination database error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_25N16(
            "The transaction has been terminated. Retry your operation in a new transaction, and you should see a successful result. Reason: { %s }",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.msg, List.of(FIXED_TEXT, SCHEMA, NON_SENSITIVE_NUMBER))
            },
            "transaction termination transient error",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_25N17(
            "The attempted operation requires an implicit transaction.",
            "implicit transaction required",
            ErrorClassification.CLIENT_ERROR),
    STATUS_2D000("", "", ErrorClassification.UNKNOWN),
    STATUS_2DN01("Failed to commit transaction.", "commit failed", ErrorClassification.DATABASE_ERROR),
    STATUS_2DN02(
            "Failed to commit constituent transaction. See debug log for details.",
            "constituent commit failed",
            ErrorClassification.DATABASE_ERROR),
    STATUS_2DN03(
            "Failed to terminate transaction. See debug log for details.",
            "transaction termination failed",
            ErrorClassification.DATABASE_ERROR),
    STATUS_2DN04(
            "Failed to terminate constituent transaction. See debug log for details.",
            "constituent transaction termination failed",
            ErrorClassification.DATABASE_ERROR),
    STATUS_2DN05(
            "There was an error on applying the transaction. See logs for more information.",
            "failed to apply transaction",
            ErrorClassification.DATABASE_ERROR),
    STATUS_2DN06(
            "There was an error on appending the transaction. See logs for more information.",
            "failed to append transaction",
            ErrorClassification.DATABASE_ERROR),
    STATUS_2DN07(
            "Unable to commit transaction because it still have non-closed inner transactions.",
            "inner transactions still open",
            ErrorClassification.CLIENT_ERROR),
    STATUS_40000("", "", ErrorClassification.UNKNOWN),
    STATUS_40003("", "statement completion unknown", ErrorClassification.UNKNOWN),
    STATUS_40N01(
            "Failed to rollback transaction. See debug log for details.",
            "rollback failed",
            ErrorClassification.DATABASE_ERROR),
    STATUS_40N02(
            "Failed to rollback constituent transaction. See debug log for details.",
            "constituent rollback failed",
            ErrorClassification.DATABASE_ERROR),
    STATUS_42000("", "", ErrorClassification.UNKNOWN),
    STATUS_42001("", "invalid syntax", ErrorClassification.CLIENT_ERROR),
    STATUS_42004("", "use of visually confusable identifiers", ErrorClassification.UNKNOWN),
    STATUS_42006("", "number of edge labels below supported minimum", ErrorClassification.UNKNOWN),
    STATUS_42007("", "number of edge labels exceeds supported maximum", ErrorClassification.UNKNOWN),
    STATUS_42008("", "number of edge properties exceeds supported maximum", ErrorClassification.UNKNOWN),
    STATUS_42009("", "number of node labels below supported minimum", ErrorClassification.UNKNOWN),
    STATUS_42010("", "number of node labels exceeds supported maximum", ErrorClassification.UNKNOWN),
    STATUS_42011("", "number of node properties exceeds supported maximum", ErrorClassification.UNKNOWN),
    STATUS_42012("", "number of node type key labels below supported minimum", ErrorClassification.UNKNOWN),
    STATUS_42013("", "number of node type key labels exceeds supported maximum", ErrorClassification.UNKNOWN),
    STATUS_42014("", "number of edge type key labels below supported minimum", ErrorClassification.UNKNOWN),
    STATUS_42015("", "number of edge type key labels exceeds supported maximum", ErrorClassification.UNKNOWN),
    STATUS_42I00(
            "'CASE' expressions must have the same number of 'WHEN' and 'THEN' operands.",
            "invalid case expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I01(
            "Invalid use of { %s } inside 'FOREACH'.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT))
            },
            "invalid FOREACH",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I02(
            "Failed to parse comment. A comment starting with '/*' must also have a closing '*/'.",
            "invalid comment",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I03(
            "A Cypher query has to contain at least one clause.", "empty request", ErrorClassification.CLIENT_ERROR),
    STATUS_42I04(
            "{ %s } cannot be used in a { %s } clause.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.expr,
                new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT, SCHEMA))
            },
            "invalid expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I05(
            "The FIELDTERMINATOR specified for LOAD CSV can only be one character wide.",
            "invalid FIELDTERMINATOR",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I06(
            "Invalid input { %s }, expected: { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input,
                new NonSensitiveGqlParam(GqlParams.ListParam.valueList, List.of(CYPHER_CONSTRUCT, FIXED_TEXT))
            },
            Map.of(GqlParams.ListParam.valueList, GqlParams.JoinStyle.ORED),
            "invalid input",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I07(
            "The given { %s } literal { %s } is invalid.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE)),
                GqlParams.StringParam.input
            },
            "invalid integer literal",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I08(
            "The lower bound of the variable length relationship used in the { %s } function must be 0 or 1.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid lower bound",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I09(
            "Expected MAP to contain the same number of keys and values, but got keys { %s } and values { %s }.",
            new GqlParams.GqlParam[] {GqlParams.ListParam.mapKeyList, GqlParams.ListParam.valueList},
            Map.of(
                    GqlParams.ListParam.mapKeyList,
                    GqlParams.JoinStyle.ANDED,
                    GqlParams.ListParam.valueList,
                    GqlParams.JoinStyle.COMMAD),
            "invalid map",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I10(
            "Mixing label expression symbols (`|`, `&`, `!`, and `%`) with colon (`:`) between labels is not allowed. This expression could be expressed as { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.syntax, List.of(SCHEMA))},
            "invalid label expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I11(
            "A { %s } name cannot be empty or contain any null-bytes: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.tokenType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(SCHEMA))
            },
            "invalid name",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I12(
            "Quantified path patterns cannot be nested.",
            "invalid nesting of quantified path patterns",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I13(
            "The procedure or function call does not provide the required number of arguments; expected { %s } but got { %s }. The procedure or function { %s } has the signature: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.count1, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.count2, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procFun, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.sig, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid number of procedure or function arguments",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I14(
            "Exactly one relationship type must be specified for { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "invalid number of relationship types",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I15(
            "Expected exactly one statement per query but got: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.count, List.of(NON_SENSITIVE_NUMBER))
            },
            "invalid number of statements",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I16(
            "Map with keys { %s } is not a valid POINT. Use either Cartesian or geographic coordinates.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.mapKeyList, List.of(TEMPORAL_SPATIAL))
            },
            Map.of(GqlParams.ListParam.mapKeyList, GqlParams.JoinStyle.ANDED),
            "invalid point",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I17(
            "A quantifier must not have a lower bound greater than the upper bound.",
            "invalid quantifier",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I18(
            "The expression contains a non-grouping sub-expression { %s }. In an aggregating context only grouping sub-expressions and constants are allowed.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.variableList, List.of(CYPHER_VARIABLE))
            },
            Map.of(GqlParams.ListParam.variableList, GqlParams.JoinStyle.ANDED),
            "reference to non-grouping sub-expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I19(
            "Failed to parse string literal. The query must contain an even number of non-escaped quotes.",
            "invalid string literal",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I20(
            "Label expressions and relationship type expressions cannot contain { %s }. To express a label disjunction use { %s } instead.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.labelExpr, List.of(CYPHER_CONSTRUCT))
            },
            "invalid symbol in expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I21(
            "Not allowed to reference { %s } from within a parenthesized/quantified path pattern like { %s } in the same graph pattern.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.variableList, List.of(CYPHER_VARIABLE)),
                GqlParams.StringParam.pat
            },
            Map.of(GqlParams.ListParam.variableList, GqlParams.JoinStyle.COMMAD),
            "invalid reference to variable out of scope",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I22(
            "The right hand side of a UNION clause must be a single query.",
            "invalid use of UNION",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I23(
            "The { %s } function cannot contain a quantified path pattern.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid quantified path pattern in shortest path",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I24(
            "Aggregate expression { %s } is not allowed in this context.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            "invalid use of aggregate function",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I25(
            "'CALL { ... } IN TRANSACTIONS' is not supported after a write clause.",
            "invalid use of CALL IN TRANSACTIONS",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I26(
            "'DELETE ...' does not support removing labels from a node. Use 'REMOVE ...' instead.",
            "invalid DELETE",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I27(
            "`DISTINCT` cannot be used with the { %s } function.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid use of DISTINCT with non-aggregate function",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I28(
            "Importing WITH can consist only of direct references to outside variables. { %s } is not allowed.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            "invalid use of importing WITH",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I29(
            "The IS keyword cannot be used together with multiple labels in { %s }. Rewrite the expression as { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input, GqlParams.StringParam.replacement},
            "invalid use of IS",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I30(
            "Label expressions cannot be used in { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.expr, List.of(CYPHER_CONSTRUCT))},
            "invalid use of label expressions",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I32(
            "Node and relationship pattern predicates cannot be used in { %s }. They can only be used in a `MATCH` clause or inside a pattern comprehension.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(CYPHER_CONSTRUCT))
            },
            "invalid use of node and relationship pattern predicate",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I33(
            "Closed Dynamic Union types cannot be appended with 'NOT NULL', specify 'NOT NULL' on inner types instead.",
            "invalid use of NOT NULL",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I34(
            "A pattern expression can only be used to test the existence of a pattern. Use a pattern comprehension instead.",
            "invalid use of pattern expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I35(
            "Relationship type expressions can only be used in 'MATCH ...'.",
            "invalid use of relationship type expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I36(
            "'REPORT STATUS' can only be used when specifying 'ON ERROR CONTINUE' or 'ON ERROR BREAK'.",
            "invalid use of REPORT STATUS",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I37(
            "'RETURN *' is not allowed when there are no variables in scope.",
            "invalid use of RETURN *",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I38(
            "'{ %s }...' can only be used at the end of a query or subquery.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT))
            },
            "invalid position of clause",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I39(
            "Mixing the { %s } function with path selectors, explicit match modes or "
                    + "explicit path modes is not allowed.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid use of shortest path function",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I40(
            "UNION and UNION ALL cannot be combined.",
            "invalid use of UNION and UNION ALL",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I41(
            "Variable length relationships cannot be used in { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.value, List.of(FIXED_TEXT))},
            "invalid use of variable length relationship",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I42(
            "Cannot use YIELD on a call to a void procedure.",
            "invalid use of YIELD",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I43(
            "'YIELD *' can only be used with a standalone procedure call.",
            "invalid use of YIELD *",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I44(
            "Cannot use a join hint for a single node pattern.",
            "invalid joint hint",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I45(
            "Multiple path patterns cannot be used in the same clause in combination with a selective path selector.{ %s }",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.action, List.of(FIXED_TEXT))},
            "invalid use of multiple path patterns",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I46(
            "Node pattern pairs are only supported for quantified path patterns.",
            "invalid use of a node pattern pair",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I47(
            "Parser Error: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg}, "parser error", ErrorClassification.CLIENT_ERROR),
    STATUS_42I48(
            "Subqueries are not allowed in a MERGE clause.",
            "invalid use of a subquery in MERGE",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I49(
            "Unknown inequality operator '!='. The operator for inequality in Cypher is '<>'.",
            "invalid inequality operator",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I50(
            "Invalid input { %s }... A { %s } name cannot be longer than { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.tokenType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.value, List.of(NON_SENSITIVE_NUMBER))
            },
            "token name too long",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I51(
            "The procedure or function { %s } must have the signature: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procFun, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.sig, List.of(PROCEDURES_FUNCTIONS)),
            },
            "invalid call signature",
            ErrorClassification.CLIENT_ERROR),
    // Used for syntax errors for features removed in Neo4j 5.0,
    // which should have a helpful error message in Cypher 5 but the more general 42I06 in Cypher 25.
    // The full old message will be inserted as the msg parameter.
    STATUS_42I52(
            "{ %s }",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.msg, List.of(CYPHER_CONSTRUCT, TOPOLOGY, FIXED_TEXT))
            },
            "no longer valid syntax",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I53(
            "Unknown coordinate type: { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(VALUE_TYPE))},
            "unsupported coordinate type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I54(
            "`{ %s }` not allowed in `INSERT`. Use `CREATE` or { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.cause, List.of(FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.replacement, List.of(FIXED_TEXT))
            },
            "invalid use of `INSERT`",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I55(
            "Dynamic { %s } using `$any()` are not allowed in `{ %s }`.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT))
            },
            "invalid use of dynamic label or type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I56(
            "Only directed relationships are supported in `{ %s }`.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT))
            },
            "invalid relationship direction",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I57(
            "{ %s } cannot contain a query ending with { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.exprType, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT))
            },
            "invalid query ending",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I58(
            "Entity, { %s }, cannot be created and referenced in the same clause.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            "invalid entity reference",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I59(
            "Dynamic label and types are only allowed in { %s } clauses.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.clauseList, List.of(CYPHER_CONSTRUCT))
            },
            Map.of(GqlParams.ListParam.clauseList, GqlParams.JoinStyle.ANDED),
            "dynamic entity type not allowed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I60(
            "Each part of the glob (a block of text up until a dot) must either be fully escaped or not escaped at all.",
            "invalid glob escaping",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I61(
            "Missing function name for the LOOKUP INDEX.",
            "missing LOOKUP INDEX function name",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I62(
            "Unknown distance metric: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            "unsupported distance metric",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I63(
            "`ORDER BY`, `{ %s }` and `LIMIT` can only be used in this order in `RETURN`.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT))
            },
            "wrong subclause order",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I64(
            "{ %s } currently not supported after `NEXT` { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.context, GqlParams.StringParam.msg},
            "unsupported operation",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I65(
            "An invalid character is used in the pattern. Verify that all characters are supported by `{ %s }`.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE))},
            "invalid pattern character",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I66(
            "Pattern parsing failed. Make sure that an even number of escapes are used in the pattern.",
            "pattern parsing failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I67(
            "The query is parsable in `CYPHER { %s }`, but it is run in `CYPHER { %s }`. Consider changing the database default Cypher version using `ALTER DATABASE SET DEFAULT LANGUAGE` or prefix the query with `CYPHER { %s }`.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.version2, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.version1, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.version2, List.of(NON_SENSITIVE_NUMBER))
            },
            "unsupported language feature",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I68(
            "Pattern, `{ %s }`, does not match input, `{ %s }`. Verify that the pattern is valid for constructing `{ %s }`.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.input1,
                GqlParams.StringParam.input2,
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE))
            },
            "mismatched pattern",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I69(
            "{ %s } must reference a variable from the same MATCH statement.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "invalid search variable reference",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I70(
            "In order to have a search clause, a MATCH statement can only have one bound variable.",
            "search clause with multiple bound variables",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I71(
            "In order to have a search clause, a MATCH statement can only have predicates on the bound node or relationship.",
            "search clause with invalid predicates",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I72(
            "In order to have a search clause, a MATCH statement can only have a single node or relationship pattern and no selectors.",
            "search clause with too complex pattern",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I73(
            "The vector search filter predicate { %s } must consist of one or more property predicates joined by AND, and the combined property predicates for each property must specify either an exact value (e.g. x.prop = 1), a half-bounded range (e.g. x.prop >= 1), or a bounded range (e.g. x.prop > 1 AND x.prop < 100). Note that this is not an exhaustive list of valid predicates, see documentation for all rules.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.expr},
            "invalid predicate for vector search with filters",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I74(
            "The variable { %s } in a vector search filter property predicate must be the same as the search clause binding variable { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.variable1, List.of(CYPHER_VARIABLE)),
                new NonSensitiveGqlParam(GqlParams.StringParam.variable2, List.of(CYPHER_VARIABLE))
            },
            "wrong variable for vector search with filters",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I75(
            "The expression { %s } in the search clause may not depend on the search clause binding variable { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.expr, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "self-referencing in vector search",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I76(
            "The provided index or constraint { %s } { %s } ({ %s } bytes) exceeded limit of { %s } bytes.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.item, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.bytes1, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.bytes2, List.of(NON_SENSITIVE_NUMBER))
            },
            "index or constraint value too long",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I77(
            "Local callable { %s } is already defined.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS))
            },
            "local callable already defined",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I78(
            "The procedure or function is available in `CYPHER { %s }`. Consider changing the database default Cypher version using `ALTER DATABASE SET DEFAULT LANGUAGE` or prefix the query with `CYPHER { %s }`.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.version1, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.version1, List.of(NON_SENSITIVE_NUMBER))
            },
            "unsupported procedure or function in language version",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I79(
            "Aggregation in subclause expression is not allowed to reference variables declared in the same clause: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.variableList, List.of(CYPHER_VARIABLE))
            },
            Map.of(GqlParams.ListParam.variableList, GqlParams.JoinStyle.ANDED),
            "invalid reference in subclause expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42I80(
            "The grouping element { %s } is not a valid grouping key. { %s }",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.expr, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.cause, List.of(CYPHER_CONSTRUCT))
            },
            "invalid grouping element",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N00(
            "A graph reference with the name { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "graph reference not found",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N01(
            "The constituent graph { %s } was not found in the in composite database { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.graph, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))
            },
            "no such constituent graph exists in composite database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N02(
            "Writing in read access mode not allowed.",
            "writing in read access mode",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N03(
            "Writing to multiple graphs in the same transaction is not allowed. Use CALL IN TRANSACTION or create separate transactions in your application.",
            "writing to multiple graphs",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N04(
            "Failed to access database identified by { %s } while connected to session database { %s }. Connect to { %s } directly.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.db1, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db2, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db3, List.of(TOPOLOGY))
            },
            "unsupported access of composite database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N05(
            "Failed to access database identified by { %s } while connected to composite session database { %s }. Connect to { %s } directly or create an alias in the composite database.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.db1, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db2, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db3, List.of(TOPOLOGY))
            },
            "unsupported access of standard database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N06(
            "{ %s } is not supported on composite databases.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.action, List.of(CYPHER_CONSTRUCT))
            },
            "unsupported action on composite database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N07(
            "The variable { %s } is shadowing a variable with the same name from the outer scope and needs to be renamed.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "variable shadowing",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N08(
            "The procedure { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procFun, List.of(PROCEDURES_FUNCTIONS))
            },
            "no such procedure",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N09(
            "A user with the name { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.user}, "no such user", ErrorClassification.CLIENT_ERROR),
    STATUS_42N0A(
            "{ %s } is not allowed with a shard target. Target the sharded database { %s } instead of { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.action, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db1, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db2, List.of(TOPOLOGY))
            },
            "invalid shard target",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N0B(
            "The database identified by { %s } is sharded. Drop the database { %s } before recreating.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.db1, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db2, List.of(TOPOLOGY))
            },
            "cannot replace sharded database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N0C(
            "{ %s } is not allowed with a { %s } target.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.action, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.typeDescription, List.of(TOPOLOGY))
            },
            "invalid database target",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N0D(
            "The function { %s } cannot be called from the current context. It can only be used { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(FIXED_TEXT))
            },
            "cannot call function from this context",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N0E(
            "The function { %s } cannot be called without metadata.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "cannot call function without metadata",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N0F(
            "The function { %s } cannot be called without metadata for realm: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.auth, List.of(FIXED_TEXT))
            },
            "cannot call function without metadata for realm",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N10(
            "A role with the name { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.role}, "no such role", ErrorClassification.CLIENT_ERROR),
    STATUS_42N11(
            "A graph reference with the name { %s } already exists.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.ListParam.dbList, List.of(TOPOLOGY))},
            Map.of(GqlParams.ListParam.dbList, GqlParams.JoinStyle.ORED),
            "graph reference already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N12(
            "A user with the name { %s } already exists.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.user},
            "user already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N13(
            "A role with the name { %s } already exists.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.role},
            "role already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N14(
            "{ %s } cannot be used together with { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.cmd, List.of(CYPHER_CONSTRUCT))
            },
            "invalid use of command",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N15(
            "{ %s } is a reserved keyword and cannot be used in this place.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.syntax, List.of(FIXED_TEXT))},
            "invalid use of reserved keyword",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N16(
            "Only single property { %s } are supported.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idxType, List.of(SCHEMA))},
            "unsupported index or constraint",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N17(
            "{ %s } is not allowed on the system database.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.input},
            "unsupported request",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N18("The database is in read-only mode.", "read-only database", ErrorClassification.CLIENT_ERROR),
    STATUS_42N19(
            "Duplicate { %s } clause.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.syntax, List.of(CYPHER_CONSTRUCT))
            },
            "duplicate clause",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N20(
            "The list range operator '[ ]' cannot be empty.",
            "empty list range operator",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N21(
            "Expression in { %s } must be aliased (use AS).",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(CYPHER_CONSTRUCT))
            },
            "unaliased return item",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N22(
            "A COLLECT subquery must end with a single return column.",
            "single return column required",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N23(
            "The aggregating function must be included in the { %s } clause for use in 'ORDER BY ...'.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT))
            },
            "missing reference to aggregation function",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N24(
            "A WITH clause is required between { %s } and { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.input1, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.input2, List.of(CYPHER_CONSTRUCT))
            },
            "missing WITH",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N25(
            "Procedure call inside a query does not support naming results implicitly. Use YIELD instead. Available output columns are: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.variableList, List.of(PROCEDURES_FUNCTIONS))
            },
            Map.of(GqlParams.ListParam.variableList, GqlParams.JoinStyle.ANDED),
            "missing YIELD",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N26(
            "Multiple join hints for the same variable { %s } are not supported.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "multiple join hints on same variable",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N28(
            "Only statically inferrable patterns and variables are allowed in { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(CYPHER_CONSTRUCT))},
            "patterns or variables not statically inferrable",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N29(
            "Pattern expressions are not allowed to introduce new variables: { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "unbound variables in pattern expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N31(
            "Expected { %s } to be { %s } in the range { %s } to { %s } but found { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.component, List.of(FIXED_TEXT, CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.lower, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.upper, List.of(NON_SENSITIVE_NUMBER)),
                GqlParams.StringParam.value
            },
            "specified number out of range",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N32(
            "Parameter maps cannot be used in { %s } patterns. Use a literal map instead.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.keyword, List.of(CYPHER_CONSTRUCT))
            },
            "invalid use of parameter map",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N34(
            "Path cannot be bound in a quantified path pattern.",
            "path bound in quantified path pattern",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N35(
            "The path selector or explicit path mode { %s } is not supported within quantified or parenthesized path patterns.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.selectorOrPathMode, List.of(CYPHER_CONSTRUCT))
            },
            "unsupported path selector or explicit path mode in path pattern",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N36(
            "Procedure call is missing parentheses.",
            "procedure call without parentheses",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N37(
            "Relationship pattern predicates cannot be use in variable length relationships.",
            "invalid use of relationship pattern predicates in variable length relationships",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N38(
            "Return items must have unique names.", "duplicate return item name", ErrorClassification.CLIENT_ERROR),
    STATUS_42N39(
            "All { %s } must have the same return column names. Use `AS` to ensure columns have the same name.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(FIXED_TEXT))},
            "incompatible return column names",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N3A(
            "All { %s } need to either return rows or update the graph.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(FIXED_TEXT))},
            "incompatible conditional query",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N3B(
            "All { %s } must return the same number of columns.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(FIXED_TEXT))},
            "incompatible number of return columns",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N3C(
            "Not possible to enclose { %s } in 'CALL { ... }'. Use 'CALL () { ... }' instead.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(FIXED_TEXT))},
            "invalid use of CALL { ... }",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N40(
            "The { %s } function must contain one relationship pattern.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "single relationship pattern required",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N41(
            "The reduce function requires a '| expression' after the accumulator.",
            "missing |-expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N42(
            "Sub-path assignment is not supported.", "unsupported sub-path binding", ErrorClassification.CLIENT_ERROR),
    STATUS_42N44(
            "It is not possible to access the variable { %s } declared before the { %s } clause when using { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE)),
                new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.groupingConstructs, List.of(CYPHER_CONSTRUCT))
            },
            "inaccessible variable",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N45(
            "Unexpected end of input, expected 'CYPHER', 'EXPLAIN', 'PROFILE' or a query.",
            "unexpected end of input",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N46( // Duplicate of 42N52, use that instead
            "{ %s } is not a recognized Cypher type.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(VALUE_TYPE))},
            "unexpected type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N47(
            "'CALL { ... } IN TRANSACTIONS' is not supported in '... UNION ...'.",
            "invalid use of UNION and CALL IN TRANSACTIONS",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N48(
            "The function { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "no such function",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N49(
            "Unknown Normal Form: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(PROCEDURES_FUNCTIONS))
            },
            "unsupported normal form",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N50(
            "The procedure return column { %s } is not defined for this procedure. Available output columns are: { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.arg, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.ListParam.argList, List.of(PROCEDURES_FUNCTIONS))
            },
            Map.of(GqlParams.ListParam.argList, GqlParams.JoinStyle.ANDED),
            "procedure return column not defined",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N51(
            "Invalid parameter { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.param, List.of(CYPHER_CONSTRUCT))},
            "invalid parameter",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N52(
            "{ %s } is not a recognized Cypher type.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(VALUE_TYPE))},
            "invalid value type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N53(
            "The quantified path pattern may yield an infinite number of rows under match mode 'REPEATABLE ELEMENTS'. Add an upper bound to the quantified path pattern.",
            "unsafe usage of repeatable elements",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N54(
            "The match mode { %s } is not supported.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.matchMode, List.of(CYPHER_CONSTRUCT))
            },
            "unsupported match mode",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N55(
            "The path selector { %s } is not supported.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.selector, List.of(CYPHER_CONSTRUCT))
            },
            "unsupported path selector",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N56(
            "Properties are not supported in the { %s } function.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "unsupported use of properties",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N57(
            "{ %s } cannot contain any updating clauses.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.expr, List.of(CYPHER_CONSTRUCT))},
            "invalid use of data-modifications in expressions",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N58(
            "Nested 'CALL { ... } IN TRANSACTIONS is not supported.",
            "unsupported use of nesting",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N59(
            "Variable { %s } already declared.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "variable already defined",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N60(
            "REPEATABLE ELEMENTS with { %s } path mode is not supported.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.pathMode, List.of(CYPHER_CONSTRUCT))
            },
            "unsupported combination of match mode and path mode",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N61(
            "Mixing { %s } in the same graph pattern is not supported. Split the pattern into separate MATCH clauses instead.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.ListParam.pathModes, List.of(CYPHER_CONSTRUCT))
            },
            Map.of(GqlParams.ListParam.pathModes, GqlParams.JoinStyle.ANDED),
            "unsupported mixing of different path modes",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N62(
            "Variable { %s } not defined.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "variable not defined",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N63(
            "All inner types in a Closed Dynamic Union must be nullable, or be appended with 'NOT NULL'.",
            "inner type with different nullability",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N64(
            "A quantified or parenthesized path pattern must have at least one node or relationship pattern.",
            "at least one node or relationship required",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N65(
            "The { %s } function requires bound node variables when it is not part of a 'MATCH ...'.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "node variable not bound",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N66(
            "Bound relationships are not allowed in calls to the { %s } function.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "relationship variable already bound",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N67(
            "Duplicate parameter { %s } in local callable signature.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procParam, List.of(PROCEDURES_FUNCTIONS))
            },
            "duplicate parameter",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N68(
            "Variables cannot be defined more than once in a { %s } clause.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT))
            },
            "duplicate variable definition",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N69(
            "The { %s } function is only allowed as a top-level element and not inside an { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.expr, List.of(CYPHER_CONSTRUCT))
            },
            "function not allowed inside expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N70(
            "The function { %s } requires a WHERE clause.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "function without required WHERE clause",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N71(
            "A query must conclude with a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call without a YIELD clause.",
            "incomplete query",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N72(
            "Calling graph functions is only supported on composite databases. Use the name directly or connect to a composite database with the desired constituents.",
            "graph function only supported on composite databases",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N73(
            "The USE clause must be the first clause of a query or an operand to '... UNION ...' . In a CALL sub-query, it can also be the second clause if the first clause is an importing WITH.",
            "invalid placement of USE clause",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N74(
            "Failed to access { %s } and { %s }. Child USE clauses must target the same graph as their parent query. Run in separate (sub)queries instead.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.db1, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db2, List.of(TOPOLOGY))
            },
            "invalid nested USE clause",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N75(
            "A call to the graph function { %s } is only allowed as the top-level argument of a USE clause.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid use of graph function",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N76(
            "The hint(s) { %s } cannot be fulfilled.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.hintList, List.of(CYPHER_VARIABLE, SCHEMA))
            },
            Map.of(GqlParams.ListParam.hintList, GqlParams.JoinStyle.ANDED),
            "unfulfillable hints",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N77(
            "The hint { %s } cannot be fulfilled. The query does not contain a compatible predicate for { %s } on { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.hint, List.of(CYPHER_VARIABLE, SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.entityType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE))
            },
            "missing hint predicate",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N78(
            "Node { %s } has already been bound and cannot be modified by the { %s } clause.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.variable, List.of(CYPHER_VARIABLE)),
                new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT))
            },
            "variable already bound",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N79(
            "The USE clause is not required for administration commands. Retry the query without the USE clause, and it will be routed automatically.",
            "invalid USE clause",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N7A(
            "'DISJOINT BY' can only be used in 'CALL { ... } IN CONCURRENT TRANSACTIONS'.",
            "disjoint by requires concurrent transactions",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N7B(
            "'DISJOINT BY' expressions must be deterministic.",
            "non-deterministic disjoint by expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N7C(
            "'DISJOINT BY' expressions must not contain subquery expressions.",
            "subquery in disjoint by expression",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N81(
            "Expected { %s }, but got { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.paramList1, List.of(CYPHER_VARIABLE)),
                new NonSensitiveGqlParam(GqlParams.ListParam.paramList2, List.of(CYPHER_VARIABLE))
            },
            Map.of(
                    GqlParams.ListParam.paramList1,
                    GqlParams.JoinStyle.ANDED,
                    GqlParams.ListParam.paramList2,
                    GqlParams.JoinStyle.ANDED),
            "missing request parameter",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N82(
            "The database identified by { %s } has one or more aliases. Drop the aliases { %s } before dropping the database.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.ListParam.aliasList, List.of(TOPOLOGY))
            },
            Map.of(GqlParams.ListParam.aliasList, GqlParams.JoinStyle.ANDED),
            "cannot drop database with aliases",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N83(
            "Cannot impersonate a user while password change required.",
            "impersonation disallowed while password change required",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N84(
            "WHERE clause without YIELD clause. Use 'TERMINATE TRANSACTION ... YIELD ... WHERE ...'.",
            "TERMINATE TRANSACTION misses YIELD clause",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N85(
            "Allowed and denied database options are mutually exclusive.",
            "cannot specify both allowed and denied databases",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N86(
            "{ %s } failed. Parameterized database and graph names do not support wildcards.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.syntax},
            "wildcard in parameter",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N87(
            "The database or alias name { %s } conflicts with the name { %s } of an existing database or alias.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.db1, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.db2, List.of(TOPOLOGY))
            },
            "database or alias with similar name exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N89(
            "Failed evaluating the given driver settings. { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cause},
            "invalid driver settings map",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N90(
            "Composite databases cannot be altered (database: { %s }).",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "cannot alter immutable composite database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N91(
            // Never used, this error scenario is unreachable
            "Cannot index nested properties (property: { %s }).",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.propKey, List.of(SCHEMA))},
            "cannot index nested property",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N92(
            "Cannot combine old and new auth syntax for the same auth provider.",
            "cannot combine old and new auth provider syntax",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N93(
            // Never used, 22N06 was used instead
            "No auth given for user.", "missing auth clause", ErrorClassification.CLIENT_ERROR),
    STATUS_42N94(
            "'ALTER USER' requires at least one clause.",
            "incomplete ALTER USER command",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N95(
            "The combination of provider and id is already in use.",
            "provider-id combination already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N96(
            "User has no auth provider. Add at least one auth provider for the user or consider suspending them.",
            "invalid user configuration",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N97(
            "Clause { %s } is mandatory for auth provider { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.clause, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.auth, List.of(FIXED_TEXT))
            },
            "missing mandatory auth clause",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N98(
            "Cannot modify the user record of the current user.",
            "cannot modify own user",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42N99(
            "Cannot delete the user record of the current user.",
            "cannot delete own user",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA0(
            "Query contains operations that must be executed on the constituent.",
            "operations must be executed on constituent",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA1(
            "Graph access operations are not supported on composite databases.",
            "graph access operations on composite database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA2(
            "Database operations are not supported on composite databases.",
            "database operations on composite database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA3(
            "Schema operations are not supported on composite databases.",
            "schema operations on composite database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA4(
            "Transaction operations are not supported on composite databases.",
            "transaction operations on composite database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA5(
            "Accessing multiple graphs in the same query is only supported on composite databases. Connect to a composite database with the desired constituents.",
            "accessing multiple graphs only supported on composite databases",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA6(
            "Aliases are not allowed to target composite databases.",
            "invalid alias target",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA7(
            "No database is corresponding to { %s }. Verify that the elementId is correct.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "referenced database not found",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA8(
            "Invalid reference in command { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.cmd},
            "invalid reference in command",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NA9(
            "The system database supports a restricted set of Cypher clauses. The supported clauses include procedure calls (if the procedure is allowed), a subset of show and terminate commands, and combinations of the two. 'YIELD' and 'RETURN' are also permitted when combined with procedure calls, show, or terminate commands.",
            "system database rules",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAA(
            "Incorrectly formatted graph reference { %s }. Expected a single quoted or unquoted identifier. Separate name parts should not be quoted individually.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(TOPOLOGY))},
            "incorrectly formatted graph reference",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAB(
            "WHERE is not supported in a standalone call. Use `CALL ... YIELD ... WHERE ... RETURN ...` instead.",
            "not supported standalone call",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAC(
            "The backup metadata script contains an invalid or missing CREATE DATABASE statement. The metadata script must contain exactly one CREATE DATABASE statement and it must use the parameter $database.",
            "invalid CREATE DATABASE statement",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAD(
            "An auth rule with the name { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.authRule, List.of(CYPHER_VARIABLE))
            },
            "no such auth rule",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAE(
            "An auth rule with the name { %s } already exists.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.authRule, List.of(CYPHER_VARIABLE))
            },
            "auth rule already exists",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAF(
            "USE clause is not supported in local procedure definitions.",
            "not supported local procedure definition",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAG(
            "USE clause is not supported in local function definitions.",
            "not supported local function definition",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAH(
            "Return column { %s } does not match output signature of local procedure { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.ident, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS))
            },
            "return column error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAI(
            "Return column { %s } is missing to match output signature of local procedure { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.ident, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS))
            },
            "missing return column",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAJ(
            "{ %s } is not supported as local procedure output type. Adjust the type of output field { %s } of local procedure { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.typeDescription, List.of(VALUE_TYPE)),
                new NonSensitiveGqlParam(GqlParams.StringParam.ident, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS))
            },
            "not supported local procedure output type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAK(
            "{ %s } is not supported as local function return type. Adjust the return type of local function { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.typeDescription, List.of(VALUE_TYPE)),
                new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "not supported local function return type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAL(
            "{ %s } is not supported as local callable parameter type. Adjust the type of parameter { %s } of local callable { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.typeDescription, List.of(VALUE_TYPE)),
                new NonSensitiveGqlParam(GqlParams.StringParam.ident, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS))
            },
            "not supported local callable parameter type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAM(
            "{ %s } cannot be used in auth rule conditions as it retrieves the current time. Only transaction start time is available at the time of auth rule evaluation. Use { %s } instead.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.input1, List.of(CYPHER_CONSTRUCT))
            },
            "unsupported temporal function form in auth rule condition",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAN(
            "Non-scalar query result not supported in local function definitions. "
                    + "Query in local function definitions { %s } requires a `RETURN` clause with a single column and computing a total aggregate or containing `LIMIT 1`.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "non-scalar query result in local function definition",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NAO(
            "'CALL { ... } IN TRANSACTIONS' is not supported in combination with 'DEFINE'.",
            "invalid use of CALL IN TRANSACTIONS and local callables",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NFC(
            "Authentication and/or authorization could not be validated. See security logs for details.",
            "auth info validation error",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_42NFD(
            "Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.",
            "credentials expired",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NFE(
            "Authentication and/or authorization info expired.", "auth info expired", ErrorClassification.CLIENT_ERROR),
    STATUS_42NFF(
            "Access denied, see the security logs for details.",
            "permission/access denied",
            ErrorClassification.CLIENT_ERROR),
    // Start reserved block: 42NG0 - 42NGX
    // ###################################
    STATUS_42NG0("Unsupported syntax.", "unsupported syntax", ErrorClassification.CLIENT_ERROR),
    STATUS_42NG1(
            "Unsupported syntax: { %s }.",
            new GqlParams.GqlParam[] {GqlParams.StringParam.syntax},
            "unsupported syntax",
            ErrorClassification.CLIENT_ERROR),
    STATUS_42NGA("The query has no valid solution.", "bad query", ErrorClassification.CLIENT_ERROR),
    // End reserved block: 42NG0 - 42NGX
    // #################################
    STATUS_50N00(
            "Internal exception raised { %s }: { %s }",
            new GqlParams.GqlParam[] {
                /*
                 * The convention is that msgTitle is the class in the codebase which threw the internal error (this.getClass.getSimpleName)
                 * In some instances it can also be an Exception class or a hardcoded text.
                 */
                new NonSensitiveGqlParam(GqlParams.StringParam.msgTitle, List.of(METADATA, FIXED_TEXT)),
                GqlParams.StringParam.msg
            },
            "internal error",
            ErrorClassification.UNKNOWN),
    STATUS_50N01(
            "Remote execution by { %s } raised { %s }: { %s }",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.server, List.of(TOPOLOGY)),
                GqlParams.StringParam.msgTitle,
                GqlParams.StringParam.msg
            },
            "remote execution error",
            ErrorClassification.UNKNOWN),
    STATUS_50N05(
            "Deadlock detected while trying to acquire locks. See log for more details.",
            "deadlock detected",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_50N06(
            "Remote execution failed. See cause for more details.",
            "remote execution client error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N07(
            "Execution failed. See cause and debug log for details.",
            "execution failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N09(
            "The server transitioned into a server state that is not valid in the current context: { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.boltServerState, List.of(METADATA))
            },
            "invalid server state transition",
            ErrorClassification.DATABASE_ERROR),
    STATUS_50N10(
            "Unable to drop { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idxDescrOrName, List.of(SCHEMA))},
            "index drop failed",
            ErrorClassification.DATABASE_ERROR),
    STATUS_50N11(
            "Unable to create { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName, List.of(SCHEMA))
            },
            "constraint creation failed",
            ErrorClassification.DATABASE_ERROR),
    STATUS_50N12(
            "Unable to drop { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName, List.of(SCHEMA))
            },
            "constraint drop failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N13(
            "Unable to validate constraint { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.constrDescrOrName, List.of(SCHEMA))
            },
            "constraint validation error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N14(
            "A constraint imposed by the database was violated.",
            "constraint violation",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N15(
            "The system attempted to execute an unsupported operation on index { %s }. See debug.log for more information.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idx, List.of(SCHEMA))},
            "unsupported index operation",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N16(
            "Remote execution failed. See cause for more details.",
            "remote execution transient error",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_50N17(
            "Remote execution failed. See cause for more details.",
            "remote execution database error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_50N18(
            "Communication with shard { %s } failed. See cause for more details.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.graph, List.of(TOPOLOGY))},
            "shard execution transient error",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_50N19(
            "Communication with shard { %s } failed. See cause for more details.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.graph, List.of(TOPOLOGY))},
            "shard execution database error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_50N1A(
            "Failed to retrieve all shard replica locations of { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.graph, List.of(TOPOLOGY))},
            "failed to retrieve all shard replica locations",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_50N20(
            "Communication with shard { %s } failed. See cause for more details.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.graph, List.of(TOPOLOGY))},
            "shard execution client error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N21(
            "The { %s } was not found for { %s }. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.schemaDescrType, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.StringParam.schemaDescr, List.of(SCHEMA))
            },
            "no such schema descriptor",
            ErrorClassification.DATABASE_ERROR),
    STATUS_50N22(
            "Failed to parse { %s } as a graph name. Graph name parts that contain unsupported characters for unescaped identifiers require backtick escaping. Graph name parts with special characters may require additional escaping of those characters.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "invalid graph name",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N23(
            "Transaction retry aborted after { %s } attempts. Retry timed out with a maximum retry duration of { %s } { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.count, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.timeAmount, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.StringParam.timeUnit, List.of(TEMPORAL_SPATIAL))
            },
            "transaction retry aborted",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_50N24(
            "Unexpected exception while getting transaction state.",
            "sharded properties transaction handling database error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_50N25(
            "Unexpected exception while getting transaction state.",
            "sharded properties transaction handling client error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N26(
            "The backup metadata script contains invalid syntax.",
            "invalid backup metadata script",
            ErrorClassification.CLIENT_ERROR),
    STATUS_50N27(
            "The transaction read outdated data and cannot be recovered due to concurrent data modification. Retry the transaction.",
            "outdated read",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_50N42(
            "Unexpected error has occurred. See debug log for details.",
            "unexpected error",
            ErrorClassification.UNKNOWN),
    STATUS_51N00(
            "Failed to register procedure/function.", "procedure registration error", ErrorClassification.CLIENT_ERROR),
    STATUS_51N01(
            "The field { %s } in the class { %s } is annotated as a '@Context' field, but it is declared as static. '@Context' fields must be public, non-final and non-static.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procField, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procClass, List.of(PROCEDURES_FUNCTIONS))
            },
            "class field annotation should be public, non-final, and non-static",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N02(
            "Unable to set up injection for procedure { %s }. The field { %s } has type { %s } which is not a supported injectable component.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procClass, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procField, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procFieldType, List.of(PROCEDURES_FUNCTIONS))
            },
            "unsupported injectable component type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N03(
            "Unable to set up injection for { %s }, failed to access field { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procClass, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procField, List.of(PROCEDURES_FUNCTIONS))
            },
            "unable to access field",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N04(
            "The field { %s } on { %s } must be annotated as a '@Context' field in order to store its state.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procField, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procClass, List.of(PROCEDURES_FUNCTIONS))
            },
            "missing class field annotation",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N05(
            "The field { %s } on { %s } must be declared non-final and public.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procField, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procClass, List.of(PROCEDURES_FUNCTIONS))
            },
            "class field should be public and non-final",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N06(
            "The argument at position { %s } in { %s } requires a '@Name' annotation and a non-empty name.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.pos, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procMethod, List.of(PROCEDURES_FUNCTIONS))
            },
            "missing argument name",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N07(
            "The { %s } contains a non-default argument after a default argument. Non-default arguments are not allowed to be positioned after default arguments.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procFun, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid ordering of default arguments",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N08(
            "The class { %s } must contain exactly one '@UserAggregationResult' method and exactly one '@UserAggregationUpdate' method.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procClass, List.of(PROCEDURES_FUNCTIONS))
            },
            "exactly one @UserAggregationResult method and one @UserAggregationUpdate method required",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N09(
            "The '@UserAggregationUpdate' method { %s } of { %s } must be public and have the return type 'void'.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procMethod, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procClass, List.of(PROCEDURES_FUNCTIONS))
            },
            "@UserAggregationUpdate method must be public and void",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N10(
            "The method { %s } of { %s } must be public.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procMethod, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procClass, List.of(PROCEDURES_FUNCTIONS))
            },
            "aggregation method not public",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N11(
            "The class { %s } must be public.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procClass, List.of(PROCEDURES_FUNCTIONS))
            },
            "class not public",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N12(
            "The procedure { %s } has zero return columns and must be defined as void.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS))
            },
            "class not void",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N13(
            "Unable to register the procedure or function { %s } because the name is already in use.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procFun, List.of(PROCEDURES_FUNCTIONS))
            },
            "procedure or function name already in use",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N14(
            "The procedure { %s } has a duplicate { %s } field, { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procFieldType, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procField, List.of(PROCEDURES_FUNCTIONS))
            },
            "duplicate field name",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N15(
            "Type mismatch for map key. Required 'STRING', but found { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE))},
            "invalid map key type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N16(
            "Type mismatch for the default value. Required { %s }, but found { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.valueType, List.of(VALUE_TYPE)),
                new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid default value type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N17(
            "Procedures and functions cannot be defined in the root namespace, or use a reserved namespace. Use the package name instead (e.g., org.example.com.{ %s }).",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procFun, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid procedure or function name",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N18(
            "The method { %s } has an invalid return type. Procedures must return a stream of records, where each record is of a defined concrete class.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procMethod, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid method return type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N20(
            "The field { %s } is not injectable. Ensure the field is marked as public and non-final.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procField, List.of(PROCEDURES_FUNCTIONS))
            },
            "cannot inject field",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N21(
            "The procedure registration failed because the procedure registry was busy. Try again.",
            "procedure registry is busy",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N22(
            "Finding the shortest path for the given pattern requires an exhaustive search. To enable exhaustive searches, set 'cypher.forbid_exhaustive_shortestpath' to false.",
            "exhaustive shortest path search disabled",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N23(
            "Cannot find the shortest path when the start and end nodes are the same. To enable this behavior, set 'dbms.cypher.forbid_shortestpath_common_nodes' to false.",
            "cyclic shortest path search disabled",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N24(
            "Could not find a query plan within given time and space limits.",
            "insufficient resources for plan search",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N25(
            "Cannot compile query due to excessive updates to indexes and constraints.",
            "database is busy",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N26(
            "{ %s } is not available. This implementation of Cypher does not support { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.item, List.of(FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.feat, List.of(FIXED_TEXT))
            },
            "not supported in this version",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N27(
            "{ %s } is not supported in { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.feat, List.of(SCHEMA, CYPHER_CONSTRUCT, FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.edition, List.of(FIXED_TEXT))
            },
            "not supported in this edition",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N28(
            "This Cypher command must be executed against the database { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "not supported by this database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N29(
            "The command { %s } must be executed on the current 'LEADER' server.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.cmd, List.of(CYPHER_CONSTRUCT))},
            "not supported by this server",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N2A(
            "The command { %s } is not available with auth disabled.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.cmd, List.of(CYPHER_CONSTRUCT))},
            "not supported with auth disabled",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N30(
            "{ %s } is not supported in { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.item, List.of(FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(FIXED_TEXT))
            },
            "not supported with this configuration",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N31(
            "{ %s } is not supported in { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(
                        GqlParams.StringParam.feat, List.of(CYPHER_CONSTRUCT, VALUE_TYPE, SCHEMA, FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(FIXED_TEXT))
            },
            "not supported",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N32("Server is in panic.", "server panic", ErrorClassification.DATABASE_ERROR),
    STATUS_51N33(
            "This member failed to replicate transaction, try again.",
            "replication error",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N34(
            "Failed to write to the database due to a cluster leader change. Retrying your request at a later time may succeed.",
            "write transaction failed due to leader change",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N35(
            "The location of { %s } has changed while the transaction was running.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "database location changed",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N36(
            "There is not enough memory to perform the current task.",
            "out of memory",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N37(
            "There is not enough stack size to perform the current task.",
            "stack overflow",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N38(
            "There are insufficient threads available for executing the current task.",
            "failed to acquire execution thread",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N39(
            "Expected set of files not found on disk. Please restore from backup.",
            "raft log corrupted",
            ErrorClassification.DATABASE_ERROR),
    STATUS_51N40(
            "Database { %s } failed to start. Try restarting it.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "unable to start database",
            ErrorClassification.DATABASE_ERROR),
    STATUS_51N41(
            "Server or database admin operation not possible. Reason: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            "admin operation failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N42(
            "Unable to check if allocator { %s } is available.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.alloc, List.of(TOPOLOGY))},
            "allocator availability check failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N43(
            "Cannot deallocate server(s) { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.ListParam.serverList, List.of(TOPOLOGY))},
            Map.of(GqlParams.ListParam.serverList, GqlParams.JoinStyle.ANDED),
            "cannot deallocate servers",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N44(
            "Cannot drop server { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.server, List.of(TOPOLOGY))},
            "cannot drop server",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N45(
            "Cannot cordon server { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.server, List.of(TOPOLOGY))},
            "cannot cordon server",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N46(
            "Cannot alter server { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.server, List.of(TOPOLOGY))},
            "cannot alter server",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N47(
            "Cannot rename server { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.server, List.of(TOPOLOGY))},
            "cannot rename server",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N48(
            "Cannot enable server { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.server, List.of(TOPOLOGY))},
            "cannot enable server",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N49(
            "Cannot alter database { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "cannot alter database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N50(
            "Cannot recreate database { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "cannot recreate database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N51(
            "Cannot create database { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "cannot create database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N52(
            "Cannot { %s } database topology. Number of primaries { %s } needs to be at least 1 and may not exceed { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.action, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.count, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.upper, List.of(NON_SENSITIVE_NUMBER))
            },
            "number primaries out of range",
            ErrorClassification.CLIENT_ERROR) // message sounds a little off for me for these two. maybe "numbe rof
    // primaries/secondaries ({ %s }) instead?
    ,
    STATUS_51N53(
            "Cannot { %s } database topology. Number of secondaries { %s } needs to be at least 0 and may not exceed { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.action, List.of(CYPHER_CONSTRUCT)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.count, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.upper, List.of(NON_SENSITIVE_NUMBER))
            },
            "number secondaries out of range",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N54(
            "Failed to calculate reallocation for databases. { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            "cannot reallocate",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N55(
            "Failed to create the database { %s }. The limit of databases is reached. Either increase the limit using the config setting { %s } or drop a database.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.cfgSetting, List.of(CONFIG_SETTING))
            },
            "cannot create additional database",
            ErrorClassification.DATABASE_ERROR),
    STATUS_51N56(
            "The number of { %s } seeding servers { %s } is larger than the desired number of { %s } allocations { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.serverType, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.count1, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.StringParam.allocType, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.count2, List.of(NON_SENSITIVE_NUMBER))
            },
            "topology out of range",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N57(
            "Unexpected error while picking allocations. { %s }",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.msg, List.of(FIXED_TEXT))},
            "generic topology modification error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_51N58(
            "Invalid database shard topology. The number of { %s } { %s } needs to be at least 1 and may not exceed { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.context, List.of(FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.count, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.upper, List.of(NON_SENSITIVE_NUMBER))
            },
            "invalid shard topology",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N59(
            "The DBMS is unable to handle the request, please retry later or contact the system operator. More information is present in the logs.",
            "internal resource exhaustion",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N60(
            "The DBMS is unable to determine the enterprise license acceptance status.",
            "unable to check enterprise license acceptance",
            ErrorClassification.DATABASE_ERROR),
    STATUS_51N61(
            "Index { %s } population failed.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idx, List.of(SCHEMA))},
            "index population failed",
            ErrorClassification.DATABASE_ERROR),
    STATUS_51N62(
            "Unable to use index { %s } because it is in a failed state. See logs for more information.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idx, List.of(SCHEMA))},
            "index is in a failed state",
            ErrorClassification.DATABASE_ERROR),
    STATUS_51N63(
            "Index { %s } is not ready yet. Wait until it finishes populating and retry the transaction.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.idx, List.of(SCHEMA))},
            "index is still populating",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N64("The index dropped while sampling.", "index dropped while sampling", ErrorClassification.CLIENT_ERROR),
    STATUS_51N65(
            "Vector index { %s } has a configured dimensionality of { %s }, but the provided vector has a dimension { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.idx, List.of(SCHEMA)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.dim1, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.dim2, List.of(NON_SENSITIVE_NUMBER))
            },
            "vector index dimensionality mismatch",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N66(
            "Insufficient resources to complete the request.", "resource exhaustion", ErrorClassification.CLIENT_ERROR),
    STATUS_51N67(
            "Unexpected CDC selector { %s } at { %s }, expected selector to be a { %s } selector.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.selectorType1, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.input, List.of(FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.selectorType2, List.of(FIXED_TEXT))
            },
            "invalid CDC selector type",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N68(
            "Change Data Capture is not currently enabled for this database.",
            "CDC is disabled for this database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N69(
            "It is not possible to perform { %s } on the system database.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.operation, List.of(CYPHER_CONSTRUCT))
            },
            "system database is immutable",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N70(
            "Cannot get routing table for { %s } because Bolt is not enabled. Please update your configuration such that 'server.bolt.enabled' is set to true.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "bolt is not enabled",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N71(
            "Feature: { %s } is not available in a sharded database.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.feat, List.of(FIXED_TEXT, SCHEMA))
            },
            "unsupported operation on a sharded database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N72(
            "Failed to allocate memory in a memory pool. See { %s } in the neo4j.conf file.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.cfgSetting, List.of(CONFIG_SETTING))
            },
            "memory pool out of memory",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N73(
            "The transaction uses more memory than it is allowed. The maximum allowed size for a transaction can be configured with { %s } in the neo4j.conf file.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.cfgSetting, List.of(CONFIG_SETTING))
            },
            "transaction memory limit reached",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N74(
            "Failed to start a new transaction. The limit of concurrent transactions is reached. Increase the number of concurrent transactions using { %s } in the neo4j.conf file.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.cfgSetting, List.of(CONFIG_SETTING))
            },
            "maximum number of transactions reached",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N75(
            "Unable to find entity with id { %s } since it is not up to date. Retrying your request at a later time may succeed.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.entityId, List.of(SCHEMA))},
            "shard execution error",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_51N76("The upgrade to a new Neo4j version failed.", "upgrade failed", ErrorClassification.CLIENT_ERROR),
    STATUS_51N77(
            "{ %s } is not supported in { %s } store format.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.feat, List.of(FIXED_TEXT)),
                new NonSensitiveGqlParam(GqlParams.StringParam.storeFormat, List.of(FIXED_TEXT))
            },
            "not supported in this store format",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N78(
            "Routing is not permitted via this connector. Switch the connection URI scheme to bolt:// or connect to a connector with routing support.",
            "routing unavailable",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N79(
            "Access to database { %s } is not permitted via this connector.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "database unavailable",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N7A(
            "No admin user candidate is found. Use `neo4j-admin dbms set-default-admin` to select a valid admin.",
            "no admin user candidate",
            ErrorClassification.CLIENT_ERROR),
    STATUS_51N7B(
            "Feature: { %s } is not available in a composite database.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.feat, List.of(FIXED_TEXT, SCHEMA))
            },
            "unsupported operation on a composite database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N01(
            "Execution of the procedure { %s } timed out after { %s } { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.timeAmount, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.StringParam.timeUnit, List.of(TEMPORAL_SPATIAL))
            },
            "procedure execution timeout",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N02(
            "Execution of the procedure { %s } failed due to a client error.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS))
            },
            "procedure execution client error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N03(
            "Execution of the procedure { %s } failed due to an invalid specified execution mode { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procExeMode, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid procedure execution mode",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N05(
            "Cannot invoke procedure on this member because it is not a secondary for the database { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "must invoke procedure on secondary",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N06(
            "Unexpected number of arguments (expected 0-2 but received { %s }).",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.count, List.of(NON_SENSITIVE_NUMBER))
            },
            "invalid number of arguments to checkConnectivity",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N07(
            "Unrecognised port name { %s } (valid values are: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.port, List.of(METADATA)),
                new NonSensitiveGqlParam(GqlParams.ListParam.portList, List.of(METADATA))
            },
            Map.of(GqlParams.ListParam.portList, GqlParams.JoinStyle.ANDED),
            "invalid port argument to checkConnectivity",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N08(
            "Unable to parse server id { %s }.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.server, List.of(TOPOLOGY))},
            "invalid server id argument to checkConnectivity",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N09(
            "Execution of the procedure { %s } failed due to a database error.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS))
            },
            "procedure execution database error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_52N10(
            "An address key is included in the query string provided to the GetRoutingTableProcedure, but its value could not be parsed.",
            "invalid address key",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N11(
            "An unexpected error has occurred: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            "generic topology procedure error",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N12(
            "The previous default database { %s } is still running.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "cannot change default database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N13(
            "New default database { %s } does not exist.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.db, List.of(TOPOLOGY))},
            "new default database does not exist",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N14(
            "System database cannot be set as default.",
            "system cannot be default database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N15(
            "Provided allocator { %s } is not available or was not initialized. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.alloc, List.of(TOPOLOGY))},
            "no such allocator",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N16(
            "Invalid arguments to procedure.", "invalid procedure argument list", ErrorClassification.CLIENT_ERROR),
    STATUS_52N17(
            "Setting/removing the quarantine marker failed: { %s }",
            new GqlParams.GqlParam[] {GqlParams.StringParam.msg},
            "quarantine change failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N18(
            "The number of seeding servers { %s } is larger than the defined number of allocations { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.NumberParam.countSeeders, List.of(NON_SENSITIVE_NUMBER)),
                new NonSensitiveGqlParam(GqlParams.NumberParam.countAllocs, List.of(NON_SENSITIVE_NUMBER))
            },
            "too many seeders",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N19(
            "The specified seeding server with id { %s } was not found. Verify that the spelling is correct.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.server, List.of(TOPOLOGY))},
            "no such seeder",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N20(
            "The recreation of a database is not supported when seed updating is not enabled.",
            "seed updating not enabled",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N21(
            "Failed to clean the system graph.", "failed to clean the system graph", ErrorClassification.CLIENT_ERROR),
    STATUS_52N22(
            "Invalid argument { %s } for { %s } on procedure { %s }. The expected format of { %s } is { %s }.",
            new GqlParams.GqlParam[] {
                GqlParams.StringParam.field,
                new NonSensitiveGqlParam(GqlParams.StringParam.procParam, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procParam, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procParamFmt, List.of(FIXED_TEXT))
            },
            "invalid procedure argument",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N23(
            "The following namespaces are not reloadable: { %s }",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.ListParam.namespaceList, List.of(PROCEDURES_FUNCTIONS))
            },
            Map.of(GqlParams.ListParam.namespaceList, GqlParams.JoinStyle.ANDED),
            "non-reloadable namespace",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N24(
            "Failed to reload procedures. See logs for more information.",
            "failed to reload procedures",
            ErrorClassification.DATABASE_ERROR),
    STATUS_52N25(
            "JMX error while accessing { %s }. See logs for more information.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.param, List.of(METADATA))},
            "JMX error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_52N26(
            "{ %s } is not a valid change identifier.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.param, List.of(TOPOLOGY))},
            "invalid change identifier",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N27(
            "The commit timestamp for the provided transaction ID does not match the one in the transaction log.",
            "invalid commit timestamp",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N28(
            "{ %s } is not a valid change identifier. Transaction ID does not exist.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.transactionId, List.of(TOPOLOGY))},
            "invalid change identifier and transaction id",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N29(
            "Given ChangeIdentifier describes a transaction that occurred before any enrichment records exist.",
            "outdated change identifier",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N30(
            "Given ChangeIdentifier describes a transaction that hasn't yet occurred.",
            "future change identifier",
            ErrorClassification.TRANSIENT_ERROR),
    STATUS_52N31(
            "Change identifier { %s } does not belong to this database.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.param, List.of(TOPOLOGY))},
            "wrong database",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N32(
            "Change identifier { %s } has an invalid sequence number { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.param1, List.of(TOPOLOGY)),
                new NonSensitiveGqlParam(GqlParams.StringParam.param2, List.of(NON_SENSITIVE_NUMBER))
            },
            "invalid sequence number",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N33(
            "Failed to invoke procedure/function { %s } caused by: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.sig, List.of(PROCEDURES_FUNCTIONS)),
                GqlParams.StringParam.msg
            },
            "procedure invocation failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N34(
            "{ %s } is restricted and accesses database internals. Procedure restriction is controlled by the dbms.security.procedures.unrestricted setting. Only un-restrict procedures you can trust with access to database internals.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS))
            },
            "procedure restricted",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N35(
            "Failed to compile procedure defined in { %s }: { %s }",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.procClass, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.msg, List.of(PROCEDURES_FUNCTIONS))
            },
            "procedure compilation failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N36(
            "Invalid argument { %s } for { %s } on procedure { %s }. The expected format of { %s } is outlined in the { %s } API documentation.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.field, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procParam, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procParam, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.procParamFmt, List.of(PROCEDURES_FUNCTIONS))
            },
            "invalid procedure argument with API documentation hint",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52N37(
            "Execution of the procedure { %s } failed.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS))
            },
            "procedure execution error",
            ErrorClassification.UNKNOWN),
    STATUS_52N38(
            "Cannot find a start position in the logs.", "cdc start position not found", ErrorClassification.UNKNOWN),
    STATUS_52N39("The log scanner is no longer active.", "cdc scanner inactive", ErrorClassification.UNKNOWN),
    STATUS_52N40(
            "Reconciliation failed during writing the topology graph, transaction may not be committed.",
            "reconciler execution error",
            ErrorClassification.DATABASE_ERROR),
    STATUS_52N41(
            "The key value for { %s } in the query string cannot be parsed when getting a routing table.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.field, List.of(FIXED_TEXT))},
            "invalid routing key",
            ErrorClassification.CLIENT_ERROR),
    STATUS_52U00(
            "Execution of the procedure { %s } failed due to { %s }: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.proc, List.of(PROCEDURES_FUNCTIONS)),
                // msgTitle is the name of the underlying exception class
                new NonSensitiveGqlParam(GqlParams.StringParam.msgTitle, List.of(METADATA)),
                GqlParams.StringParam.msg
            },
            "custom procedure execution error cause",
            ErrorClassification.UNKNOWN),
    STATUS_53N33(
            "Failed to invoke function { %s } caused by: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.sig, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.msg, List.of(FIXED_TEXT))
            },
            "function invocation failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_53N34(
            "{ %s } is restricted and accesses database internals. User-defined function restriction is controlled by the dbms.security.procedures.unrestricted setting. Only un-restrict user-defined functions you can trust with access to database internals.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "function restricted",
            ErrorClassification.CLIENT_ERROR),
    STATUS_53N35(
            "Failed to compile function defined in { %s }: { %s }",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.funClass, List.of(PROCEDURES_FUNCTIONS)),
                new NonSensitiveGqlParam(GqlParams.StringParam.msg, List.of(PROCEDURES_FUNCTIONS))
            },
            "function compilation failed",
            ErrorClassification.CLIENT_ERROR),
    STATUS_53N37(
            "Execution of the function { %s } failed.",
            new GqlParams.GqlParam[] {new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS))
            },
            "function execution error",
            ErrorClassification.UNKNOWN),
    STATUS_53U00(
            "Execution of the function { %s } failed due to { %s }: { %s }.",
            new GqlParams.GqlParam[] {
                new NonSensitiveGqlParam(GqlParams.StringParam.fun, List.of(PROCEDURES_FUNCTIONS)),
                // msgTitle is the name of the underlying exception class
                new NonSensitiveGqlParam(GqlParams.StringParam.msgTitle, List.of(METADATA)),
                GqlParams.StringParam.msg
            },
            "custom function execution error cause",
            ErrorClassification.UNKNOWN),
    STATUS_G1000("", "", ErrorClassification.UNKNOWN),
    STATUS_G1001("", "edges still exist", ErrorClassification.CLIENT_ERROR),
    STATUS_G1002("", "endpoint node is deleted", ErrorClassification.UNKNOWN),
    STATUS_G1003("", "endpoint node not in current working graph", ErrorClassification.UNKNOWN),
    STATUS_G2000("", "", ErrorClassification.UNKNOWN),
    ;

    private final GqlStatus gqlStatus;
    private final GqlParams.GqlParam[] statusParameterKeys;
    private final String subCondition;
    private final Condition condition;
    private final GqlClassification classification;
    private final String template;
    private final int[] offsets;
    private final Map<GqlParams.ListParam, GqlParams.JoinStyle> joinStyles;
    private final List<String> nonSensitiveParameterKeys;

    GqlStatusInfoCodes(String template, String subCondition, GqlClassification classification) {
        this(template, new GqlParams.GqlParam[] {}, emptyMap(), subCondition, classification);
    }

    GqlStatusInfoCodes(
            String template,
            GqlParams.GqlParam[] statusParameterKeys,
            String subCondition,
            GqlClassification classification) {
        this(template, statusParameterKeys, emptyMap(), subCondition, classification);
    }

    GqlStatusInfoCodes(
            String template,
            GqlParams.GqlParam[] statusParameterKeys,
            Map<GqlParams.ListParam, GqlParams.JoinStyle> joinStyles,
            String subCondition,
            GqlClassification classification) {
        validateNameFormat();
        this.gqlStatus = extractGqlStatusFromName();
        this.statusParameterKeys = new GqlParams.GqlParam[statusParameterKeys.length];
        this.nonSensitiveParameterKeys = new ArrayList<>(statusParameterKeys.length);

        for (int i = 0; i < statusParameterKeys.length; i++) {
            if (statusParameterKeys[i] instanceof NonSensitiveGqlParam nonSensitiveKey) {
                this.statusParameterKeys[i] = nonSensitiveKey.getInnerParam();
                this.nonSensitiveParameterKeys.add(nonSensitiveKey.name());
            } else {
                this.statusParameterKeys[i] = statusParameterKeys[i];
            }
        }

        this.joinStyles = joinStyles;
        this.condition = mapConditionFromName();
        this.subCondition = subCondition;
        this.classification = classification;
        this.template = template;
        this.offsets = getOffSets();
    }

    private void validateNameFormat() {
        String expectedNamePattern = "STATUS_[A-Z0-9]{5}";
        if (!this.name().matches(expectedNamePattern)) {
            throw new IllegalStateException(
                    "Expected GqlStatusInfoCodes enum entry to be of format STATUS_ABCDE, but was %s"
                            .formatted(this.name()));
        }
    }

    private GqlStatus extractGqlStatusFromName() {
        // The validateNameFormat method has been run before this one, so the name will be on the format STATUS_ABCDE
        String gqlStatusString = this.name().substring(7);
        return new GqlStatus(gqlStatusString);
    }

    private Condition mapConditionFromName() {
        // The validateNameFormat method has been run before this one, so the name will be on the format STATUS_ABCDE
        String classNbr = this.name().substring(7, 9);
        return Condition.fromClass(classNbr);
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

    public List<String> getNonSensitiveParameterKeys() {
        return nonSensitiveParameterKeys;
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
