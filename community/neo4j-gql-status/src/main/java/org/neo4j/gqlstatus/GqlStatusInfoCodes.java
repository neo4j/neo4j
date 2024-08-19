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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public enum GqlStatusInfoCodes implements GqlStatusInfo {
    STATUS_00000(
            new GqlStatus("00000"),
            """
            """,
            new GqlMessageParams[] {},
            Condition.SUCCESSFUL_COMPLETION,
            ""),
    STATUS_00001(
            new GqlStatus("00001"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SUCCESSFUL_COMPLETION,
            "omitted result"),
    STATUS_00N50(
            new GqlStatus("00N50"),
            """
                    The database `%s` does not exist. Verify that the spelling is correct or create the database for the command to take effect.""",
            new GqlMessageParams[] {GqlMessageParams.db},
            Condition.SUCCESSFUL_COMPLETION,
            "home database not found"),
    STATUS_00N70(
            new GqlStatus("00N70"),
            """
                    `%s` has no effect. The role or privilege is already assigned.""",
            new GqlMessageParams[] {GqlMessageParams.cmd},
            Condition.SUCCESSFUL_COMPLETION,
            "role or privilege already assigned"),
    STATUS_00N71(
            new GqlStatus("00N71"),
            """
                    `%s` has no effect. The role or privilege is not assigned.""",
            new GqlMessageParams[] {GqlMessageParams.cmd},
            Condition.SUCCESSFUL_COMPLETION,
            "role or privilege not assigned"),
    STATUS_00N72(
            new GqlStatus("00N72"),
            """
                    The auth provider `%s` is not defined in the configuration. Verify that the spelling is correct or define `%s` in the configuration.""",
            new GqlMessageParams[] {GqlMessageParams.provider, GqlMessageParams.provider},
            Condition.SUCCESSFUL_COMPLETION,
            "no such auth provider"),
    STATUS_00N80(
            new GqlStatus("00N80"),
            """
                    `ENABLE SERVER` has no effect. Server `%s` is already enabled. Verify that this is the intended server.""",
            new GqlMessageParams[] {GqlMessageParams.server},
            Condition.SUCCESSFUL_COMPLETION,
            "server already enabled"),
    STATUS_00N81(
            new GqlStatus("00N81"),
            """
                    `CORDON SERVER` has no effect. Server `%s` is already cordoned. Verify that this is the intended server.""",
            new GqlMessageParams[] {GqlMessageParams.server},
            Condition.SUCCESSFUL_COMPLETION,
            "server already cordoned"),
    STATUS_00N82(
            new GqlStatus("00N82"),
            """
                    `REALLOCATE DATABASES` has no effect. No databases were reallocated. No better allocation is currently possible.""",
            new GqlMessageParams[] {},
            Condition.SUCCESSFUL_COMPLETION,
            "no databases reallocated"),
    STATUS_00N83(
            new GqlStatus("00N83"),
            """
                    Cordoned servers existed when making an allocation decision. Server(s) `%s` are cordoned. This can impact allocation decisions.""",
            new GqlMessageParams[] {GqlMessageParams.serverList},
            Condition.SUCCESSFUL_COMPLETION,
            "cordoned servers existed during allocation"),
    STATUS_00N84(
            new GqlStatus("00N84"),
            """
                    `ALTER DATABASE` has no effect. The requested topology matched the current topology. No allocations were changed.""",
            new GqlMessageParams[] {},
            Condition.SUCCESSFUL_COMPLETION,
            "requested topology matched current topology"),
    STATUS_00NA0(
            new GqlStatus("00NA0"),
            """
                    `%s` has no effect. `%s` already exists.""",
            new GqlMessageParams[] {GqlMessageParams.cmd, GqlMessageParams.indexConstrPat},
            Condition.SUCCESSFUL_COMPLETION,
            "index or constraint already exists"),
    STATUS_00NA1(
            new GqlStatus("00NA1"),
            """
                    `%s` has no effect. `%s` does not exist.""",
            new GqlMessageParams[] {GqlMessageParams.cmd, GqlMessageParams.indexConstrName},
            Condition.SUCCESSFUL_COMPLETION,
            "index or constraint does not exist"),
    STATUS_01000(new GqlStatus("01000"), """
            """, new GqlMessageParams[] {}, Condition.WARNING, ""),
    STATUS_01004(
            new GqlStatus("01004"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.WARNING,
            "string data, right truncation"),
    STATUS_01G03(
            new GqlStatus("01G03"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.WARNING,
            "graph does not exist"),
    STATUS_01G04(
            new GqlStatus("01G04"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.WARNING,
            "graph type does not exist"),
    STATUS_01G11(
            new GqlStatus("01G11"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.WARNING,
            "null value eliminated in set function"),
    STATUS_01N00(
            new GqlStatus("01N00"),
            """
                    %s""",
            new GqlMessageParams[] {GqlMessageParams.msg},
            Condition.WARNING,
            "feature deprecated"),
    STATUS_01N01(
            new GqlStatus("01N01"),
            """
                    `%s` is deprecated. It is replaced by `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.thing1, GqlMessageParams.thing2},
            Condition.WARNING,
            "feature deprecated with replacement"),
    STATUS_01N02(
            new GqlStatus("01N02"),
            """
                    `%s` is deprecated and will be removed without a replacement.""",
            new GqlMessageParams[] {GqlMessageParams.thing},
            Condition.WARNING,
            "feature deprecated without replacement"),
    STATUS_01N03(
            new GqlStatus("01N03"),
            """
                    `%s` for procedure `%s` is deprecated.""",
            new GqlMessageParams[] {GqlMessageParams.field, GqlMessageParams.proc},
            Condition.WARNING,
            "procedure field deprecated"),
    STATUS_01N30(
            new GqlStatus("01N30"),
            """
                    Unable to create a plan with `JOIN ON %s`. Try to change the join key(s) or restructure your query.""",
            new GqlMessageParams[] {GqlMessageParams.varList},
            Condition.WARNING,
            "join hint unfulfillable"),
    STATUS_01N31(
            new GqlStatus("01N31"),
            """
                    Unable to create a plan with `%s` because the index does not exist.""",
            new GqlMessageParams[] {GqlMessageParams.indexDescr},
            Condition.WARNING,
            "hinted index not found"),
    STATUS_01N40(
            new GqlStatus("01N40"),
            """
                    The query cannot be executed with `%s`, `%s` is used. Cause: `%s`.""",
            new GqlMessageParams[] {
                GqlMessageParams.preparserInput1, GqlMessageParams.preparserInput2, GqlMessageParams.msg
            },
            Condition.WARNING,
            "unsupported runtime"),
    STATUS_01N42(
            new GqlStatus("01N42"),
            """
                    Unknown warning.""",
            new GqlMessageParams[] {},
            Condition.WARNING,
            "unknown warning"),
    STATUS_01N50(
            new GqlStatus("01N50"),
            """
                    The label `%s` does not exist. Verify that the spelling is correct.""",
            new GqlMessageParams[] {GqlMessageParams.label},
            Condition.WARNING,
            "unknown label"),
    STATUS_01N51(
            new GqlStatus("01N51"),
            """
                    The relationship type `%s` does not exist. Verify that the spelling is correct.""",
            new GqlMessageParams[] {GqlMessageParams.reltype},
            Condition.WARNING,
            "unknown relationship type"),
    STATUS_01N52(
            new GqlStatus("01N52"),
            """
                    The property `%s` does not exist. Verify that the spelling is correct.""",
            new GqlMessageParams[] {GqlMessageParams.propkey},
            Condition.WARNING,
            "unknown property key"),
    STATUS_01N60(
            new GqlStatus("01N60"),
            """
                    The query plan cannot be cached and is not executable without `EXPLAIN` due to the undefined parameter(s) `%s`. Provide the parameter(s).""",
            new GqlMessageParams[] {GqlMessageParams.paramList},
            Condition.WARNING,
            "parameter missing"),
    STATUS_01N61(
            new GqlStatus("01N61"),
            """
                    The expression `%s` cannot be satisfied because relationships must have exactly one type.""",
            new GqlMessageParams[] {GqlMessageParams.labelExpr},
            Condition.WARNING,
            "unsatisfiable relationship type expression"),
    STATUS_01N62(
            new GqlStatus("01N62"),
            """
                    The procedure `%s` generates the warning `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.proc, GqlMessageParams.msg},
            Condition.WARNING,
            "procedure or function execution warning"),
    STATUS_01N63(
            new GqlStatus("01N63"),
            """
                    `%s` is repeated in `%s`, which leads to no results.""",
            new GqlMessageParams[] {GqlMessageParams.var, GqlMessageParams.pat},
            Condition.WARNING,
            "repeated relationship reference"),
    STATUS_01N70(
            new GqlStatus("01N70"),
            """
                    `%s` has no effect. %s Make sure nothing is misspelled. This notification will become an error in a future major version.""",
            new GqlMessageParams[] {GqlMessageParams.cmd, GqlMessageParams.msg},
            Condition.WARNING,
            "impossible revoke command"),
    STATUS_01N71(
            new GqlStatus("01N71"),
            """
                    Use setting `dbms.security.require_local_user` to enable external auth.""",
            new GqlMessageParams[] {},
            Condition.WARNING,
            "external auth disabled"),
    STATUS_01U00(
            new GqlStatus("01U00"),
            """
                    Execution of the procedure `%s` generated warning `%s`: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.proc, GqlMessageParams.param2, GqlMessageParams.param3},
            Condition.WARNING,
            "custom procedure warning cause"),
    STATUS_02000(new GqlStatus("02000"), """
            """, new GqlMessageParams[] {}, Condition.NO_DATA, ""),
    STATUS_02N42(
            new GqlStatus("02N42"),
            """
                    Unknown GQLSTATUS from old server.""",
            new GqlMessageParams[] {},
            Condition.NO_DATA,
            "unknown subcondition"),
    STATUS_03000(new GqlStatus("03000"), """
            """, new GqlMessageParams[] {}, Condition.INFORMATIONAL, ""),
    STATUS_03N42(
            new GqlStatus("03N42"),
            """
                    Unknown notification.""",
            new GqlMessageParams[] {},
            Condition.INFORMATIONAL,
            "unknown notification"),
    STATUS_03N60(
            new GqlStatus("03N60"),
            """
                    The variable `%s` in the subquery uses the same name as a variable from the outer query. Use `WITH %s` in the subquery to import the one from the outer scope unless you want it to be a new variable.""",
            new GqlMessageParams[] {GqlMessageParams.var, GqlMessageParams.var},
            Condition.INFORMATIONAL,
            "subquery variable shadowing"),
    STATUS_03N90(
            new GqlStatus("03N90"),
            """
                    The disconnected patterns `%s` build a cartesian product. A cartesian product may produce a large amount of data and slow down query processing.""",
            new GqlMessageParams[] {GqlMessageParams.pat},
            Condition.INFORMATIONAL,
            "cartesian product"),
    STATUS_03N91(
            new GqlStatus("03N91"),
            """
                    The provided pattern `%s` is unbounded. Shortest path with an unbounded pattern may result in long execution times. Use an upper limit (e.g. `[*..5]`) on the number of node hops in your pattern.""",
            new GqlMessageParams[] {GqlMessageParams.pat},
            Condition.INFORMATIONAL,
            "unbounded variable length pattern"),
    STATUS_03N92(
            new GqlStatus("03N92"),
            """
                    The query runs with exhaustive shortest path due to the existential predicate(s) `%s`. It may be possible to use `WITH` to separate the `MATCH` from the existential predicate(s).""",
            new GqlMessageParams[] {GqlMessageParams.predList},
            Condition.INFORMATIONAL,
            "exhaustive shortest path"),
    STATUS_03N93(
            new GqlStatus("03N93"),
            """
                    `LOAD CSV` in combination with `MATCH` or `MERGE` on a label that does not have an index may result in long execution times. Consider adding an index for label `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.label},
            Condition.INFORMATIONAL,
            "no applicable index"),
    STATUS_03N94(
            new GqlStatus("03N94"),
            """
                    The query execution plan contains the `Eager` operator. `LOAD CSV` in combination with `Eager` can consume a lot of memory.""",
            new GqlMessageParams[] {},
            Condition.INFORMATIONAL,
            "eager operator"),
    STATUS_03N95(
            new GqlStatus("03N95"),
            """
                    An index exists on label/type(s) `%s`. It is not possible to use indexes for dynamic properties. Consider using static properties.""",
            new GqlMessageParams[] {GqlMessageParams.labelList},
            Condition.INFORMATIONAL,
            "dynamic property"),
    STATUS_03N96(
            new GqlStatus("03N96"),
            """
                    Failed to generate code, falling back to interpreted %s engine. A stacktrace can be found in the debug.log. Cause: %s.""",
            new GqlMessageParams[] {GqlMessageParams.enginetype, GqlMessageParams.msg},
            Condition.INFORMATIONAL,
            "failed code generation"),
    STATUS_08000(
            new GqlStatus("08000"), """
            """, new GqlMessageParams[] {}, Condition.CONNECTION_EXCEPTION, ""),
    STATUS_08007(
            new GqlStatus("08007"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.CONNECTION_EXCEPTION,
            "transaction resolution unknown"),
    STATUS_08N00(
            new GqlStatus("08N00"),
            """
                    Unable to connect to `%s`. Unable to get bolt address of the leader. Check the status of the database. Retrying your request at a later time may succeed.""",
            new GqlMessageParams[] {GqlMessageParams.dbName},
            Condition.CONNECTION_EXCEPTION,
            "unable to connect to database"),
    STATUS_08N01(
            new GqlStatus("08N01"),
            """
                    Unable to write to `%s` on this server. Server-side routing is disabled. Either connect to the database leader directly or enable server-side routing by setting `%s=true`.""",
            new GqlMessageParams[] {GqlMessageParams.dbName, GqlMessageParams.routingEnabledSetting},
            Condition.CONNECTION_EXCEPTION,
            "unable to write to database"),
    STATUS_08N02(
            new GqlStatus("08N02"),
            """
                    Unable to connect to database `%s`. Server-side routing is disabled. Either connect to `%s` directly, or enable server-side routing by setting `%s=true`.""",
            new GqlMessageParams[] {
                GqlMessageParams.dbName, GqlMessageParams.dbName, GqlMessageParams.routingEnabledSetting
            },
            Condition.CONNECTION_EXCEPTION,
            "unable to route to database"),
    STATUS_08N03(
            new GqlStatus("08N03"),
            """
                    Failed to write to `%s`. Check the defined access mode in both driver and database.""",
            new GqlMessageParams[] {GqlMessageParams.graph},
            Condition.CONNECTION_EXCEPTION,
            "failed to write to graph"),
    STATUS_08N04(
            new GqlStatus("08N04"),
            """
                    Routing with `%s` is not supported in embedded sessions. Connect to the database directly or try running the query using a Neo4j driver or the HTTP API.""",
            new GqlMessageParams[] {GqlMessageParams.useClause},
            Condition.CONNECTION_EXCEPTION,
            "unable to route use clause"),
    STATUS_08N05(
            new GqlStatus("08N05"),
            """
                    Routing administration commands is not supported in embedded sessions. Connect to the system database directly or try running the query using a Neo4j driver or the HTTP API.""",
            new GqlMessageParams[] {},
            Condition.CONNECTION_EXCEPTION,
            "unable to route administration command"),
    STATUS_08N06(
            new GqlStatus("08N06"),
            """
                    General network protocol error.""",
            new GqlMessageParams[] {},
            Condition.CONNECTION_EXCEPTION,
            "protocol error"),
    STATUS_08N07(
            new GqlStatus("08N07"),
            """
                    This member is not the leader.""",
            new GqlMessageParams[] {},
            Condition.CONNECTION_EXCEPTION,
            "not the leader"),
    STATUS_08N08(
            new GqlStatus("08N08"),
            """
                    This database is read only on this server.""",
            new GqlMessageParams[] {},
            Condition.CONNECTION_EXCEPTION,
            "database is read only"),
    STATUS_08N09(
            new GqlStatus("08N09"),
            """
                    The database `%s` is currently unavailable. Check the database status. Retry your request at a later time.""",
            new GqlMessageParams[] {GqlMessageParams.db},
            Condition.CONNECTION_EXCEPTION,
            "database unavailable"),
    STATUS_08N10(
            new GqlStatus("08N10"),
            """
                    The server is not in a state that can process message of type: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.msgType},
            Condition.CONNECTION_EXCEPTION,
            "invalid server state"),
    STATUS_08N11(
            new GqlStatus("08N11"),
            """
                    The request is invalid and could not be processed by the server. See cause for further details.""",
            new GqlMessageParams[] {},
            Condition.CONNECTION_EXCEPTION,
            "request error"),
    STATUS_08N12(
            new GqlStatus("08N12"),
            """
                    Failed to parse the supplied bookmark. Verify it is correct or check the debug log for more information.""",
            new GqlMessageParams[] {},
            Condition.CONNECTION_EXCEPTION,
            "failed to parse bookmark"),
    STATUS_08N13(
            new GqlStatus("08N13"),
            """
                    `%s` is not up to the requested bookmark `%s`. The latest transaction ID is `%s`.""",
            new GqlMessageParams[] {
                GqlMessageParams.dbName, GqlMessageParams.oldestAcceptableTxId, GqlMessageParams.latestTransactionId
            },
            Condition.CONNECTION_EXCEPTION,
            "database not up to requested bookmark"),
    STATUS_08N14(
            new GqlStatus("08N14"),
            """
                    Unable to provide a routing table for the database `%s` because the request comes from another alias `%s` and alias chains are not permitted.""",
            new GqlMessageParams[] {GqlMessageParams.aliasName, GqlMessageParams.sourceAliasString},
            Condition.CONNECTION_EXCEPTION,
            "alias chains are not permitted"),
    STATUS_08N15(
            new GqlStatus("08N15"),
            """
                    Policy definition for '%s' could not be found.""",
            new GqlMessageParams[] {GqlMessageParams.routingPolicy},
            Condition.CONNECTION_EXCEPTION,
            "unknown routing policy"),
    STATUS_22000(new GqlStatus("22000"), """
            """, new GqlMessageParams[] {}, Condition.DATA_EXCEPTION, ""),
    STATUS_22001(
            new GqlStatus("22001"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "string data, right truncation"),
    STATUS_22003(
            new GqlStatus("22003"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "numeric value out of range"),
    STATUS_22004(
            new GqlStatus("22004"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "null value not allowed"),
    STATUS_22007(
            new GqlStatus("22007"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime format"),
    STATUS_22008(
            new GqlStatus("22008"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "datetime field overflow"),
    STATUS_22011(
            new GqlStatus("22011"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "substring error"),
    STATUS_22012(
            new GqlStatus("22012"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "division by zero"),
    STATUS_22015(
            new GqlStatus("22015"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "interval field overflow"),
    STATUS_22018(
            new GqlStatus("22018"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid character value for cast"),
    STATUS_2201E(
            new GqlStatus("2201E"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid argument for natural logarithm"),
    STATUS_2201F(
            new GqlStatus("2201F"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid argument for power function"),
    STATUS_22027(
            new GqlStatus("22027"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "trim error"),
    STATUS_2202F(
            new GqlStatus("2202F"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "array data, right truncation"),
    STATUS_22G02(
            new GqlStatus("22G02"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "negative limit value"),
    STATUS_22G03(
            new GqlStatus("22G03"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid value type"),
    STATUS_22G04(
            new GqlStatus("22G04"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "values not comparable"),
    STATUS_22G05(
            new GqlStatus("22G05"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime function field name"),
    STATUS_22G06(
            new GqlStatus("22G06"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid datetime function value"),
    STATUS_22G07(
            new GqlStatus("22G07"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid duration function field name"),
    STATUS_22G0B(
            new GqlStatus("22G0B"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "list data, right truncation"),
    STATUS_22G0C(
            new GqlStatus("22G0C"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "list element error"),
    STATUS_22G0F(
            new GqlStatus("22G0F"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid number of paths or groups"),
    STATUS_22G0H(
            new GqlStatus("22G0H"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid duration format"),
    STATUS_22G0M(
            new GqlStatus("22G0M"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "multiple assignments to a graph element property"),
    STATUS_22G0N(
            new GqlStatus("22G0N"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "number of node labels below supported minimum"),
    STATUS_22G0P(
            new GqlStatus("22G0P"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "number of node labels exceeds supported maximum"),
    STATUS_22G0Q(
            new GqlStatus("22G0Q"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "number of edge labels below supported minimum"),
    STATUS_22G0R(
            new GqlStatus("22G0R"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "number of edge labels exceeds supported maximum"),
    STATUS_22G0S(
            new GqlStatus("22G0S"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "number of node properties exceeds supported maximum"),
    STATUS_22G0T(
            new GqlStatus("22G0T"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "number of edge properties exceeds supported maximum"),
    STATUS_22G0U(
            new GqlStatus("22G0U"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "record fields do not match"),
    STATUS_22G0V(
            new GqlStatus("22G0V"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "reference value, invalid base type"),
    STATUS_22G0W(
            new GqlStatus("22G0W"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "reference value, invalid constrained type"),
    STATUS_22G0X(
            new GqlStatus("22G0X"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "record data, field unassignable"),
    STATUS_22G0Y(
            new GqlStatus("22G0Y"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "record data, field missing"),
    STATUS_22G0Z(
            new GqlStatus("22G0Z"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "malformed path"),
    STATUS_22G10(
            new GqlStatus("22G10"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "path data, right truncation"),
    STATUS_22G11(
            new GqlStatus("22G11"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "reference value, referent deleted"),
    STATUS_22G12(
            new GqlStatus("22G12"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid value type"),
    STATUS_22G13(
            new GqlStatus("22G13"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid group variable value"),
    STATUS_22G14(
            new GqlStatus("22G14"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "incompatible temporal instant unit groups"),
    STATUS_22N00(
            new GqlStatus("22N00"),
            """
                    The provided value is unsupported and cannot be processed.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "unsupported value"),
    STATUS_22N01(
            new GqlStatus("22N01"),
            """
                    Expected `%s` to be of `%s` (or of `%s` ...), but was of type `%s`.""",
            new GqlMessageParams[] {
                GqlMessageParams.value, GqlMessageParams.type1, GqlMessageParams.type2, GqlMessageParams.valueType
            },
            Condition.DATA_EXCEPTION,
            "invalid type"),
    STATUS_22N02(
            new GqlStatus("22N02"),
            """
                    Expected `%s` to be a positive number but found '%s' instead.""",
            new GqlMessageParams[] {GqlMessageParams.name, GqlMessageParams.value},
            Condition.DATA_EXCEPTION,
            "negative number"),
    STATUS_22N03(
            new GqlStatus("22N03"),
            """
                    Expected `%s` to be `%s` in the range `%s` to `%s` but found '%s'.""",
            new GqlMessageParams[] {
                GqlMessageParams.component,
                GqlMessageParams.numberType,
                GqlMessageParams.lower,
                GqlMessageParams.upper,
                GqlMessageParams.value
            },
            Condition.DATA_EXCEPTION,
            "number out of range"),
    STATUS_22N04(
            new GqlStatus("22N04"),
            """
                    Invalid input `%s` for `%s`. Expected one of %s.""",
            new GqlMessageParams[] {GqlMessageParams.input, GqlMessageParams.context, GqlMessageParams.validValuesList},
            Condition.DATA_EXCEPTION,
            "invalid input value"),
    STATUS_22N05(
            new GqlStatus("22N05"),
            """
                    Invalid input `%s` for `%s`. Expected %s.

                    eg: Invalid input for password. Expected at least N characters.""",
            new GqlMessageParams[] {
                GqlMessageParams.input, GqlMessageParams.context, GqlMessageParams.explainValidValues
            },
            Condition.DATA_EXCEPTION,
            "input failed validation"),
    STATUS_22N06(
            new GqlStatus("22N06"),
            """
                    Invalid input. %s is not allowed to be an empty string.""",
            new GqlMessageParams[] {GqlMessageParams.entity},
            Condition.DATA_EXCEPTION,
            "empty input string"),
    STATUS_22N07(
            new GqlStatus("22N07"),
            """
                    Invalid pre-parser option: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.keys},
            Condition.DATA_EXCEPTION,
            "invalid pre-parser option key"),
    STATUS_22N08(
            new GqlStatus("22N08"),
            """
                    Invalid pre-parser option, cannot combine `%s` with `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.option1, GqlMessageParams.option2},
            Condition.DATA_EXCEPTION,
            "invalid pre-parser combination"),
    STATUS_22N09(
            new GqlStatus("22N09"),
            """
                    Invalid pre-parser option, cannot specify multiple conflicting values for `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.name},
            Condition.DATA_EXCEPTION,
            "conflicting pre-parser combination"),
    STATUS_22N10(
            new GqlStatus("22N10"),
            """
                    Invalid pre-parser option, `%s` is not a valid option for `%s`. Valid options are: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.input, GqlMessageParams.name, GqlMessageParams.validOptions},
            Condition.DATA_EXCEPTION,
            "invalid pre-parser option value"),
    STATUS_22N11(
            new GqlStatus("22N11"),
            """
                    Invalid argument: cannot process `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.providedArgument},
            Condition.DATA_EXCEPTION,
            "invalid argument"),
    STATUS_22N12(
            new GqlStatus("22N12"),
            """
                    Invalid argument: cannot process `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.providedArgument},
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime format"),
    STATUS_22N13(
            new GqlStatus("22N13"),
            """
                    Specified time zones must include a date component.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid time zone"),
    STATUS_22N14(
            new GqlStatus("22N14"),
            """
                    Cannot select both `%s` and `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.field, GqlMessageParams.component},
            Condition.DATA_EXCEPTION,
            "invalid temporal value combination"),
    STATUS_22N15(
            new GqlStatus("22N15"),
            """
                    Cannot read the specified `%s` component from `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.component, GqlMessageParams.temporal},
            Condition.DATA_EXCEPTION,
            "invalid temporal component"),
    STATUS_22N16(
            new GqlStatus("22N16"),
            """
                    Importing entity values to a graph with a `USE` clause is not supported. Attempted to import `%s` to `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.expr, GqlMessageParams.graph},
            Condition.DATA_EXCEPTION,
            "invalid import value"),
    STATUS_22N17(
            new GqlStatus("22N17"),
            """
                    Cannot read the specified `%s` component from `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.component, GqlMessageParams.temporal},
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime function field name"),
    STATUS_22N18(
            new GqlStatus("22N18"),
            """
                    A `%s` `POINT` must contain `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.crs, GqlMessageParams.mandatoryKeys},
            Condition.DATA_EXCEPTION,
            "incomplete spatial value"),
    STATUS_22N19(
            new GqlStatus("22N19"),
            """
                    A `POINT` must contain either 'x' and 'y', or 'latitude' and 'longitude'.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid spatial value"),
    STATUS_22N20(
            new GqlStatus("22N20"),
            """
                    Cannot create `POINT` with `%s` coordinate reference system (CRS) and `%s` coordinates. Use the equivalent `%s` coordinate reference system instead.""",
            new GqlMessageParams[] {
                GqlMessageParams.crsDimensions, GqlMessageParams.crsLength, GqlMessageParams.crsLength
            },
            Condition.DATA_EXCEPTION,
            "invalid spatial value dimensions"),
    STATUS_22N21(
            new GqlStatus("22N21"),
            """
                    Geographic `POINT` values do not support the coordinate reference system (CRS): `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.crs},
            Condition.DATA_EXCEPTION,
            "unsupported coordinate reference system"),
    STATUS_22N22(
            new GqlStatus("22N22"),
            """
                    Cannot specify both coordinate reference system (CRS) and spatial reference identifier (SRID).""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid spatial value combination"),
    STATUS_22N23(
            new GqlStatus("22N23"),
            """
                    Cannot create WGS84 `POINT` with invalid coordinate: `%s`. The valid range for the latitude coordinate is [-90, 90].""",
            new GqlMessageParams[] {GqlMessageParams.coordinate},
            Condition.DATA_EXCEPTION,
            "invalid latitude value"),
    STATUS_22N24(
            new GqlStatus("22N24"),
            """
                    Cannot construct a `%s` from: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.valueType, GqlMessageParams.coordinates},
            Condition.DATA_EXCEPTION,
            "invalid coordinate arguments"),
    STATUS_22N25(
            new GqlStatus("22N25"),
            """
                    Cannot construct `%s` from: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.valueType, GqlMessageParams.temporal},
            Condition.DATA_EXCEPTION,
            "invalid temporal arguments"),
    STATUS_22N26(
            new GqlStatus("22N26"),
            """
                    Unknown rounding mode. Valid values are: CEILING, FLOOR, UP, DOWN, HALF_EVEN, HALF_UP, HALF_DOWN, UNNECESSARY.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "unknown rounding mode"),
    STATUS_22N27(
            new GqlStatus("22N27"),
            """
                    Expected `%s` to be `%s` (or `%s`, ...), but found `%s` of type `%s`.""",
            new GqlMessageParams[] {
                GqlMessageParams.name,
                GqlMessageParams.type,
                GqlMessageParams.type2,
                GqlMessageParams.value,
                GqlMessageParams.valueType
            },
            Condition.DATA_EXCEPTION,
            "invalid entity type"),
    STATUS_22N28(
            new GqlStatus("22N28"),
            """
                    The result of the operation `%s` has caused an overflow.""",
            new GqlMessageParams[] {GqlMessageParams.operation},
            Condition.DATA_EXCEPTION,
            "overflow error"),
    STATUS_22N29(
            new GqlStatus("22N29"),
            """
                    Unknown coordinate reference system (CRS).""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "unknown coordinate reference system"),
    STATUS_22N30(
            new GqlStatus("22N30"),
            """
                    At least one temporal unit must be specified.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "missing temporal unit"),
    STATUS_22N31(
            new GqlStatus("22N31"),
            """
                    `MERGE` cannot be used for `NODE` and `RELATIONSHIP` property values that are `null` or NaN.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid properties in merge pattern"),
    STATUS_22N32(
            new GqlStatus("22N32"),
            """
                    `ORDER BY` expressions must be deterministic.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "non-deterministic sort expression"),
    STATUS_22N33(
            new GqlStatus("22N33"),
            """
                    Shortest path expressions must contain start and end nodes. Cannot find: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.node},
            Condition.DATA_EXCEPTION,
            "invalid shortest path expression"),
    STATUS_22N34(
            new GqlStatus("22N34"),
            """
                    Cannot use the `%s()` function inside an aggregate function.""",
            new GqlMessageParams[] {GqlMessageParams.function},
            Condition.DATA_EXCEPTION,
            "invalid use of aggregate function"),
    STATUS_22N35(
            new GqlStatus("22N35"),
            """
                    Cannot parse `%s` to a `DATE`. Calendar dates are formatted as `YYYY-MM`, while ordinal dates are formatted `YYYY-DDD`.""",
            new GqlMessageParams[] {GqlMessageParams.text},
            Condition.DATA_EXCEPTION,
            "invalid date format"),
    STATUS_22N36(
            new GqlStatus("22N36"),
            """
                    Cannot parse `%s` to `@typeName`.""",
            new GqlMessageParams[] {GqlMessageParams.text},
            Condition.DATA_EXCEPTION,
            "invalid temporal format"),
    STATUS_22N37(
            new GqlStatus("22N37"),
            """
                    Cannot coerce `%s` to `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.value, GqlMessageParams.type},
            Condition.DATA_EXCEPTION,
            "invalid coercion"),
    STATUS_22N38(
            new GqlStatus("22N38"),
            """
                    Expected input to the `%s()` function to be `%s` (or `%s`, ...), but found `%s` of type `%s`.""",
            new GqlMessageParams[] {
                GqlMessageParams.function,
                GqlMessageParams.type1,
                GqlMessageParams.type2,
                GqlMessageParams.value,
                GqlMessageParams.valueType
            },
            Condition.DATA_EXCEPTION,
            "invalid function input type"),
    STATUS_22N39(
            new GqlStatus("22N39"),
            """
                    Value `%s` cannot be stored in properties.""",
            new GqlMessageParams[] {GqlMessageParams.value},
            Condition.DATA_EXCEPTION,
            "unsupported property value type"),
    STATUS_22N40(
            new GqlStatus("22N40"),
            """
                    Cannot assign `%s` to `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.component, GqlMessageParams.expectedTemporalType},
            Condition.DATA_EXCEPTION,
            "non-assignable temporal component"),
    STATUS_22N41(
            new GqlStatus("22N41"),
            """
                    The `MERGE` clause did not find a matching node `%s` and cannot create a new node due to conflicts with existing uniqueness constraints.""",
            new GqlMessageParams[] {GqlMessageParams.node},
            Condition.DATA_EXCEPTION,
            "merge node uniqueness constraint violation"),
    STATUS_22N42(
            new GqlStatus("22N42"),
            """
                    The `MERGE` clause did not find a matching relationship `%s` and cannot create a new relationship due to conflicts with existing uniqueness constraints.""",
            new GqlMessageParams[] {GqlMessageParams.relationship},
            Condition.DATA_EXCEPTION,
            "merge relationship uniqueness constraint violation"),
    STATUS_22N43(
            new GqlStatus("22N43"),
            """
                    Could not load external resource from `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.url},
            Condition.DATA_EXCEPTION,
            "unable to load external resource"),
    STATUS_22N44(
            new GqlStatus("22N44"),
            """
                    Parallel runtime has been disabled, enable it or upgrade to a bigger Aura instance.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "parallel runtime disabled"),
    STATUS_22N45(
            new GqlStatus("22N45"),
            """
                    The `%s` feature is not supported by Neo4j Community Edition.""",
            new GqlMessageParams[] {GqlMessageParams.feature},
            Condition.DATA_EXCEPTION,
            "feature not supported on Neo4j Community Edition"),
    STATUS_22N46(
            new GqlStatus("22N46"),
            """
                    Parallel runtime does not support updating queries or a change in the state of transactions. Use another runtime.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "unsupported use of parallel runtime"),
    STATUS_22N47(
            new GqlStatus("22N47"),
            """
                    No workers are configured for the parallel runtime. Set 'server.cypher.parallel.worker_limit' to a larger value.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid parallel runtime configuration"),
    STATUS_22N48(
            new GqlStatus("22N48"),
            """
                    Cannot use the specified runtime  `%s` due to  `%s`. Use another runtime.""",
            new GqlMessageParams[] {GqlMessageParams.runtime, GqlMessageParams.unsupportedParts},
            Condition.DATA_EXCEPTION,
            "unable to use specified runtime"),
    STATUS_22N49(
            new GqlStatus("22N49"),
            """
                    Cannot read a CSV field larger than the set buffer size. Ensure the field does not have an unterminated quote, or increase the buffer size via `dbms.import.csv.buffer_size`.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "CSV buffer size overflow"),
    STATUS_22N51(
            new GqlStatus("22N51"),
            """
                    A [composite] database or alias with the name `%s` does not exist. Verify that the spelling is correct.""",
            new GqlMessageParams[] {GqlMessageParams.name},
            Condition.DATA_EXCEPTION,
            "database does not exist"),
    STATUS_22N52(
            new GqlStatus("22N52"),
            """
                    `PROFILE` and `EXPLAIN` cannot be combined.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid combination of `PROFILE` and `EXPLAIN`"),
    STATUS_22N53(
            new GqlStatus("22N53"),
            """
                    Cannot `PROFILE` query before results are materialized.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "invalid use of `PROFILE`"),
    STATUS_22N54(
            new GqlStatus("22N54"),
            """
                    Duplicate key specified for `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.key},
            Condition.DATA_EXCEPTION,
            "invalid map"),
    STATUS_22N55(
            new GqlStatus("22N55"),
            """
                    Map requires key `%s`but was missing from field `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.key, GqlMessageParams.fieldName},
            Condition.DATA_EXCEPTION,
            "required key missing from map"),
    STATUS_22N56(
            new GqlStatus("22N56"),
            """
                    Protocol message length limit exceeded (limit: `%s`).""",
            new GqlMessageParams[] {GqlMessageParams.limit},
            Condition.DATA_EXCEPTION,
            "protocol message length limit overflow"),
    STATUS_22N57(
            new GqlStatus("22N57"),
            """
                    Protocol type is invalid. Invalid number of struct components (received `%s` but expected `%s`).""",
            new GqlMessageParams[] {GqlMessageParams.actualComponentCount, GqlMessageParams.expectedComponentCount},
            Condition.DATA_EXCEPTION,
            "invalid protocol type"),
    STATUS_22N58(
            new GqlStatus("22N58"),
            """
                    Cannot read the specified `%s` component from `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.component, GqlMessageParams.spatial},
            Condition.DATA_EXCEPTION,
            "invalid spatial component"),
    STATUS_22N59(
            new GqlStatus("22N59"),
            """
                    The `%s` token with id `%s` was not found.""",
            new GqlMessageParams[] {GqlMessageParams.tokenType, GqlMessageParams.tokenId},
            Condition.DATA_EXCEPTION,
            "no such token"),
    STATUS_22N62(
            new GqlStatus("22N62"),
            """
                    Relationship Type `%s` not found.""",
            new GqlMessageParams[] {GqlMessageParams.relType},
            Condition.DATA_EXCEPTION,
            "no such relationship type"),
    STATUS_22N63(
            new GqlStatus("22N63"),
            """
                    Property Key `%s` not found.""",
            new GqlMessageParams[] {GqlMessageParams.propKey},
            Condition.DATA_EXCEPTION,
            "no such property key"),
    STATUS_22N64(
            new GqlStatus("22N64"),
            """
                    No such constraint: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.constraintDescriptorOrName},
            Condition.DATA_EXCEPTION,
            "no such constraint"),
    STATUS_22N65(
            new GqlStatus("22N65"),
            """
                    An equivalent constraint already exists: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.constraintDescriptorOrName},
            Condition.DATA_EXCEPTION,
            "equivalent constraint already exists"),
    STATUS_22N66(
            new GqlStatus("22N66"),
            """
                    A conflicting constraint already exists: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.constraintDescriptorOrName},
            Condition.DATA_EXCEPTION,
            "conflicting constraint already exists"),
    STATUS_22N67(
            new GqlStatus("22N67"),
            """
                    A constraint with the same name already exists: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.constraintName},
            Condition.DATA_EXCEPTION,
            "duplicated constraint name"),
    STATUS_22N68(
            new GqlStatus("22N68"),
            """
                    Dependent constraints cannot be managed individually and must be managed together with its Graph Type.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "dependent constraint managed individually"),
    STATUS_22N69(
            new GqlStatus("22N69"),
            """
                    No such index: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.indexDescriptorOrName},
            Condition.DATA_EXCEPTION,
            "no such index"),
    STATUS_22N70(
            new GqlStatus("22N70"),
            """
                    An equivalent index already exists: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.indexDescriptorOrName},
            Condition.DATA_EXCEPTION,
            "equivalent index already exists"),
    STATUS_22N71(
            new GqlStatus("22N71"),
            """
                    An index with the same name already exists: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.indexName},
            Condition.DATA_EXCEPTION,
            "duplicated index name"),
    STATUS_22N72(
            new GqlStatus("22N72"),
            """
                    A requested operation can not be performed on the specified index because the index is part of a constraint.""",
            new GqlMessageParams[] {},
            Condition.DATA_EXCEPTION,
            "index required by existing constraint"),
    STATUS_22N73(
            new GqlStatus("22N73"),
            """
                    Constraint conflicts with already existing index `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.indexName},
            Condition.DATA_EXCEPTION,
            "constraint conflicts with existing index"),
    STATUS_22N74(
            new GqlStatus("22N74"),
            """
                    An index that belongs to the constraint `%s` contains a conflicting index.""",
            new GqlMessageParams[] {GqlMessageParams.constraintName},
            Condition.DATA_EXCEPTION,
            "index conflicts with existing constraint"),
    STATUS_22N75(
            new GqlStatus("22N75"),
            """
                    Constraint `%s` includes a %s more than once.""",
            new GqlMessageParams[] {
                GqlMessageParams.constraintDescriptorOrName, GqlMessageParams.labelOrRelTypeOrPropKey
            },
            Condition.DATA_EXCEPTION,
            "constraint contains duplicated tokens"),
    STATUS_22N76(
            new GqlStatus("22N76"),
            """
                    Index `%s` includes a %s more than once.""",
            new GqlMessageParams[] {GqlMessageParams.indexDescriptorOrName, GqlMessageParams.labelOrRelTypeOrPropKey},
            Condition.DATA_EXCEPTION,
            "index contains duplicated tokens"),
    STATUS_22N77(
            new GqlStatus("22N77"),
            """
                    `%s` (%s) with %s `%s` must have the following propertie(s): %s.""",
            new GqlMessageParams[] {
                GqlMessageParams.nodeOrRel,
                GqlMessageParams.entityId,
                GqlMessageParams.labelOrType,
                GqlMessageParams.labelOrTypeName,
                GqlMessageParams.propkeyList
            },
            Condition.DATA_EXCEPTION,
            "property presence verification failed"),
    STATUS_22N78(
            new GqlStatus("22N78"),
            """
                    `%s` (%s) with %s `%s` must have the property `%s` with type `%s`.""",
            new GqlMessageParams[] {
                GqlMessageParams.nodeOrRel,
                GqlMessageParams.entityId,
                GqlMessageParams.labelOrType,
                GqlMessageParams.labelOrTypeName,
                GqlMessageParams.propkey,
                GqlMessageParams.type
            },
            Condition.DATA_EXCEPTION,
            "property type verification failed"),
    STATUS_22N79(
            new GqlStatus("22N79"),
            """
                    Both `%s` (%s) and `%s` (%s) have the same value for property `%s`.""",
            new GqlMessageParams[] {
                GqlMessageParams.nodeOrRel,
                GqlMessageParams.entityOneId,
                GqlMessageParams.nodeOrRel,
                GqlMessageParams.entityTwoId,
                GqlMessageParams.propkey
            },
            Condition.DATA_EXCEPTION,
            "property uniqueness verification failed"),
    STATUS_25000(
            new GqlStatus("25000"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            ""),
    STATUS_25G01(
            new GqlStatus("25G01"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "active GQL-transaction"),
    STATUS_25G02(
            new GqlStatus("25G02"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "catalog and data statement mixing not supported"),
    STATUS_25G03(
            new GqlStatus("25G03"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "read-only GQL-transaction"),
    STATUS_25G04(
            new GqlStatus("25G04"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "accessing multiple graphs not supported"),
    STATUS_25N01(
            new GqlStatus("25N01"),
            """
                    %s' cannot be executed after '%s'. It must be executed in a different transaction.""",
            new GqlMessageParams[] {GqlMessageParams.query1, GqlMessageParams.query2},
            Condition.INVALID_TRANSACTION_STATE,
            "invalid mix of queries"),
    STATUS_25N02(
            new GqlStatus("25N02"),
            """
                    Unable to complete transaction. See debug log for details.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "unable to complete transaction"),
    STATUS_25N03(
            new GqlStatus("25N03"),
            """
                    Transaction is being used concurrently by another request.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "concurrent access violation"),
    STATUS_25N04(
            new GqlStatus("25N04"),
            """
                    Transaction `%s` does not exist.""",
            new GqlMessageParams[] {GqlMessageParams.transactionId},
            Condition.INVALID_TRANSACTION_STATE,
            "no such transcation"),
    STATUS_25N05(
            new GqlStatus("25N05"),
            """
                    Transaction has been terminated or closed.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "transaction terminated or closed"),
    STATUS_25N06(
            new GqlStatus("25N06"),
            """
                    Failed to start transaction. See debug log for details.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "transaction start failed"),
    STATUS_25N07(
            new GqlStatus("25N07"),
            """
                    Failed to start constituent transaction. See debug log for details.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "constituent transaction start failed"),
    STATUS_25N08(
            new GqlStatus("25N08"),
            """
                    The lease for the transaction is no longer valid.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "invalid transaction lease"),
    STATUS_25N09(
            new GqlStatus("25N09"),
            """
                    The transaction failed due to an internal error.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "internal transaction failure"),
    STATUS_25N11(
            new GqlStatus("25N11"),
            """
                    There was a conflict detected between the transaction state and applied updates. Please retry the transaction.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "conflicting transaction state"),
    STATUS_25N12(
            new GqlStatus("25N12"),
            """
                    Index `%s` was dropped in this transaction and cannot be used.""",
            new GqlMessageParams[] {GqlMessageParams.indexName},
            Condition.INVALID_TRANSACTION_STATE,
            "index was dropped"),
    STATUS_25N13(
            new GqlStatus("25N13"),
            """
                    A %s was accessed after being deleted in this transaction. Verify the transaction statements.""",
            new GqlMessageParams[] {GqlMessageParams.nodeOrRelationship},
            Condition.INVALID_TRANSACTION_STATE,
            "cannot access entity after removal"),
    STATUS_2D000(
            new GqlStatus("2D000"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            ""),
    STATUS_2DN01(
            new GqlStatus("2DN01"),
            """
                    Failed to commit transaction. See debug log for details.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "commit failed"),
    STATUS_2DN02(
            new GqlStatus("2DN02"),
            """
                    Failed to commit constituent transaction. See debug log for details.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "constituent commit failed"),
    STATUS_2DN03(
            new GqlStatus("2DN03"),
            """
                    Failed to terminate transaction. See debug log for details.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "termination failed"),
    STATUS_2DN04(
            new GqlStatus("2DN04"),
            """
                    Failed to terminate constituent transaction. See debug log for details.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "constituent termination failed"),
    STATUS_2DN05(
            new GqlStatus("2DN05"),
            """
                    There was an error on applying the transaction. See logs for more information.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "failed to apply transaction"),
    STATUS_2DN06(
            new GqlStatus("2DN06"),
            """
                    There was an error on appending the transaction. See logs for more information.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "failed to append transaction"),
    STATUS_2DN07(
            new GqlStatus("2DN07"),
            """
                    Unable to commit transaction because it still have non-closed inner transactions.""",
            new GqlMessageParams[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "inner transactions still open"),
    STATUS_40000(
            new GqlStatus("40000"), """
            """, new GqlMessageParams[] {}, Condition.TRANSACTION_ROLLBACK, ""),
    STATUS_40003(
            new GqlStatus("40003"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.TRANSACTION_ROLLBACK,
            "statement completion unknown"),
    STATUS_40N01(
            new GqlStatus("40N01"),
            """
                    Failed to rollback transaction. See debug log for details.""",
            new GqlMessageParams[] {},
            Condition.TRANSACTION_ROLLBACK,
            "rollback failed"),
    STATUS_40N02(
            new GqlStatus("40N02"),
            """
                    Failed to rollback constituent transaction. See debug log for details.""",
            new GqlMessageParams[] {},
            Condition.TRANSACTION_ROLLBACK,
            "constituent rollback failed"),
    STATUS_42000(
            new GqlStatus("42000"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            ""),
    STATUS_42001(
            new GqlStatus("42001"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid syntax"),
    STATUS_42002(
            new GqlStatus("42002"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid reference"),
    STATUS_42004(
            new GqlStatus("42004"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "use of visually confusable identifiers"),
    STATUS_42006(
            new GqlStatus("42006"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge labels below supported minimum"),
    STATUS_42007(
            new GqlStatus("42007"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge labels exceeds supported maximum"),
    STATUS_42008(
            new GqlStatus("42008"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge properties exceeds supported maximum"),
    STATUS_42009(
            new GqlStatus("42009"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node labels below supported minimum"),
    STATUS_42010(
            new GqlStatus("42010"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node labels exceeds supported maximum"),
    STATUS_42011(
            new GqlStatus("42011"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node properties exceeds supported maximum"),
    STATUS_42012(
            new GqlStatus("42012"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node type key labels below supported minimum"),
    STATUS_42013(
            new GqlStatus("42013"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node type key labels exceeds supported maximum"),
    STATUS_42014(
            new GqlStatus("42014"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge type key labels below supported minimum"),
    STATUS_42015(
            new GqlStatus("42015"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge type key labels exceeds supported maximum"),
    STATUS_42I00(
            new GqlStatus("42I00"),
            """
                    `CASE` expressions must have the same number of `WHEN` and `THEN` operands.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid case expression"),
    STATUS_42I01(
            new GqlStatus("42I01"),
            """
                    Invalid use of `%s` inside `FOREACH`.""",
            new GqlMessageParams[] {GqlMessageParams.clause},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid FOREACH"),
    STATUS_42I02(
            new GqlStatus("42I02"),
            """
                    Failed to parse comment. A comment starting with `/*` must also have a closing `*/`.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid comment"),
    STATUS_42I03(
            new GqlStatus("42I03"),
            """
                    A Cypher query has to contain at least one clause.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "empty request"),
    STATUS_42I04(
            new GqlStatus("42I04"),
            """
                    `%s ` cannot be used in a `%s` clause.""",
            new GqlMessageParams[] {GqlMessageParams.expr, GqlMessageParams.clause},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid expression"),
    STATUS_42I05(
            new GqlStatus("42I05"),
            """
                    The `LOAD CSV` `FIELDTERMINATOR` can only be one character wide.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid FIELDTERMINATOR"),
    STATUS_42I06(
            new GqlStatus("42I06"),
            """
                    Invalid input `%s`, expected: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.input, GqlMessageParams.expectedInput},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid input"),
    STATUS_42I07(
            new GqlStatus("42I07"),
            """
                    The given %s integer literal `%s` is invalid.""",
            new GqlMessageParams[] {GqlMessageParams.integerType, GqlMessageParams.input},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid integer literal"),
    STATUS_42I08(
            new GqlStatus("42I08"),
            """
                    The lower bound of the variable length relationship used in the `%s()` function must be 0 or 1.""",
            new GqlMessageParams[] {GqlMessageParams.shortestPathFunc},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid lower bound"),
    STATUS_42I09(
            new GqlStatus("42I09"),
            """
                    Expected `MAP` to contain the same number of keys and values, but got keys `%s` and values `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.keys, GqlMessageParams.values},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid map"),
    STATUS_42I10(
            new GqlStatus("42I10"),
            // %% is used to escape % in formatted block string, i.e. it will become a single %
            """
                    Mixing label expression symbols (`|`, `&`, `!`, and `%%`) with colon (`:`) between labels is not allowed. This expression could be expressed as %s.""",
            new GqlMessageParams[] {GqlMessageParams.syntax},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid label expression"),
    STATUS_42I11(
            new GqlStatus("42I11"),
            """
                    A `%s` name cannot be empty or contain any null-bytes: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.typeOrLabelOrProp, GqlMessageParams.input},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid name"),
    STATUS_42I12(
            new GqlStatus("42I12"),
            """
                    Quantified path patterns cannot be nested.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid nesting of quantified path patterns"),
    STATUS_42I13(
            new GqlStatus("42I13"),
            """
                    The %s call does not provide the required number of arguments: expected `%s` got `%s`.

                    The %s `%s` has the signature: `%s`.""",
            new GqlMessageParams[] {
                GqlMessageParams.procOrFun,
                GqlMessageParams.expectedCount,
                GqlMessageParams.actualCount,
                GqlMessageParams.procOrFun,
                GqlMessageParams.procFun,
                GqlMessageParams.sig
            },
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid number of procedure or function arguments"),
    STATUS_42I14(
            new GqlStatus("42I14"),
            """
                    Exactly one relationship type must be specified for `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.var},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid number of relationship types"),
    STATUS_42I15(
            new GqlStatus("42I15"),
            """
                    Expected exactly one statement per query but got: %s.""",
            new GqlMessageParams[] {GqlMessageParams.actualCount},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid number of statements"),
    STATUS_42I16(
            new GqlStatus("42I16"),
            """
                    Map with keys `%s` is not a valid `POINT`. Use either Cartesian or geographic coordinates.""",
            new GqlMessageParams[] {GqlMessageParams.keys},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid point"),
    STATUS_42I17(
            new GqlStatus("42I17"),
            """
                    A quantifier must not have a lower bound greater than the upper bound.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid quantifier"),
    STATUS_42I18(
            new GqlStatus("42I18"),
            """
                    The aggregation column contains implicit grouping expressions referenced by the variables `%s`. Implicit grouping expressions are variables not explicitly declared as grouping keys.""",
            new GqlMessageParams[] {GqlMessageParams.vars},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid reference to implicitly grouped expressions"),
    STATUS_42I19(
            new GqlStatus("42I19"),
            """
                    Failed to parse string literal. The query must contain an even number of non-escaped quotes.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid string literal"),
    STATUS_42I20(
            new GqlStatus("42I20"),
            """
                    %s expressions cannot contain `%s`. To express a label disjunction use `%s%s` instead.""",
            new GqlMessageParams[] {
                GqlMessageParams.labelOrRelType,
                GqlMessageParams.input,
                GqlMessageParams.isOrColon,
                GqlMessageParams.sanitizedLabelExpression
            },
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid symbol in expression"),
    STATUS_42I22(
            new GqlStatus("42I22"),
            """
                    The right hand side of a `UNION` clause must be a single query.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of UNION"),
    STATUS_42I23(
            new GqlStatus("42I23"),
            """
                    The `%s()` function cannot contain a quantified path pattern.""",
            new GqlMessageParams[] {GqlMessageParams.fun},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid quantified path pattern in shortest path"),
    STATUS_42I24(
            new GqlStatus("42I24"),
            """
                    Aggregate expressions are not allowed inside of %s.""",
            new GqlMessageParams[] {GqlMessageParams.expr},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of aggregate function"),
    STATUS_42I25(
            new GqlStatus("42I25"),
            """
                    `CALL { ... } IN TRANSACTIONS` after a write clause is not supported.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of CALL IN TRANSACTIONS"),
    STATUS_42I26(
            new GqlStatus("42I26"),
            """
                    The `DELETE` clause does not support removing labels from a node. Use `REMOVE`.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid DELETE"),
    STATUS_42I27(
            new GqlStatus("42I27"),
            """
                    `DISTINCT` cannot be used with the '%s()' function.""",
            new GqlMessageParams[] {GqlMessageParams.fun},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of DISTINCT with non-aggregate function"),
    STATUS_42I28(
            new GqlStatus("42I28"),
            """
                    Importing `WITH` can consist only of direct references to outside variables. `%s` is not allowed.""",
            new GqlMessageParams[] {GqlMessageParams.input},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of importing WITH"),
    STATUS_42I29(
            new GqlStatus("42I29"),
            """
                    The `IS` keyword cannot be used together with multiple labels in `%s`. Rewrite the expression as `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.input, GqlMessageParams.replacement},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of IS"),
    STATUS_42I30(
            new GqlStatus("42I30"),
            """
                    Label expressions cannot be used in a %s.""",
            new GqlMessageParams[] {GqlMessageParams.expr},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of label expressions"),
    STATUS_42I31(
            new GqlStatus("42I31"),
            """
                    A `MATCH` clause cannot directly follow an `OPTIONAL MATCH` clause. Use a `WITH` clause between them.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of MATCH"),
    STATUS_42I32(
            new GqlStatus("42I32"),
            """
                    Node and relationship pattern predicates cannot be used in a `%s` clause. They can only be used in a `MATCH` clause or inside a pattern comprehension.""",
            new GqlMessageParams[] {GqlMessageParams.clause},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of node and relationship pattern predicate"),
    STATUS_42I33(
            new GqlStatus("42I33"),
            """
                    Closed Dynamic Union types cannot be appended with `NOT NULL`, specify `NOT NULL` on inner types instead.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of NOT NULL"),
    STATUS_42I34(
            new GqlStatus("42I34"),
            """
                    A pattern expression can only be used to test the existence of a pattern. Use a pattern comprehension instead.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of pattern expression"),
    STATUS_42I35(
            new GqlStatus("42I35"),
            """
                    Relationship type expressions can only be used in a `MATCH` clause.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of relationship type expression"),
    STATUS_42I36(
            new GqlStatus("42I36"),
            """
                    `REPORT STATUS` can only be used when specifying `ON ERROR CONTINUE` or `ON ERROR BREAK`.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of REPORT STATUS"),
    STATUS_42I37(
            new GqlStatus("42I37"),
            """
                    `RETURN` * is not allowed when there are no variables in scope.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of RETURN *"),
    STATUS_42I38(
            new GqlStatus("42I38"),
            """
                    A `RETURN` clause can only be used at the end of a query or subquery.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of RETURN"),
    STATUS_42I39(
            new GqlStatus("42I39"),
            """
                    Mixing the `%s` function with path selectors or explicit match modes is not allowed.""",
            new GqlMessageParams[] {GqlMessageParams.fun},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of shortest path function"),
    STATUS_42I40(
            new GqlStatus("42I40"),
            """
                    `UNION` and `UNION ALL` cannot be combined.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of UNION and UNION ALL"),
    STATUS_42I41(
            new GqlStatus("42I41"),
            """
                    Variable length relationships cannot be used %s.""",
            new GqlMessageParams[] {GqlMessageParams.expression},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of variable length relationship"),
    STATUS_42I42(
            new GqlStatus("42I42"),
            """
                    Cannot use `YIELD` on a void procedure.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of YIELD"),
    STATUS_42I43(
            new GqlStatus("42I43"),
            """
                    `YIELD *` can only be used with a standalone procedure `CALL`.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of YIELD *"),
    STATUS_42I44(
            new GqlStatus("42I44"),
            """
                    Cannot use a join hint for a single node pattern.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid joint hint"),
    STATUS_42I45(
            new GqlStatus("42I45"),
            """
                    Multiple path patterns cannot be used in the same clause in combination with a selective path selector. %s""",
            new GqlMessageParams[] {GqlMessageParams.action},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of multiple path patterns"),
    STATUS_42I46(
            new GqlStatus("42I46"),
            """
                    Node pattern pairs are only supported for quantified path patterns.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of a node pattern pair"),
    STATUS_42I47(
            new GqlStatus("42I47"),
            """
                    Parser Error, `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.wrapped},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "parser error"),
    STATUS_42I48(
            new GqlStatus("42I48"),
            """
                    Subqueries are not allowed in a `MERGE` clause.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of a subquery in MERGE"),
    STATUS_42I49(
            new GqlStatus("42I49"),
            """
                    Unknown inequality operator '!='. The operator for inequality in Cypher is '<>'.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid inequality operator"),
    STATUS_42N00(
            new GqlStatus("42N00"),
            """
                    No such database `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.param1},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such database"),
    STATUS_42N01(
            new GqlStatus("42N01"),
            """
                    No such constituent graph `%s` in composite database `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.param1, GqlMessageParams.param2},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such constituent graph in composite database"),
    STATUS_42N02(
            new GqlStatus("42N02"),
            """
                    Writing in read access mode not allowed.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "writing in read access mode"),
    STATUS_42N03(
            new GqlStatus("42N03"),
            """
                    Writing to multiple graphs in the same transaction is not allowed. Use CALL IN TRANSACTION or create separate transactions in your application.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "writing to multiple graphs"),
    STATUS_42N04(
            new GqlStatus("42N04"),
            """
                    Failed to access `%s` while connected to `%s`. Connect to `%s` directly.""",
            new GqlMessageParams[] {
                GqlMessageParams.constituentOrComposite, GqlMessageParams.sessionDb, GqlMessageParams.compositeDb
            },
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported access of composite database"),
    STATUS_42N05(
            new GqlStatus("42N05"),
            """
                    Failed to access `%s` while connected to `%s`. Connect to `%s` directly or create an alias in the composite database.""",
            new GqlMessageParams[] {GqlMessageParams.db, GqlMessageParams.compositeSessionDb, GqlMessageParams.db},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported access of standard database"),
    STATUS_42N06(
            new GqlStatus("42N06"),
            """
                    %s is not supported on composite databases. %s""",
            new GqlMessageParams[] {GqlMessageParams.action, GqlMessageParams.workaround},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported action on composite database"),
    STATUS_42N07(
            new GqlStatus("42N07"),
            """
                    The variable `%s` is shadowing a variable with the same name from the outer scope and needs to be renamed.""",
            new GqlMessageParams[] {GqlMessageParams.varName},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable already defined"),
    STATUS_42N08(
            new GqlStatus("42N08"),
            """
                    There is no `%s` with the `%s` registered for this database instance.""",
            new GqlMessageParams[] {GqlMessageParams.procOrFunc, GqlMessageParams.procOrFuncNameOrId},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such procedure or function"),
    STATUS_42N09(
            new GqlStatus("42N09"),
            """
                    A user with the name `%s` does not exist.""",
            new GqlMessageParams[] {GqlMessageParams.name},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such user"),
    STATUS_42N10(
            new GqlStatus("42N10"),
            """
                    A role with the name `%s` does not exist.""",
            new GqlMessageParams[] {GqlMessageParams.name},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such role"),
    STATUS_42N11(
            new GqlStatus("42N11"),
            """
                    A [composite] database or alias with the name `%s` already exists.""",
            new GqlMessageParams[] {GqlMessageParams.name},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "database already exists"),
    STATUS_42N12(
            new GqlStatus("42N12"),
            """
                    A user with the name `%s` already exists.""",
            new GqlMessageParams[] {GqlMessageParams.name},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "user already exists"),
    STATUS_42N13(
            new GqlStatus("42N13"),
            """
                    A role with the name `%s` already exists.""",
            new GqlMessageParams[] {GqlMessageParams.name},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "role already exists"),
    STATUS_42N14(
            new GqlStatus("42N14"),
            """
                    `%s` cannot be used together with `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.clause, GqlMessageParams.command},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of command"),
    STATUS_42N15(
            new GqlStatus("42N15"),
            """
                    `%s` is a reserved keyword and cannot be used in this place.""",
            new GqlMessageParams[] {GqlMessageParams.keyword},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of reserved keyword"),
    STATUS_42N16(
            new GqlStatus("42N16"),
            """
                    Only single property %s are supported.""",
            new GqlMessageParams[] {GqlMessageParams.type},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported index or constraint"),
    STATUS_42N17(
            new GqlStatus("42N17"),
            """
                    `%s` is not allowed on the system database.""",
            new GqlMessageParams[] {GqlMessageParams.thing},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported request"),
    STATUS_42N18(
            new GqlStatus("42N18"),
            """
                    The database is in read-only mode.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "read-only database"),
    STATUS_42N20(
            new GqlStatus("42N20"),
            """
                    The list range operator `[ ]` cannot be empty.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "empty list range operator"),
    STATUS_42N21(
            new GqlStatus("42N21"),
            """
                    Expression in `%s` must be aliased (use `AS`).""",
            new GqlMessageParams[] {GqlMessageParams.clause},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unaliased return item"),
    STATUS_42N22(
            new GqlStatus("42N22"),
            """
                    A `COLLECT` subquery must end with a single return column.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "single return column required"),
    STATUS_42N23(
            new GqlStatus("42N23"),
            """
                    The aggregating function must be included in the `%s` clause to be used in the `ORDER BY`.""",
            new GqlMessageParams[] {GqlMessageParams.clause},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing reference to aggregation function"),
    STATUS_42N24(
            new GqlStatus("42N24"),
            """
                    A `WITH` clause is required between `%s` and `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.input1, GqlMessageParams.input2},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing WITH"),
    STATUS_42N25(
            new GqlStatus("42N25"),
            """
                    Procedure call inside a query does not support naming results implicitly. Use `YIELD` instead.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing YIELD"),
    STATUS_42N26(
            new GqlStatus("42N26"),
            """
                    Multiple join hints for the same variable `%s` are not supported.""",
            new GqlMessageParams[] {GqlMessageParams.var},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "multiple join hints on same variable"),
    STATUS_42N28(
            new GqlStatus("42N28"),
            """
                    Only statically inferrable patterns and variables are allowed in `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.name},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "patterns or variables not statically inferrable"),
    STATUS_42N29(
            new GqlStatus("42N29"),
            """
                    Pattern expressions are not allowed to introduce new variables: '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.var},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unbound variables in pattern expression"),
    STATUS_42N31(
            new GqlStatus("42N31"),
            """
                    Expected `%s` to be `%s` in the range `%s` to `%s` but found '%s'.""",
            new GqlMessageParams[] {
                GqlMessageParams.component,
                GqlMessageParams.numberType,
                GqlMessageParams.lower,
                GqlMessageParams.upper,
                GqlMessageParams.value
            },
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "specified number out of range"),
    STATUS_42N32(
            new GqlStatus("42N32"),
            """
                    Parameter maps cannot be used in `%s` patterns. Use a literal map instead.""",
            new GqlMessageParams[] {GqlMessageParams.keyword},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of parameter map"),
    STATUS_42N34(
            new GqlStatus("42N34"),
            """
                    Path cannot be bound in a quantified path pattern.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "path bound in quantified path pattern"),
    STATUS_42N35(
            new GqlStatus("42N35"),
            """
                    The path selector `%s` is not supported within %s path patterns.""",
            new GqlMessageParams[] {GqlMessageParams.selector, GqlMessageParams.quantifiedOrParenthesized},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported path selector in path pattern"),
    STATUS_42N36(
            new GqlStatus("42N36"),
            """
                    Procedure call is missing parentheses.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "procedure call without parentheses"),
    STATUS_42N37(
            new GqlStatus("42N37"),
            """
                    Relationship pattern predicates cannot be use in variable length relationships.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of relationship pattern predicates in variable length relationships"),
    STATUS_42N38(
            new GqlStatus("42N38"),
            """
                    Return items must have unique names.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "duplicate return item name"),
    STATUS_42N39(
            new GqlStatus("42N39"),
            """
                    All subqueries in a `UNION` clause must have the same return column names.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "incompatible return columns"),
    STATUS_42N40(
            new GqlStatus("42N40"),
            """
                    The `%s()` function must contain one relationship pattern.""",
            new GqlMessageParams[] {GqlMessageParams.fun},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "single relationship pattern required"),
    STATUS_42N41(
            new GqlStatus("42N41"),
            """
                    The `reduce()` function requires a `| expression` after the accumulator.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing |-expression"),
    STATUS_42N42(
            new GqlStatus("42N42"),
            """
                    Sub-path assignment is not supported.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported sub-path binding"),
    STATUS_42N44(
            new GqlStatus("42N44"),
            """
                    It is not possible to access the variable `%s` declared before the `%s` clause when using `DISTINCT` or an aggregation.""",
            new GqlMessageParams[] {GqlMessageParams.var, GqlMessageParams.clause},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "inaccessible variable"),
    STATUS_42N45(
            new GqlStatus("42N45"),
            """
                    Unexpected end of input, expected `CYPHER`, `EXPLAIN`, `PROFILE` or a query.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unexpected end of input"),
    STATUS_42N46(
            new GqlStatus("42N46"),
            """
                    `%s` is not a recognized Cypher type.""",
            new GqlMessageParams[] {GqlMessageParams.input},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unexpected type"),
    STATUS_42N47(
            new GqlStatus("42N47"),
            """
                    `CALL { ... } IN TRANSACTIONS` in a `UNION` is not supported.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of UNION"),
    STATUS_42N48(
            new GqlStatus("42N48"),
            """
                    Unknown function `%s`. Verify that the spelling is correct.""",
            new GqlMessageParams[] {GqlMessageParams.fun},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unknown function"),
    STATUS_42N49(
            new GqlStatus("42N49"),
            """
                    Unknown Normal Form: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.input},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unknown normal form"),
    STATUS_42N50(
            new GqlStatus("42N50"),
            """
                    Unknown procedure output: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.outputName},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unknown procedure"),
    STATUS_42N52(
            new GqlStatus("42N52"),
            """
                    `%s` is not a recognized Cypher type.""",
            new GqlMessageParams[] {GqlMessageParams.type},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported type"),
    STATUS_42N53(
            new GqlStatus("42N53"),
            """
                    The quantified path pattern may yield an infinite number of rows under match mode `REPEATABLE ELEMENTS`. Use a path selector or add an upper bound to the quantified path pattern.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsafe usage of repeatable elements"),
    STATUS_42N54(
            new GqlStatus("42N54"),
            """
                    The match mode `%s ` is not supported.""",
            new GqlMessageParams[] {GqlMessageParams.matchMode},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported match mode"),
    STATUS_42N55(
            new GqlStatus("42N55"),
            """
                    The path selector `%s` is not supported.""",
            new GqlMessageParams[] {GqlMessageParams.selector},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported path selector"),
    STATUS_42N56(
            new GqlStatus("42N56"),
            """
                    Properties are not supported in the `%s()` function.""",
            new GqlMessageParams[] {GqlMessageParams.fun},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported use of properties"),
    STATUS_42N57(
            new GqlStatus("42N57"),
            """
                    %s cannot contain any updating clauses.""",
            new GqlMessageParams[] {GqlMessageParams.expr},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of data-modifications in expressions"),
    STATUS_42N58(
            new GqlStatus("42N58"),
            """
                    Nested `CALL { ... } IN TRANSACTIONS` is not supported.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported use of nesting"),
    STATUS_42N59(
            new GqlStatus("42N59"),
            """
                    Variable `%s` already declared.""",
            new GqlMessageParams[] {GqlMessageParams.var},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable already defined"),
    STATUS_42N62(
            new GqlStatus("42N62"),
            """
                    Variable `%s` not defined.""",
            new GqlMessageParams[] {GqlMessageParams.var},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable not defined"),
    STATUS_42N63(
            new GqlStatus("42N63"),
            """
                    All inner types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "inner type with different nullability"),
    STATUS_42N64(
            new GqlStatus("42N64"),
            """
                    A %s path pattern must have at least one %s pattern.""",
            new GqlMessageParams[] {GqlMessageParams.quantifiedOrParenthesized, GqlMessageParams.nodeOrRelationship},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "at least one node or relationship required"),
    STATUS_42N65(
            new GqlStatus("42N65"),
            """
                    The `%s()` function requires bound node variables when it is not part of a `MATCH` clause.""",
            new GqlMessageParams[] {GqlMessageParams.fun},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "node variable not bound"),
    STATUS_42N66(
            new GqlStatus("42N66"),
            """
                    Bound relationships are not allowed in the `%s()` function.""",
            new GqlMessageParams[] {GqlMessageParams.fun},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "relationship variable already bound"),
    STATUS_42N67(
            new GqlStatus("42N67"),
            """
                    Duplicated `%s` parameter.""",
            new GqlMessageParams[] {GqlMessageParams.param},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "duplicate parameter"),
    STATUS_42N68(
            new GqlStatus("42N68"),
            """
                    Variables cannot be defined more than once in a `%s` clause.""",
            new GqlMessageParams[] {GqlMessageParams.clause},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "duplicate variable definition"),
    STATUS_42N69(
            new GqlStatus("42N69"),
            """
                    The `%s()` function is only allowed as a top-level element and not inside a `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.fun, GqlMessageParams.expr},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "function not allowed inside expression"),
    STATUS_42N70(
            new GqlStatus("42N70"),
            """
                    The function `%s()` requires a `WHERE` clause.""",
            new GqlMessageParams[] {GqlMessageParams.fun},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "function without required WHERE clause"),
    STATUS_42N71(
            new GqlStatus("42N71"),
            """
                    A query must conclude with a `RETURN` clause, a `FINISH` clause, an update clause, a unit subquery call, or a procedure call with no `YIELD`.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "incomplete query"),
    STATUS_42N72(
            new GqlStatus("42N72"),
            """
                    %s is only supported on composite databases. %s""",
            new GqlMessageParams[] {GqlMessageParams.action, GqlMessageParams.workaround},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "action only supported on composite databases"),
    STATUS_42N73(
            new GqlStatus("42N73"),
            """
                    `USE` clause must be the first clause of a query or `UNION` part. In a `CALL` sub-query, it can also be the second clause if the first clause is an importing `WITH`.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid placement of USE clause"),
    STATUS_42N74(
            new GqlStatus("42N74"),
            """
                    Failed to access `%s` and `%s`. Child `USE` clauses must target the same graph as their parent query. Run in separate (sub)queries instead.""",
            new GqlMessageParams[] {GqlMessageParams.db1, GqlMessageParams.db2},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid nested USE clause"),
    STATUS_42N75(
            new GqlStatus("42N75"),
            """
                    `%s` is only allowed at the first position of a `USE` clause.""",
            new GqlMessageParams[] {GqlMessageParams.graphFunction},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of graph function"),
    STATUS_42N76(
            new GqlStatus("42N76"),
            """
                    The hint `%s` cannot be fulfilled.""",
            new GqlMessageParams[] {GqlMessageParams.prettifiedHint},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unfulfillable hint"),
    STATUS_42N77(
            new GqlStatus("42N77"),
            """
                    The hint `%s` cannot be fulfilled. The query does not contain a compatible predicate for `%s` on `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.prettifiedHint, GqlMessageParams.entity, GqlMessageParams.variable
            },
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing hint predicate"),
    STATUS_42N78(
            new GqlStatus("42N78"),
            """
                    Node `%s` has already been bound and cannot be modified by the `%s` clause.""",
            new GqlMessageParams[] {GqlMessageParams.node, GqlMessageParams.clause},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable already bound"),
    STATUS_42N81(
            new GqlStatus("42N81"),
            """
                    Expected `%s`, but found `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.expectedParameter, GqlMessageParams.actualParameters},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing parameter"),
    STATUS_42N82(
            new GqlStatus("42N82"),
            """
                    The database `%s` has one or more aliases. Drop the aliases `%s` before dropping the database.""",
            new GqlMessageParams[] {GqlMessageParams.param1, GqlMessageParams.param2},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "database alias without target not allowed"),
    STATUS_42N83(
            new GqlStatus("42N83"),
            """
                    Cannot impersonate a user while password change required.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "impersonation disallowed while password change required"),
    STATUS_42N84(
            new GqlStatus("42N84"),
            """
                    `WHERE` is not allowed by itself. Use `TERMINATE TRANSACTION ... YIELD ... WHERE ...` instead.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing YIELD clause"),
    STATUS_42N85(
            new GqlStatus("42N85"),
            """
                    Allowed and denied database options are mutually exclusive.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot specify both allowed and denied databases"),
    STATUS_42N86(
            new GqlStatus("42N86"),
            """
                    `%s` failed. Parameterized database and graph names do not support wildcards.""",
            new GqlMessageParams[] {GqlMessageParams.failingQueryPart},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "wildcard in parameter"),
    STATUS_42N87(
            new GqlStatus("42N87"),
            """
                    The name `%s` conflicts with the name `%s` of an existing database or alias.""",
            new GqlMessageParams[] {GqlMessageParams.name, GqlMessageParams.otherName},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "name conflict"),
    STATUS_42N88(
            new GqlStatus("42N88"),
            """
                    Permission cannot be granted for `REMOVE IMMUTABLE PRIVILEGE`.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid privilege"),
    STATUS_42N89(
            new GqlStatus("42N89"),
            """
                    Failed evaluating the given driver settings. %s""",
            new GqlMessageParams[] {GqlMessageParams.cause},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid driver settings map"),
    STATUS_42N90(
            new GqlStatus("42N90"),
            """
                    Composite databases cannot be altered (database: `%s`).""",
            new GqlMessageParams[] {GqlMessageParams.name},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "composite database is immutable"),
    STATUS_42N91(
            new GqlStatus("42N91"),
            """
                    Cannot index nested properties (property: `%s`).""",
            new GqlMessageParams[] {GqlMessageParams.property},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot index nested property"),
    STATUS_42N92(
            new GqlStatus("42N92"),
            """
                    Cannot combine old and new auth syntax for the same auth provider.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot combine old and new syntax"),
    STATUS_42N93(
            new GqlStatus("42N93"),
            """
                    No auth given for user.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing auth clause"),
    STATUS_42N94(
            new GqlStatus("42N94"),
            """
                    `ALTER USER` requires at least one clause.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing clause"),
    STATUS_42N95(
            new GqlStatus("42N95"),
            """
                    The combination of provider and id is already in use.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "provider-id comination already specified"),
    STATUS_42N96(
            new GqlStatus("42N96"),
            """
                    User has no auth provider. Add at least one auth provider for the user or consider suspending them.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid user configuration"),
    STATUS_42N97(
            new GqlStatus("42N97"),
            """
                    Clause `%s` is mandatory for auth provider `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.clause, GqlMessageParams.provider},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing mandatory auth clause"),
    STATUS_42NFC(
            new GqlStatus("42NFC"),
            """
                    Authentication and/or authorization could not be validated. See security logs for details.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "auth info validation error"),
    STATUS_42NFD(
            new GqlStatus("42NFD"),
            """
                    Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "credentials expired"),
    STATUS_42NFE(
            new GqlStatus("42NFE"),
            """
                    Authentication and/or authorization info expired.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "auth info expired"),
    STATUS_42NFF(
            new GqlStatus("42NFF"),
            """
                    Access denied, see the security logs for details.""",
            new GqlMessageParams[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "permission/access denied"),
    STATUS_50N00(
            new GqlStatus("50N00"),
            """
                    Internal exception raised `%s`: %s""",
            new GqlMessageParams[] {GqlMessageParams.param1, GqlMessageParams.param2},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "internal error"),
    STATUS_50N01(
            new GqlStatus("50N01"),
            """
                    Remote execution by `%s` raised `%s`: %s""",
            new GqlMessageParams[] {GqlMessageParams.param1, GqlMessageParams.param2, GqlMessageParams.param3},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "remote execution error"),
    STATUS_50N05(
            new GqlStatus("50N05"),
            """
                    Deadlock detected while trying to acquire locks. See log for more details.""",
            new GqlMessageParams[] {},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "deadlock detected"),
    STATUS_50N07(
            new GqlStatus("50N07"),
            """
                    Execution failed. See cause and debug log for details.""",
            new GqlMessageParams[] {},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "transaction terminated or closed"),
    STATUS_50N08(
            new GqlStatus("50N08"),
            """
                    Unable to create `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.indexDescriptorOrName},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "index creation failed"),
    STATUS_50N09(
            new GqlStatus("50N09"),
            """
                    The server transitioned into a server state that is not valid in the current context: `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.stateName},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "invalid server state transition"),
    STATUS_50N10(
            new GqlStatus("50N10"),
            """
                    Unable to drop `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.indexDescriptorOrName},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "index drop failed"),
    STATUS_50N11(
            new GqlStatus("50N11"),
            """
                    Unable to create `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.constraintDescriptorOrName},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "constraint creation failed"),
    STATUS_50N12(
            new GqlStatus("50N12"),
            """
                    Unable to drop `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.constraintDescriptorOrName},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "constraint drop failed"),
    STATUS_50N13(
            new GqlStatus("50N13"),
            """
                    Unable to validate constraint `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.constraintDescriptorOrName},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "constraint validation error"),
    STATUS_50N14(
            new GqlStatus("50N14"),
            """
                    A constraint imposed by the database was violated.""",
            new GqlMessageParams[] {},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "constraint violation"),
    STATUS_50N15(
            new GqlStatus("50N15"),
            """
                    The system attemped to execute an unsupported operation on index `%s`. See debug.log for more information.""",
            new GqlMessageParams[] {GqlMessageParams.indexName},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "unsupported index operation"),
    STATUS_50N21(
            new GqlStatus("50N21"),
            """
                    No `%s` was found for `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.schemaDescriptor, GqlMessageParams.tokenName},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "no such schema descriptor"),
    STATUS_50N42(
            new GqlStatus("50N42"),
            """
                    Unexpected error has occurred. See debug log for details.""",
            new GqlMessageParams[] {},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "unexpected error"),
    STATUS_51N00(
            new GqlStatus("51N00"),
            """
                    Failed to register procedure.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "procedure registration error"),
    STATUS_51N01(
            new GqlStatus("51N01"),
            """
                    The field `%s` in the class `%s` is annotated as a `@Context` field, but it is declared as static. `@Context` fields must be public, non-final and non-static.""",
            new GqlMessageParams[] {GqlMessageParams.fieldName, GqlMessageParams.className},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class field annotation should be public, non-final, and non-static"),
    STATUS_51N02(
            new GqlStatus("51N02"),
            """
                    Unable to set up injection for procedure `%s`. The field `%s` has type `%s` which is not a supported injectable component.""",
            new GqlMessageParams[] {GqlMessageParams.javaClass, GqlMessageParams.javaField, GqlMessageParams.javaType},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unsupported injectable component type"),
    STATUS_51N03(
            new GqlStatus("51N03"),
            """
                    Unable to set up injection for `%s`, failed to access field `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.javaClass, GqlMessageParams.javaField},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unable to access field"),
    STATUS_51N04(
            new GqlStatus("51N04"),
            """
                    The field `%s` on `%s` must be annotated as a `@Context` field in order to store its state.""",
            new GqlMessageParams[] {GqlMessageParams.javaField, GqlMessageParams.javaClass},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "missing class field annotation"),
    STATUS_51N05(
            new GqlStatus("51N05"),
            """
                    The field `%s` on `%s` must be declared non-final and public.""",
            new GqlMessageParams[] {GqlMessageParams.javaField, GqlMessageParams.javaClass},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class field should be public and non-final"),
    STATUS_51N06(
            new GqlStatus("51N06"),
            """
                    The argument at position `%s` in `%s` requires a `@Name` annotation and a non-empty name.""",
            new GqlMessageParams[] {GqlMessageParams.positionNum, GqlMessageParams.javaMethod},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "missing argument name"),
    STATUS_51N07(
            new GqlStatus("51N07"),
            """
                    The `%s` contains a non-default argument before a default argument. Non-default arguments are not allowed to be positioned after default arguments.""",
            new GqlMessageParams[] {GqlMessageParams.procFun},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid ordering of default arguments"),
    STATUS_51N08(
            new GqlStatus("51N08"),
            """
                    The class `%s` must contain exactly one '@UserAggregationResult' method and exactly one '@UserAggregationUpdate' method.""",
            new GqlMessageParams[] {GqlMessageParams.javaClass},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "exactly one @UserAggregationResult method and one @UserAggregationUpdate method required"),
    STATUS_51N09(
            new GqlStatus("51N09"),
            """
                    The '@UserAggregationUpdate' method `%s` in %s must be public and have the return type 'void'.""",
            new GqlMessageParams[] {GqlMessageParams.javaMethod, GqlMessageParams.javaClass},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "@UserAggregationUpdate method must be public and void"),
    STATUS_51N10(
            new GqlStatus("51N10"),
            """
                    The '%s' method `%s` in %s must be public.""",
            new GqlMessageParams[] {GqlMessageParams.javaMethod, GqlMessageParams.methodName, GqlMessageParams.javaClass
            },
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "aggregation method not public"),
    STATUS_51N11(
            new GqlStatus("51N11"),
            """
                    The class `%s` must be public.""",
            new GqlMessageParams[] {GqlMessageParams.javaClass},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class not public"),
    STATUS_51N12(
            new GqlStatus("51N12"),
            """
                    The procedure `%s` has zero output fields and must be defined as void.""",
            new GqlMessageParams[] {GqlMessageParams.proc},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class not void"),
    STATUS_51N13(
            new GqlStatus("51N13"),
            """
                    Unable to register the %s `%s` because the name is already in use.""",
            new GqlMessageParams[] {GqlMessageParams.procOrFun, GqlMessageParams.procFun},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "procedure or function name already in use"),
    STATUS_51N14(
            new GqlStatus("51N14"),
            """
                    The procedure `%s` has a duplicate %s field, `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.proc, GqlMessageParams.javaType, GqlMessageParams.javaField},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "duplicate field name"),
    STATUS_51N15(
            new GqlStatus("51N15"),
            """
                    Type mismatch for map key. Required `STRING`, but found %s.""",
            new GqlMessageParams[] {GqlMessageParams.type},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid map key type"),
    STATUS_51N16(
            new GqlStatus("51N16"),
            """
                    Type mismatch for the default value. Required %s, but found %s.""",
            new GqlMessageParams[] {GqlMessageParams.type, GqlMessageParams.input},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid default value type"),
    STATUS_51N17(
            new GqlStatus("51N17"),
            """
                    Procedures and functions cannot be defined in the root namespace, or use a reserved namespace. Use the package name instead e.g. `org.example.com.%s`.""",
            new GqlMessageParams[] {GqlMessageParams.procFun},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid procedure or function name"),
    STATUS_51N18(
            new GqlStatus("51N18"),
            """
                    The method `%s` has an invalid return type. Procedures must return a stream of records, where each record is of a defined concrete class.""",
            new GqlMessageParams[] {GqlMessageParams.javaMethod},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid method return type"),
    STATUS_51N20(
            new GqlStatus("51N20"),
            """
                    The field `%s` is not injectable. Ensure the field is marked as public and non-final.""",
            new GqlMessageParams[] {GqlMessageParams.javaField},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot inject field"),
    STATUS_51N21(
            new GqlStatus("51N21"),
            """
                    The procedure registration failed because the procedure registry was busy. Try again.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "procedure registry is busy"),
    STATUS_51N22(
            new GqlStatus("51N22"),
            """
                    Finding the shortest path for the given pattern requires an exhaustive search. To enable exhaustive searches, set `cypher.forbid_exhaustive_shortestpath` to `false`.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "exhaustive shortest path search disabled"),
    STATUS_51N23(
            new GqlStatus("51N23"),
            """
                    Cannot find the shortest path when the start and end nodes are the same. To enable this behavior, set `dbms.cypher.forbid_shortestpath_common_nodes` to `false`.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cyclic shortest path search disabled"),
    STATUS_51N24(
            new GqlStatus("51N24"),
            """
                    Could not find a query plan within given time and space limits.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "insufficient resources for plan search"),
    STATUS_51N25(
            new GqlStatus("51N25"),
            """
                    Cannot compile query due to excessive updates to indexes and constraints.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "database is busy"),
    STATUS_51N26(
            new GqlStatus("51N26"),
            """
                    %s is not available. This implementation of Cypher does not support %s.""",
            new GqlMessageParams[] {GqlMessageParams.thing, GqlMessageParams.featureDescr},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported in this version"),
    STATUS_51N27(
            new GqlStatus("51N27"),
            """
                    The administration command `%s` is not supported in Community Edition and Aura.""",
            new GqlMessageParams[] {GqlMessageParams.param},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported in this edition"),
    STATUS_51N28(
            new GqlStatus("51N28"),
            """
                    This Cypher command must be executed against the database `system`.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported by this database"),
    STATUS_51N29(
            new GqlStatus("51N29"),
            """
                    `%s` must be executed on the `LEADER` server.""",
            new GqlMessageParams[] {GqlMessageParams.commandName},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported by this server"),
    STATUS_51N30(
            new GqlStatus("51N30"),
            """
                    %s is not supported in %s.

                    eg: Impersonation is not supported in a database with native auth disabled.""",
            new GqlMessageParams[] {GqlMessageParams.thing, GqlMessageParams.context},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported with this configuration"),
    STATUS_51N31(
            new GqlStatus("51N31"),
            """
                    %s is not supported in %s.

                    eg: URL pattern is not supported in LOAD privileges.""",
            new GqlMessageParams[] {GqlMessageParams.thing, GqlMessageParams.context},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported"),
    STATUS_51N32(
            new GqlStatus("51N32"),
            """
                    Server is in panic.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "server panic"),
    STATUS_51N33(
            new GqlStatus("51N33"),
            """
                    This member failed to replicate transaction, try again.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "replication error"),
    STATUS_51N34(
            new GqlStatus("51N34"),
            """
                    Failed to write to the database due to a cluster leader change. Retrying your request at a later time may succeed.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "write transaction failed due to leader change"),
    STATUS_51N35(
            new GqlStatus("51N35"),
            """
                    The location of `%s` has changed while the transaction was running.""",
            new GqlMessageParams[] {GqlMessageParams.name},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "database location changed"),
    STATUS_51N36(
            new GqlStatus("51N36"),
            """
                    There is not enough memory to perform the current task.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "out of memory"),
    STATUS_51N37(
            new GqlStatus("51N37"),
            """
                    There is not enough stack size to perform the current task.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "stack overflow"),
    STATUS_51N38(
            new GqlStatus("51N38"),
            """
                    There are insufficient threads available for executing the current task.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "failed to acquire execution thread"),
    STATUS_51N39(
            new GqlStatus("51N39"),
            """
                    Expected set of files not found on disk. Please restore from backup.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "raft log corrupted"),
    STATUS_51N40(
            new GqlStatus("51N40"),
            """
                    Database `%s` failed to start. Try restarting it.""",
            new GqlMessageParams[] {GqlMessageParams.namedDatabaseId},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unable to start database"),
    STATUS_51N41(
            new GqlStatus("51N41"),
            """
                    Server or database admin operation not possible.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "admin operation failed"),
    STATUS_51N42(
            new GqlStatus("51N42"),
            """
                    Unable to check if allocator '%s' is available.""",
            new GqlMessageParams[] {GqlMessageParams.allocator},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unknown allocator"),
    STATUS_51N43(
            new GqlStatus("51N43"),
            """
                    Cannot deallocate server(s) %s.""",
            new GqlMessageParams[] {GqlMessageParams.servers},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot deallocate servers"),
    STATUS_51N44(
            new GqlStatus("51N44"),
            """
                    Cannot drop server '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.server},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot drop server"),
    STATUS_51N45(
            new GqlStatus("51N45"),
            """
                    Cannot cordon server '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.server},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot cordon server"),
    STATUS_51N46(
            new GqlStatus("51N46"),
            """
                    Cannot alter server '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.server},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot alter server"),
    STATUS_51N47(
            new GqlStatus("51N47"),
            """
                    Cannot rename server '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.server},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot rename server"),
    STATUS_51N48(
            new GqlStatus("51N48"),
            """
                    Cannot enable server '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.server},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot enable server"),
    STATUS_51N49(
            new GqlStatus("51N49"),
            """
                    Cannot alter database '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.databaseName},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot alter database"),
    STATUS_51N50(
            new GqlStatus("51N50"),
            """
                    Cannot recreate database '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.databaseName},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot recreate database"),
    STATUS_51N51(
            new GqlStatus("51N51"),
            """
                    Cannot create database '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.databaseName},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot create database"),
    STATUS_51N52(
            new GqlStatus("51N52"),
            """
                    Number of primaries '%s' may not exceed %s and needs to be at least 1.""",
            new GqlMessageParams[] {GqlMessageParams.number, GqlMessageParams.maxPrimaries},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot alter database topology"),
    STATUS_51N53(
            new GqlStatus("51N53"),
            """
                    Number of secondaries '%s' may not exceed %s and needs to be at least 0.""",
            new GqlMessageParams[] {GqlMessageParams.number, GqlMessageParams.maxSecondaries},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot alter database topology"),
    STATUS_51N54(
            new GqlStatus("51N54"),
            """
                    Failed to calculate reallocation for databases. %s""",
            new GqlMessageParams[] {GqlMessageParams.wrappedError},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot reallocate"),
    STATUS_51N55(
            new GqlStatus("51N55"),
            """
                    Failed to create the database `%s`. The limit of databases is reached. Either increase the limit using the config setting `%s` or drop a database.""",
            new GqlMessageParams[] {GqlMessageParams.databaseName, GqlMessageParams.setting},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "database limit reached"),
    STATUS_51N56(
            new GqlStatus("51N56"),
            """
                    The number of primary constrained seeding servers '%s', is larger than the desired number of primary allocations '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.primaryConstrainedServers, GqlMessageParams.desiredPrimaries},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "topology out of range"),
    STATUS_51N57(
            new GqlStatus("51N57"),
            """
                    Unexpected error while picking allocations - primary exceeded.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "generic topology modification error"),
    STATUS_51N58(
            new GqlStatus("51N58"),
            "",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            ""),
    STATUS_51N59(
            new GqlStatus("51N59"),
            """
                    The DBMS is unable to handle the request, please retry later or contact the system operator. More information is present in the logs.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "internal resource exhaustion"),
    STATUS_51N60(
            new GqlStatus("51N60"),
            """
                    The DBMS is unable to determine the enterprise license acceptance status.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unable to check enterprise license acceptance"),
    STATUS_51N61(
            new GqlStatus("51N61"),
            """
                    Index `%s` population failed.""",
            new GqlMessageParams[] {GqlMessageParams.indexName},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "index population failed"),
    STATUS_51N62(
            new GqlStatus("51N62"),
            """
                    Unable to use index `%s` because it is in a failed state. See logs for more information.""",
            new GqlMessageParams[] {GqlMessageParams.indexName},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "index is in a failed state"),
    STATUS_51N63(
            new GqlStatus("51N63"),
            """
                    Index is not ready yet. Wait until it finishes populating and retry the transaction.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "index is still populating"),
    STATUS_51N64(
            new GqlStatus("51N64"),
            """
                    The index dropped while sampling.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "index dropped while sampling"),
    STATUS_51N65(
            new GqlStatus("51N65"),
            """
                    Vector index `%s` has a dimensionality of `%s`, but indexed vectors have `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.indexName, GqlMessageParams.indexDim, GqlMessageParams.vectorsDim},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "vector index dimensionality mismatch"),
    STATUS_51N66(
            new GqlStatus("51N66"),
            """
                    Insufficient resources to complete the request.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "resource exhaustion"),
    STATUS_51N67(
            new GqlStatus("51N67"),
            """
                    Unexpected selector `%s` at `%s`, expected selector to be a `%s` selector.""",
            new GqlMessageParams[] {GqlMessageParams.selectorType, GqlMessageParams.in, GqlMessageParams.selectorType2},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "incorrect selector type"),
    STATUS_51N68(
            new GqlStatus("51N68"),
            """
                    Change Data Capture is not currently enabled for this database.""",
            new GqlMessageParams[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "CDC is disabled for this database"),
    STATUS_51N69(
            new GqlStatus("51N69"),
            """
                    It is not possible to perform %s on the system database.""",
            new GqlMessageParams[] {GqlMessageParams.action},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "system database is immutable"),
    STATUS_52N01(
            new GqlStatus("52N01"),
            """
                    Execution of the procedure `%s` timed out after %s %s.""",
            new GqlMessageParams[] {GqlMessageParams.proc, GqlMessageParams.time, GqlMessageParams.unit},
            Condition.PROCEDURE_EXCEPTION,
            "procedure execution timeout"),
    STATUS_52N02(
            new GqlStatus("52N02"),
            """
                    Execution of the procedure `%s` failed.""",
            new GqlMessageParams[] {GqlMessageParams.proc},
            Condition.PROCEDURE_EXCEPTION,
            "procedure execution error"),
    STATUS_52N03(
            new GqlStatus("52N03"),
            """
                    Execution of the procedure `%s` failed due to an invalid specified execution mode `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.proc, GqlMessageParams.xmode},
            Condition.PROCEDURE_EXCEPTION,
            "invalid procedure execution mode"),
    STATUS_52N04(
            new GqlStatus("52N04"),
            """
                    Temporarily could not execute the procedure `%s`. Please retry.""",
            new GqlMessageParams[] {GqlMessageParams.proc},
            Condition.PROCEDURE_EXCEPTION,
            "transient procedure execution error"),
    STATUS_52N05(
            new GqlStatus("52N05"),
            """
                    Can't invoke procedure on this member because it is not a secondary for database '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.namedDatabaseId},
            Condition.PROCEDURE_EXCEPTION,
            "cannot invoke procedure on a primary"),
    STATUS_52N06(
            new GqlStatus("52N06"),
            """
                    Unexpected number of arguments (expected 0-2 but received %s).""",
            new GqlMessageParams[] {GqlMessageParams.nbr},
            Condition.PROCEDURE_EXCEPTION,
            "invalid number of arguments to checkConnectivity"),
    STATUS_52N07(
            new GqlStatus("52N07"),
            """
                    Unrecognised port name '%s (valid values are: %s).""",
            new GqlMessageParams[] {GqlMessageParams.port, GqlMessageParams.values},
            Condition.PROCEDURE_EXCEPTION,
            "invalid port argument to checkConnectivity"),
    STATUS_52N08(
            new GqlStatus("52N08"),
            """
                    Unable to parse server id '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.serverIdString},
            Condition.PROCEDURE_EXCEPTION,
            "invalid server id argument to checkConnectivity"),
    STATUS_52N09(
            new GqlStatus("52N09"),
            """
                    Cannot get routing table for %s because Bolt is not enabled. Please update your configuration for 'server.bolt.enabled'.""",
            new GqlMessageParams[] {GqlMessageParams.databaseAlias},
            Condition.PROCEDURE_EXCEPTION,
            "bolt is not enabled"),
    STATUS_52N10(
            new GqlStatus("52N10"),
            """
                    An address key is included in the query string provided to the GetRoutingTableProcedure, but its value could not be parsed.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "unknown address key"),
    STATUS_52N11(
            new GqlStatus("52N11"),
            """
                    An unexpected error has occurred. Please refer to the server's debug log for more information.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "generic topology procedure error"),
    STATUS_52N12(
            new GqlStatus("52N12"),
            """
                    The previous default database %s is still running.""",
            new GqlMessageParams[] {GqlMessageParams.oldDatabaseName},
            Condition.PROCEDURE_EXCEPTION,
            "cannot change default database"),
    STATUS_52N13(
            new GqlStatus("52N13"),
            """
                    New default database %s does not exist.""",
            new GqlMessageParams[] {GqlMessageParams.databaseName},
            Condition.PROCEDURE_EXCEPTION,
            "unknown default database"),
    STATUS_52N14(
            new GqlStatus("52N14"),
            """
                    System database cannot be set as default.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "system cannot be default database"),
    STATUS_52N15(
            new GqlStatus("52N15"),
            """
                    Provided allocator '%s' is not available or was not initialized!""",
            new GqlMessageParams[] {GqlMessageParams.allocator},
            Condition.PROCEDURE_EXCEPTION,
            "unknown allocator"),
    STATUS_52N16(
            new GqlStatus("52N16"),
            """
                    Invalid arguments to procedure.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "invalid procedure argument list"),
    STATUS_52N17(
            new GqlStatus("52N17"),
            """
                    Setting/removing the quarantine marker failed.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "quarantine change failed"),
    STATUS_52N18(
            new GqlStatus("52N18"),
            """
                    The number of seeding servers '%s' is larger than the defined number of allocations '%s'.""",
            new GqlMessageParams[] {GqlMessageParams.nbrSeedingServers, GqlMessageParams.nbrAllocations},
            Condition.PROCEDURE_EXCEPTION,
            "too many seeders"),
    STATUS_52N19(
            new GqlStatus("52N19"),
            """
                    The specified seeding server with id '%s' could not be found.""",
            new GqlMessageParams[] {GqlMessageParams.serverId},
            Condition.PROCEDURE_EXCEPTION,
            "unknown seeder"),
    STATUS_52N20(
            new GqlStatus("52N20"),
            """
                    The recreation of a database is not supported when seed updating is not enabled.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "seed updating not enabled"),
    STATUS_52N21(
            new GqlStatus("52N21"),
            """
                    Failed to clean the system graph.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "failed to clean the system graph"),
    STATUS_52N22(
            new GqlStatus("52N22"),
            """
                    Invalid argument `%s` for `%s` on procedure `%s`. `%s` expected format is `%s`.""",
            new GqlMessageParams[] {
                GqlMessageParams.arg,
                GqlMessageParams.param,
                GqlMessageParams.proc,
                GqlMessageParams.param,
                GqlMessageParams.paramFmt
            },
            Condition.PROCEDURE_EXCEPTION,
            "invalid procedure argument"),
    STATUS_52N24(
            new GqlStatus("52N24"),
            """
                    Failed to reload procedures. See logs for more information.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "failed to reload procedures"),
    STATUS_52N25(
            new GqlStatus("52N25"),
            """
                    JMX error while accessing `%s`. See logs for more information.""",
            new GqlMessageParams[] {GqlMessageParams.param},
            Condition.PROCEDURE_EXCEPTION,
            "JMX error"),
    STATUS_52N26(
            new GqlStatus("52N26"),
            """
                    Invalid change identifier.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "invalid change identifier"),
    STATUS_52N27(
            new GqlStatus("52N27"),
            """
                    The commit timestamp for the provided transaction ID does not match the one in the transaction log.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "invalid commit timestamp"),
    STATUS_52N28(
            new GqlStatus("52N28"),
            """
                    `%s` is not a valid change identifier. Transaction ID `%s` does not exist.""",
            new GqlMessageParams[] {GqlMessageParams.param1, GqlMessageParams.param2},
            Condition.PROCEDURE_EXCEPTION,
            "invalid transaction id"),
    STATUS_52N29(
            new GqlStatus("52N29"),
            """
                    Given ChangeIdentifier describes a transaction that occurred before any enrichment records exist.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "outdated change identifier"),
    STATUS_52N30(
            new GqlStatus("52N30"),
            """
                    Given ChangeIdentifier describes a transaction that hasn't yet occurred.""",
            new GqlMessageParams[] {},
            Condition.PROCEDURE_EXCEPTION,
            "future change identifier"),
    STATUS_52N31(
            new GqlStatus("52N31"),
            """
                    Change identifier `%s` does not belong to this database.""",
            new GqlMessageParams[] {GqlMessageParams.param},
            Condition.PROCEDURE_EXCEPTION,
            "wrong database"),
    STATUS_52N32(
            new GqlStatus("52N32"),
            """
                    Change identifier `%s` has an invalid sequence number `%s`.""",
            new GqlMessageParams[] {GqlMessageParams.param1, GqlMessageParams.param2},
            Condition.PROCEDURE_EXCEPTION,
            "invalid sequence number"),
    STATUS_52U00(
            new GqlStatus("52U00"),
            """
                    Execution of the procedure `%s` failed due to `%s`: `%s`""",
            new GqlMessageParams[] {GqlMessageParams.proc, GqlMessageParams.clazz, GqlMessageParams.msg},
            Condition.PROCEDURE_EXCEPTION,
            "custom procedure execution error cause"),
    STATUS_G1000(
            new GqlStatus("G1000"),
            """
            """,
            new GqlMessageParams[] {},
            Condition.DEPENDENT_OBJECT_ERROR,
            ""),
    STATUS_G1001(
            new GqlStatus("G1001"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DEPENDENT_OBJECT_ERROR,
            "edges still exist"),
    STATUS_G1002(
            new GqlStatus("G1002"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DEPENDENT_OBJECT_ERROR,
            "endpoint node is deleted"),
    STATUS_G1003(
            new GqlStatus("G1003"),
            """
                    """,
            new GqlMessageParams[] {},
            Condition.DEPENDENT_OBJECT_ERROR,
            "endpoint node not in current working graph"),
    STATUS_G2000(
            new GqlStatus("G2000"), """
            """, new GqlMessageParams[] {}, Condition.GRAPH_TYPE_VIOLATION, "");

    private final GqlStatus gqlStatus;
    private final String message;
    private final GqlMessageParams[] statusParameterKeys;
    private final String subCondition;
    private final Condition condition;

    GqlStatusInfoCodes(
            GqlStatus gqlStatus,
            String message,
            GqlMessageParams[] statusParameterKeys,
            Condition condition,
            String subCondition) {
        this.gqlStatus = gqlStatus;
        this.statusParameterKeys = statusParameterKeys;
        this.message = message.formatted(Arrays.stream(statusParameterKeys)
                .map(GqlMessageParams::toParamFormat)
                .toArray());
        this.condition = condition;
        this.subCondition = subCondition;
    }

    @Override
    public GqlStatus getGqlStatus() {
        return gqlStatus;
    }

    @Override
    public String getStatusString() {
        return gqlStatus.gqlStatusString();
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public String getMessage(List<String> params) {
        return GqlStatusInfoCodes.getMessage(message, params);
    }

    public static String getMessage(String message, List<String> params) {
        // Find which parameters a message has
        Pattern p = Pattern.compile("\\$([a-z][a-zA-Z0-9]*([0-9]?[A-Z][a-z0-9]*)*)");
        Matcher matcher = p.matcher(message);
        List<String> foundParams = new ArrayList<>();
        while (matcher.find()) {
            // add the parameter key, without the $ at the beginning
            foundParams.add(matcher.group(1));
        }
        // Populate the found parameters with the values sent in
        // Stopping early if the sizes differ, but still replaces as much as possible
        Map<GqlMessageParams, String> paramMap = new HashMap<>();
        for (int i = 0; i < params.size() && i < foundParams.size(); i++) {
            paramMap.put(GqlMessageParams.valueOf(foundParams.get(i)), params.get(i));
        }
        return getMessage(message, paramMap);
    }

    @Override
    public String getMessage(Map<GqlMessageParams, String> parameterMap) {
        return GqlStatusInfoCodes.getMessage(message, parameterMap);
    }

    public static String getMessage(String message, Map<GqlMessageParams, String> parameterMap) {
        String result = message;
        for (var entry : parameterMap.entrySet()) {
            result = result.replaceAll("\\$" + entry.getKey(), Matcher.quoteReplacement(entry.getValue()));
        }
        return result;
    }

    @Override
    public String getSubCondition() {
        return subCondition;
    }

    @Override
    public Condition getCondition() {
        return condition;
    }

    @Override
    public String[] getStatusParameterKeys() {
        return Arrays.stream(statusParameterKeys).map(GqlMessageParams::name).toArray(String[]::new);
    }
}
