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

import java.util.List;

public enum GqlStatusInfoCodes implements GqlStatusInfo {
    STATUS_00000(new GqlStatus("00000"), """
            """, new String[] {}, Condition.SUCCESSFUL_COMPLETION, ""),
    STATUS_00001(
            new GqlStatus("00001"),
            """
            """,
            new String[] {},
            Condition.SUCCESSFUL_COMPLETION,
            "omitted result"),
    STATUS_00N50(
            new GqlStatus("00N50"),
            """
            The database `%s` does not exist. Verify that the spelling is correct or create the database for the command to take effect.""",
            new String[] {"db"},
            Condition.SUCCESSFUL_COMPLETION,
            "home database not found"),
    STATUS_00N70(
            new GqlStatus("00N70"),
            """
            `%s` has no effect. The role or privilege is already assigned.""",
            new String[] {"cmd"},
            Condition.SUCCESSFUL_COMPLETION,
            "role or privilege already assigned"),
    STATUS_00N71(
            new GqlStatus("00N71"),
            """
            `%s` has no effect. The role or privilege is not assigned.""",
            new String[] {"cmd"},
            Condition.SUCCESSFUL_COMPLETION,
            "role or privilege not assigned"),
    STATUS_00N72(
            new GqlStatus("00N72"),
            """
            The auth provider `%s` is not defined in the configuration. Verify that the spelling is correct or define `%s` in the configuration.""",
            new String[] {"provider", "provider"},
            Condition.SUCCESSFUL_COMPLETION,
            "the auth provider is not defined"),
    STATUS_00N80(
            new GqlStatus("00N80"),
            """
            `ENABLE SERVER` has no effect. Server `%s` is already enabled. Verify that this is the intended server.""",
            new String[] {"server"},
            Condition.SUCCESSFUL_COMPLETION,
            "server already enabled"),
    STATUS_00N81(
            new GqlStatus("00N81"),
            """
            `CORDON SERVER` has no effect. Server `%s` is already cordoned. Verify that this is the intended server.""",
            new String[] {"server"},
            Condition.SUCCESSFUL_COMPLETION,
            "server already cordoned"),
    STATUS_00N82(
            new GqlStatus("00N82"),
            """
            `REALLOCATE DATABASES` has no effect. No databases were reallocated. No better allocation is currently possible.""",
            new String[] {},
            Condition.SUCCESSFUL_COMPLETION,
            "no databases reallocated"),
    STATUS_00N83(
            new GqlStatus("00N83"),
            """
            Cordoned servers existed when making an allocation decision. Server(s) `%s` are cordoned. This can impact allocation decisions.""",
            new String[] {"server_list"},
            Condition.SUCCESSFUL_COMPLETION,
            "cordoned servers existed during allocation"),
    STATUS_00N84(
            new GqlStatus("00N84"),
            """
            `ALTER DATABASE` has no effect. The requested topology matched the current topology. No allocations were changed.""",
            new String[] {},
            Condition.SUCCESSFUL_COMPLETION,
            "requested topology matched current topology"),
    STATUS_00NA0(
            new GqlStatus("00NA0"),
            """
            `%s` has no effect. `%s` already exists.""",
            new String[] {"cmd", "index_constr_pat"},
            Condition.SUCCESSFUL_COMPLETION,
            "index or constraint already exists"),
    STATUS_00NA1(
            new GqlStatus("00NA1"),
            """
            `%s` has no effect. `%s` does not exist.""",
            new String[] {"cmd", "index_constr_name"},
            Condition.SUCCESSFUL_COMPLETION,
            "index or constraint does not exist"),
    STATUS_01000(new GqlStatus("01000"), """
            """, new String[] {}, Condition.WARNING, ""),
    STATUS_01004(
            new GqlStatus("01004"),
            """
            """,
            new String[] {},
            Condition.WARNING,
            "string data, right truncation"),
    STATUS_01G03(
            new GqlStatus("01G03"), """
            """, new String[] {}, Condition.WARNING, "graph does not exist"),
    STATUS_01G04(
            new GqlStatus("01G04"),
            """
            """,
            new String[] {},
            Condition.WARNING,
            "graph type does not exist"),
    STATUS_01G11(
            new GqlStatus("01G11"),
            """
            """,
            new String[] {},
            Condition.WARNING,
            "null value eliminated in set function"),
    STATUS_01N00(
            new GqlStatus("01N00"),
            """
            %s""",
            new String[] {"msg"},
            Condition.WARNING,
            "feature deprecated"),
    STATUS_01N01(
            new GqlStatus("01N01"),
            """
            `%s` is deprecated. It is replaced by `%s`.""",
            new String[] {"thing1", "thing2"},
            Condition.WARNING,
            "feature deprecated with replacement"),
    STATUS_01N02(
            new GqlStatus("01N02"),
            """
            `%s` is deprecated and will be removed without a replacement.""",
            new String[] {"thing"},
            Condition.WARNING,
            "feature deprecated without replacement"),
    STATUS_01N03(
            new GqlStatus("01N03"),
            """
            `%s` for procedure `%s` is deprecated.""",
            new String[] {"field", "proc"},
            Condition.WARNING,
            "procedure field deprecated"),
    STATUS_01N30(
            new GqlStatus("01N30"),
            """
            Unable to create a plan with `JOIN ON %s`. Try to change the join key(s) or restructure your query.""",
            new String[] {"var_list"},
            Condition.WARNING,
            "join hint unfulfillable"),
    STATUS_01N31(
            new GqlStatus("01N31"),
            """
            Unable to create a plan with `%s` because the index does not exist.""",
            new String[] {"index_descr"},
            Condition.WARNING,
            "hinted index not found"),
    STATUS_01N40(
            new GqlStatus("01N40"),
            """
            The query cannot be executed with `%s`, `%s` is used. Cause: `%s`.""",
            new String[] {"preparser_input1", "preparser_input2", "msg"},
            Condition.WARNING,
            "unsupported runtime"),
    STATUS_01N42(
            new GqlStatus("01N42"),
            """
            Unknown warning.""",
            new String[] {},
            Condition.WARNING,
            "unknown warning"),
    STATUS_01N50(
            new GqlStatus("01N50"),
            """
            The label `%s` does not exist. Verify that the spelling is correct.""",
            new String[] {"label"},
            Condition.WARNING,
            "unknown label"),
    STATUS_01N51(
            new GqlStatus("01N51"),
            """
            The relationship type `%s` does not exist. Verify that the spelling is correct.""",
            new String[] {"reltype"},
            Condition.WARNING,
            "unknown relationship type"),
    STATUS_01N52(
            new GqlStatus("01N52"),
            """
            The property `%s` does not exist. Verify that the spelling is correct.""",
            new String[] {"propkey"},
            Condition.WARNING,
            "unknown property key"),
    STATUS_01N60(
            new GqlStatus("01N60"),
            """
            The query plan cannot be cached and is not executable without `EXPLAIN` due to the undefined parameter(s) `%s`. Provide the parameter(s).""",
            new String[] {"param_list"},
            Condition.WARNING,
            "parameter missing"),
    STATUS_01N61(
            new GqlStatus("01N61"),
            """
            The expression `%s` cannot be satisfied because relationships must have exactly one type.""",
            new String[] {"label_expr"},
            Condition.WARNING,
            "unsatisfiable relationship type expression"),
    STATUS_01N62(
            new GqlStatus("01N62"),
            """
            The procedure `%s` generates the warning `%s`.""",
            new String[] {"proc", "msg"},
            Condition.WARNING,
            "procedure or function execution warning"),
    STATUS_01N63(
            new GqlStatus("01N63"),
            """
            `%s` is repeated in `%s`, which leads to no results.""",
            new String[] {"var", "pat"},
            Condition.WARNING,
            "repeated relationship reference"),
    STATUS_01N70(
            new GqlStatus("01N70"),
            """
            `%s` has no effect. %s Make sure nothing is misspelled. This notification will become an error in a future major version.""",
            new String[] {"cmd", "msg"},
            Condition.WARNING,
            "impossible revoke command"),
    STATUS_01N71(
            new GqlStatus("01N71"),
            """
            External auth for user is not enabled. Use setting `dbms.security.require_local_user` to enable external auth.""",
            new String[] {},
            Condition.WARNING,
            "setting not enabled"),
    STATUS_01U00(
            new GqlStatus("01U00"),
            """
            Execution of the procedure `$proc` generated warning `$param2`: `$param3`.""",
            new String[] {"proc", "param2", "param3"},
            Condition.WARNING,
            "custom procedure warning cause"),
    STATUS_02000(new GqlStatus("02000"), """
            """, new String[] {}, Condition.NO_DATA, ""),
    STATUS_02N42(
            new GqlStatus("02N42"),
            """
            Unknown GQLSTATUS from old server.""",
            new String[] {},
            Condition.NO_DATA,
            "unknown subcondition"),
    STATUS_03000(new GqlStatus("03000"), """
            """, new String[] {}, Condition.INFORMATIONAL, ""),
    STATUS_03N42(
            new GqlStatus("03N42"),
            """
            Unknown notification.""",
            new String[] {},
            Condition.INFORMATIONAL,
            "unknown notification"),
    STATUS_03N60(
            new GqlStatus("03N60"),
            """
            The variable `%s` in the subquery uses the same name as a variable from the outer query. Use `WITH %s` in the subquery to import the one from the outer scope unless you want it to be a new variable.""",
            new String[] {"var", "var"},
            Condition.INFORMATIONAL,
            "subquery variable shadowing"),
    STATUS_03N90(
            new GqlStatus("03N90"),
            """
            The disconnected patterns `%s` build a cartesian product. A cartesian product may produce a large amount of data and slow down query processing.""",
            new String[] {"pat"},
            Condition.INFORMATIONAL,
            "cartesian product"),
    STATUS_03N91(
            new GqlStatus("03N91"),
            """
            The provided pattern `%s` is unbounded. Shortest path with an unbounded pattern may result in long execution times. Use an upper limit (e.g. `[*..5]`) on the number of node hops in your pattern.""",
            new String[] {"pat"},
            Condition.INFORMATIONAL,
            "unbounded variable length pattern"),
    STATUS_03N92(
            new GqlStatus("03N92"),
            """
            The query runs with exhaustive shortest path due to the existential predicate(s) `%s`. It may be possible to use `WITH` to separate the `MATCH` from the existential predicate(s).""",
            new String[] {"pred_list"},
            Condition.INFORMATIONAL,
            "exhaustive shortest path"),
    STATUS_03N93(
            new GqlStatus("03N93"),
            """
            `LOAD CSV` in combination with `MATCH` or `MERGE` on a label that does not have an index may result in long execution times. Consider adding an index for label `%s`.""",
            new String[] {"label"},
            Condition.INFORMATIONAL,
            "no applicable index"),
    STATUS_03N94(
            new GqlStatus("03N94"),
            """
            The query execution plan contains the `Eager` operator. `LOAD CSV` in combination with `Eager` can consume a lot of memory.""",
            new String[] {},
            Condition.INFORMATIONAL,
            "eager operator"),
    STATUS_03N95(
            new GqlStatus("03N95"),
            """
            An index exists on label/type(s) `%s`. It is not possible to use indexes for dynamic properties. Consider using static properties.""",
            new String[] {"label_list"},
            Condition.INFORMATIONAL,
            "dynamic property"),
    STATUS_08000(new GqlStatus("08000"), """
            """, new String[] {}, Condition.CONNECTION_EXCEPTION, ""),
    STATUS_08007(
            new GqlStatus("08007"),
            """
            """,
            new String[] {},
            Condition.CONNECTION_EXCEPTION,
            "transaction resolution unknown"),
    STATUS_08N00(
            new GqlStatus("08N00"),
            """
            Unable to connect to `$dbName`. Unable to get bolt address of the leader. Check the status of the database. Retrying your request at a later time may succeed.""",
            new String[] {"dbName"},
            Condition.CONNECTION_EXCEPTION,
            "unable to connect to database"),
    STATUS_08N01(
            new GqlStatus("08N01"),
            """
            Unable to write to `$dbName` on this server. Server-side routing is disabled. Either connect to the database leader directly or enable server-side routing by setting `$routingEnabledSetting=true`.""",
            new String[] {"dbName", "routingEnabledSetting"},
            Condition.CONNECTION_EXCEPTION,
            "unable to write to database"),
    STATUS_08N02(
            new GqlStatus("08N02"),
            """
            Unable to connect to database `$dbName`. Server-side routing is disabled. Either connect to the `$db` directly, or enable server-side routing by setting `$routingEnabledSetting=true`.""",
            new String[] {"dbName", "db", "routingEnabledSetting"},
            Condition.CONNECTION_EXCEPTION,
            "unable to route to database"),
    STATUS_08N03(
            new GqlStatus("08N03"),
            """
            Failed to write to `$graph`. Check the defined access mode in both driver and database.""",
            new String[] {"graph"},
            Condition.CONNECTION_EXCEPTION,
            "failed to write to graph"),
    STATUS_08N04(
            new GqlStatus("08N04"),
            """
            Routing with `$useClause` is not supported in embedded sessions. Connect to the database directly or try running the query using a Neo4j driver or the HTTP API.""",
            new String[] {"useClause"},
            Condition.CONNECTION_EXCEPTION,
            "unable to route use clause"),
    STATUS_08N05(
            new GqlStatus("08N05"),
            """
            Routing administration commands is not supported in embedded sessions. Connect to the system database directly or try running the query using a Neo4j driver or the HTTP API.""",
            new String[] {},
            Condition.CONNECTION_EXCEPTION,
            "unable to route administration command"),
    STATUS_08N06(
            new GqlStatus("08N06"),
            """
            General network protocol error.""",
            new String[] {},
            Condition.CONNECTION_EXCEPTION,
            "protocol error"),
    STATUS_08N07(
            new GqlStatus("08N07"),
            """
            This member is not the leader.""",
            new String[] {},
            Condition.CONNECTION_EXCEPTION,
            "not the leader"),
    STATUS_08N08(
            new GqlStatus("08N08"),
            """
            This database is read only on this server.""",
            new String[] {},
            Condition.CONNECTION_EXCEPTION,
            "database is read only"),
    STATUS_08N09(
            new GqlStatus("08N09"),
            """
            The database `$db` is currently unavailable. Check the database status. Retry your request at a later time.""",
            new String[] {"db"},
            Condition.CONNECTION_EXCEPTION,
            "database unavailable"),
    STATUS_08N10(
            new GqlStatus("08N10"),
            """
            The server is not in a state that can process message of type `$messageType`.""",
            new String[] {"messageType"},
            Condition.CONNECTION_EXCEPTION,
            "Invalid server state"),
    STATUS_08N11(
            new GqlStatus("08N11"),
            """
            The request is invalid.""",
            new String[] {},
            Condition.CONNECTION_EXCEPTION,
            "request error"),
    STATUS_08N12(
            new GqlStatus("08N12"),
            """
            Failed to parse the supplied bookmark. Verify it is correct or check the debug log for more information.""",
            new String[] {},
            Condition.CONNECTION_EXCEPTION,
            "failed to parse bookmark"),
    STATUS_08N13(
            new GqlStatus("08N13"),
            """
            `$dbName` is not up to the requested bookmark `$oldestAcceptableTxId`. The latest transaction ID is `$latestTransactionId`.""",
            new String[] {"dbName", "oldestAcceptableTxId", "latestTransactionId"},
            Condition.CONNECTION_EXCEPTION,
            "database not up to requested bookmark"),
    STATUS_08N14(
            new GqlStatus("08N14"),
            """
            Unable to provide a routing table for the database '$aliasName' because the request comes from another alias '$sourceAliasString' and alias chains are not permitted.""",
            new String[] {"aliasName", "sourceAliasString"},
            Condition.CONNECTION_EXCEPTION,
            "alias chains are not permitted"),
    STATUS_22000(new GqlStatus("22000"), """
            """, new String[] {}, Condition.DATA_EXCEPTION, ""),
    STATUS_22001(
            new GqlStatus("22001"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "string data, right truncation"),
    STATUS_22003(
            new GqlStatus("22003"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "numeric value out of range"),
    STATUS_22004(
            new GqlStatus("22004"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "null value not allowed"),
    STATUS_22007(
            new GqlStatus("22007"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime format"),
    STATUS_22008(
            new GqlStatus("22008"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "datetime field overflow"),
    STATUS_22011(
            new GqlStatus("22011"), """
            """, new String[] {}, Condition.DATA_EXCEPTION, "substring error"),
    STATUS_22012(
            new GqlStatus("22012"), """
            """, new String[] {}, Condition.DATA_EXCEPTION, "division by zero"),
    STATUS_22015(
            new GqlStatus("22015"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "interval field overflow"),
    STATUS_22018(
            new GqlStatus("22018"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid character value for cast"),
    STATUS_2201E(
            new GqlStatus("2201E"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid argument for natural logarithm"),
    STATUS_2201F(
            new GqlStatus("2201F"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid argument for power function"),
    STATUS_22027(new GqlStatus("22027"), """
            """, new String[] {}, Condition.DATA_EXCEPTION, "trim error"),
    STATUS_2202F(
            new GqlStatus("2202F"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "array data, right truncation"),
    STATUS_22G02(
            new GqlStatus("22G02"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "negative limit value"),
    STATUS_22G03(
            new GqlStatus("22G03"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid value type"),
    STATUS_22G04(
            new GqlStatus("22G04"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "values not comparable"),
    STATUS_22G05(
            new GqlStatus("22G05"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime function field name"),
    STATUS_22G06(
            new GqlStatus("22G06"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid datetime function value"),
    STATUS_22G07(
            new GqlStatus("22G07"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid duration function field name"),
    STATUS_22G0B(
            new GqlStatus("22G0B"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "list data, right truncation"),
    STATUS_22G0C(
            new GqlStatus("22G0C"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "list element error"),
    STATUS_22G0F(
            new GqlStatus("22G0F"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid number of paths or groups"),
    STATUS_22G0H(
            new GqlStatus("22G0H"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid duration format"),
    STATUS_22G0M(
            new GqlStatus("22G0M"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "multiple assignments to a graph element property"),
    STATUS_22G0N(
            new GqlStatus("22G0N"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "number of node labels below supported minimum"),
    STATUS_22G0P(
            new GqlStatus("22G0P"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "number of node labels exceeds supported maximum"),
    STATUS_22G0Q(
            new GqlStatus("22G0Q"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "number of edge labels below supported minimum"),
    STATUS_22G0R(
            new GqlStatus("22G0R"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "number of edge labels exceeds supported maximum"),
    STATUS_22G0S(
            new GqlStatus("22G0S"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "number of node properties exceeds supported maximum"),
    STATUS_22G0T(
            new GqlStatus("22G0T"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "number of edge properties exceeds supported maximum"),
    STATUS_22G0U(
            new GqlStatus("22G0U"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "record fields do not match"),
    STATUS_22G0V(
            new GqlStatus("22G0V"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "reference value, invalid base type"),
    STATUS_22G0W(
            new GqlStatus("22G0W"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "reference value, invalid constrained type"),
    STATUS_22G0X(
            new GqlStatus("22G0X"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "record data, field unassignable"),
    STATUS_22G0Y(
            new GqlStatus("22G0Y"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "record data, field missing"),
    STATUS_22G0Z(
            new GqlStatus("22G0Z"), """
            """, new String[] {}, Condition.DATA_EXCEPTION, "malformed path"),
    STATUS_22G10(
            new GqlStatus("22G10"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "path data, right truncation"),
    STATUS_22G11(
            new GqlStatus("22G11"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "reference value, referent deleted"),
    STATUS_22G12(
            new GqlStatus("22G12"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid value type"),
    STATUS_22G13(
            new GqlStatus("22G13"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid group variable value"),
    STATUS_22G14(
            new GqlStatus("22G14"),
            """
            """,
            new String[] {},
            Condition.DATA_EXCEPTION,
            "incompatible temporal instant unit groups"),
    STATUS_22N00(
            new GqlStatus("22N00"),
            """
            The provided value is unsupported and cannot be processed.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "unsupported value"),
    STATUS_22N01(
            new GqlStatus("22N01"),
            """
            Expected `$value` to be of `$type1` (or of `$type2` ...), but was of type `$valueType`.""",
            new String[] {"value", "type1", "type2", "valueType"},
            Condition.DATA_EXCEPTION,
            "invalid type"),
    STATUS_22N02(
            new GqlStatus("22N02"),
            """
            Expected `$name` to be a positive number but found '$value' instead.""",
            new String[] {"name", "value"},
            Condition.DATA_EXCEPTION,
            "negative number"),
    STATUS_22N03(
            new GqlStatus("22N03"),
            """
            Expected `$component` to be `$numberType` in the range `$lower` to `$upper` but found '$value'.""",
            new String[] {"component", "numberType", "lower", "upper", "value"},
            Condition.DATA_EXCEPTION,
            "number out of range"),
    STATUS_22N04(
            new GqlStatus("22N04"),
            """
            Invalid input `$input` for `$context`. Expected one of $validValues.""",
            new String[] {"input", "context", "validValues"},
            Condition.DATA_EXCEPTION,
            "invalid input value"),
    STATUS_22N05(
            new GqlStatus("22N05"),
            """
            Invalid input `$input` for `$context`. Expected $explainValidValues.

            eg: Invalid input for password. Expected at least N characters.""",
            new String[] {"input", "context", "explainValidValues"},
            Condition.DATA_EXCEPTION,
            "input failed validation"),
    STATUS_22N06(
            new GqlStatus("22N06"),
            """
            Invalid input. $entity is not allowed to be an empty string.""",
            new String[] {"entity"},
            Condition.DATA_EXCEPTION,
            "empty string not allowed"),
    STATUS_22N07(
            new GqlStatus("22N07"),
            """
            Invalid pre-parser option: `$keys`.""",
            new String[] {"keys"},
            Condition.DATA_EXCEPTION,
            "invalid pre-parser option key"),
    STATUS_22N08(
            new GqlStatus("22N08"),
            """
            Invalid pre-parser option, cannot combine `$option1` with `$option2`.""",
            new String[] {"option1", "option2"},
            Condition.DATA_EXCEPTION,
            "invalid pre-parser combination"),
    STATUS_22N09(
            new GqlStatus("22N09"),
            """
            Invalid pre-parser option, cannot specify multiple conflicting values for `$name`.""",
            new String[] {"name"},
            Condition.DATA_EXCEPTION,
            "conflicting pre-parser combination"),
    STATUS_22N10(
            new GqlStatus("22N10"),
            """
            Invalid pre-parser option, `$input` is not a valid option for `$name`. Valid options are: `$validOptions`.""",
            new String[] {"input", "name", "validOptions"},
            Condition.DATA_EXCEPTION,
            "invalid pre-parser option value"),
    STATUS_22N11(
            new GqlStatus("22N11"),
            """
            Invalid argument: cannot process `$providedArgument`.""",
            new String[] {"providedArgument"},
            Condition.DATA_EXCEPTION,
            "invalid argument"),
    STATUS_22N12(
            new GqlStatus("22N12"),
            """
            Invalid argument: cannot process `$providedArgument`.""",
            new String[] {"providedArgument"},
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime format"),
    STATUS_22N13(
            new GqlStatus("22N13"),
            """
            Specified time zones must include a date component.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid time zone"),
    STATUS_22N14(
            new GqlStatus("22N14"),
            """
            Cannot select both `$field` and `$component`.""",
            new String[] {"field", "component"},
            Condition.DATA_EXCEPTION,
            "invalid temporal value combination"),
    STATUS_22N15(
            new GqlStatus("22N15"),
            """
            Cannot read the specified `$component` component from `$temporal`.""",
            new String[] {"component", "temporal"},
            Condition.DATA_EXCEPTION,
            "invalid temporal component"),
    STATUS_22N16(
            new GqlStatus("22N16"),
            """
            Importing entity values to a graph with a `USE` clause is not supported. Attempted to import `$expr` to `$graph`.""",
            new String[] {"expr", "graph"},
            Condition.DATA_EXCEPTION,
            "invalid import value"),
    STATUS_22N17(
            new GqlStatus("22N17"),
            """
            Cannot read the specified `$component` component from `$temporal`.""",
            new String[] {"component", "temporal"},
            Condition.DATA_EXCEPTION,
            "invalid date, time, or datetime function field name"),
    STATUS_22N18(
            new GqlStatus("22N18"),
            """
            A `$crs` `POINT` must contain `$crs.mandatoryKeys`.""",
            new String[] {"crs", "crs"},
            Condition.DATA_EXCEPTION,
            "incomplete spatial value"),
    STATUS_22N19(
            new GqlStatus("22N19"),
            """
            A `POINT` must contain either 'x' and 'y', or 'latitude' and 'longitude'.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid spatial value"),
    STATUS_22N20(
            new GqlStatus("22N20"),
            """
            Cannot create `POINT` with `$crsDimensions` coordinate reference system (CRS) and `$crsLength` coordinates. Use the equivalent `$crsLength` coordinate reference system instead.""",
            new String[] {"crsDimensions", "crsLength", "crsLength"},
            Condition.DATA_EXCEPTION,
            "invalid spatial value dimensions"),
    STATUS_22N21(
            new GqlStatus("22N21"),
            """
            Geographic `POINT` values do not support the coordinate reference system (CRS): `$crs`.""",
            new String[] {"crs"},
            Condition.DATA_EXCEPTION,
            "unsupported coordinate reference system"),
    STATUS_22N22(
            new GqlStatus("22N22"),
            """
            Cannot specify both coordinate reference system (CRS) and spatial reference identifier (SRID).""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid spatial value combination"),
    STATUS_22N23(
            new GqlStatus("22N23"),
            """
            Cannot create WGS84 `POINT` with invalid coordinate: `$coordinate`. The valid range for the latitude coordinate is [-90, 90].""",
            new String[] {"coordinate"},
            Condition.DATA_EXCEPTION,
            "invalid latitude value"),
    STATUS_22N24(
            new GqlStatus("22N24"),
            """
            Cannot construct a `$valueType` from: `$coordinates`.""",
            new String[] {"valueType", "coordinates"},
            Condition.DATA_EXCEPTION,
            "invalid coordinate arguments"),
    STATUS_22N25(
            new GqlStatus("22N25"),
            """
            Cannot construct `$valueType` from: `$temporal`.""",
            new String[] {"valueType", "temporal"},
            Condition.DATA_EXCEPTION,
            "invalid temporal arguments"),
    STATUS_22N26(
            new GqlStatus("22N26"),
            """
            Unknown rounding mode. Valid values are: CEILING, FLOOR, UP, DOWN, HALF_EVEN, HALF_UP, HALF_DOWN, UNNECESSARY.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "unknown rounding mode"),
    STATUS_22N27(
            new GqlStatus("22N27"),
            """
            Expected `$name` to be `$type` (or `$type2`, ...), but found `$value` of type `$valueType`.""",
            new String[] {"name", "type", "type2", "value", "valueType"},
            Condition.DATA_EXCEPTION,
            "invalid entity type"),
    STATUS_22N28(
            new GqlStatus("22N28"),
            """
            The result of the operation `$operation` has caused an overflow.""",
            new String[] {"operation"},
            Condition.DATA_EXCEPTION,
            "overflow error"),
    STATUS_22N29(
            new GqlStatus("22N29"),
            """
            Unknown coordinate reference system (CRS).""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "unknown coordinate reference system"),
    STATUS_22N30(
            new GqlStatus("22N30"),
            """
            At least one temporal unit must be specified.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "missing temporal unit"),
    STATUS_22N31(
            new GqlStatus("22N31"),
            """
            `MERGE` cannot be used for `NODE` and `RELATIONSHIP` property values that are `null` or NaN.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid properties in merge pattern"),
    STATUS_22N32(
            new GqlStatus("22N32"),
            """
            `ORDER BY` expressions must be deterministic.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "non-deterministic sort expression"),
    STATUS_22N33(
            new GqlStatus("22N33"),
            """
            Shortest path expressions must contain start and end nodes. Cannot find: `$node`.""",
            new String[] {"node"},
            Condition.DATA_EXCEPTION,
            "invalid shortest path expression"),
    STATUS_22N34(
            new GqlStatus("22N34"),
            """
            Cannot use the `$function()` function inside an aggregate function.""",
            new String[] {"function"},
            Condition.DATA_EXCEPTION,
            "invalid use of aggregate function"),
    STATUS_22N35(
            new GqlStatus("22N35"),
            """
            Cannot parse `$text` to a `DATE`. Calendar dates are formatted as `YYYY-MM`, while ordinal dates are formatted `YYYY-DDD`.""",
            new String[] {"text"},
            Condition.DATA_EXCEPTION,
            "invalid date format"),
    STATUS_22N36(
            new GqlStatus("22N36"),
            """
            Cannot parse `$text` to `@typeName`.""",
            new String[] {"text"},
            Condition.DATA_EXCEPTION,
            "invalid temporal format"),
    STATUS_22N37(
            new GqlStatus("22N37"),
            """
            Cannot coerce `$value` to `$type`.""",
            new String[] {"value", "type"},
            Condition.DATA_EXCEPTION,
            "invalid coercion"),
    STATUS_22N38(
            new GqlStatus("22N38"),
            """
            Expected input to the `$function()` function to be `$type1` (or `$type2`, ...), but found `$value` of type `$valueType`.""",
            new String[] {"function", "type1", "type2", "value", "valueType"},
            Condition.DATA_EXCEPTION,
            "invalid function input type"),
    STATUS_22N39(
            new GqlStatus("22N39"),
            """
            Value `$value` cannot be stored in properties.""",
            new String[] {"value"},
            Condition.DATA_EXCEPTION,
            "unsupported property value type"),
    STATUS_22N40(
            new GqlStatus("22N40"),
            """
            Cannot assign `$component` to `$expectedTemporalType`.""",
            new String[] {"component", "expectedTemporalType"},
            Condition.DATA_EXCEPTION,
            "non-assignable temporal component"),
    STATUS_22N41(
            new GqlStatus("22N41"),
            """
            The `MERGE` clause did not find a matching node `$node` and cannot create a new node due to conflicts with existing uniqueness constraints.""",
            new String[] {"node"},
            Condition.DATA_EXCEPTION,
            "merge node uniqueness constraint violation"),
    STATUS_22N42(
            new GqlStatus("22N42"),
            """
            The `MERGE` clause did not find a matching relationship `$relationship` and cannot create a new relationship due to conflicts with existing uniqueness constraints.""",
            new String[] {"relationship"},
            Condition.DATA_EXCEPTION,
            "merge relationship uniqueness constraint violation"),
    STATUS_22N43(
            new GqlStatus("22N43"),
            """
            Could not load external resource from `$url`.""",
            new String[] {"url"},
            Condition.DATA_EXCEPTION,
            "unable to load external resource"),
    STATUS_22N44(
            new GqlStatus("22N44"),
            """
            Parallel runtime has been disabled, enable it or upgrade to a bigger Aura instance.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "parallel runtime disabled"),
    STATUS_22N45(
            new GqlStatus("22N45"),
            """
            The `$feature` feature is not supported by Neo4j Community Edition.""",
            new String[] {"feature"},
            Condition.DATA_EXCEPTION,
            "feature not supported on Neo4j Community Edition"),
    STATUS_22N46(
            new GqlStatus("22N46"),
            """
            Parallel runtime does not support updating queries or a change in the state of transactions. Use another runtime.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "unsupported use of parallel runtime"),
    STATUS_22N47(
            new GqlStatus("22N47"),
            """
            No workers are configured for the parallel runtime. Set 'server.cypher.parallel.worker_limit' to a larger value.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid parallel runtime configuration"),
    STATUS_22N48(
            new GqlStatus("22N48"),
            """
            Cannot use the specified runtime  `$runtime` due to  `$unsupportedParts`. Use another runtime.""",
            new String[] {"runtime", "unsupportedParts"},
            Condition.DATA_EXCEPTION,
            "unable to use specified runtime"),
    STATUS_22N49(
            new GqlStatus("22N49"),
            """
            Cannot read a CSV field larger than the set buffer size. Ensure the field does not have an unterminated quote, or increase the buffer size via `dbms.import.csv.buffer_size`.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "CSV buffer size overflow"),
    STATUS_22N50(
            new GqlStatus("22N50"),
            """
            Internal error. Refer to the debug log if available.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "unknown internal error"),
    STATUS_22N51(
            new GqlStatus("22N51"),
            """
            A [composite] database or alias with the name `$name` does not exist. Verify that the spelling is correct.""",
            new String[] {"name"},
            Condition.DATA_EXCEPTION,
            "database does not exist"),
    STATUS_22N52(
            new GqlStatus("22N52"),
            """
            `PROFILE` and `EXPLAIN` cannot be combined.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid combination of `PROFILE` and `EXPLAIN`"),
    STATUS_22N53(
            new GqlStatus("22N53"),
            """
            Cannot `PROFILE` query before results are materialized.""",
            new String[] {},
            Condition.DATA_EXCEPTION,
            "invalid use of `PROFILE`"),
    STATUS_22N54(
            new GqlStatus("22N54"),
            """
            Duplicate key specified for `$key`""",
            new String[] {"key"},
            Condition.DATA_EXCEPTION,
            "invalid map"),
    STATUS_22N55(
            new GqlStatus("22N55"),
            """
            Map requires key `$key`but was missing from field `$fieldName`""",
            new String[] {"key", "fieldName"},
            Condition.DATA_EXCEPTION,
            "required key missing from map"),
    STATUS_22N56(
            new GqlStatus("22N56"),
            """
            Protocol message length limit exceeded, limit: `$limit`.""",
            new String[] {"limit"},
            Condition.DATA_EXCEPTION,
            "protocol message length limit overflow"),
    STATUS_22N57(
            new GqlStatus("22N57"),
            """
            Protocol type is invalid, invalid number of components `$actualComponentCount` expected `$expectedComponentCount`""",
            new String[] {"actualComponentCount", "expectedComponentCount"},
            Condition.DATA_EXCEPTION,
            "invalid protocol type"),
    STATUS_22N58(
            new GqlStatus("22N58"),
            """
            Cannot read the specified `$component` component from `$spatial`.""",
            new String[] {"component", "spatial"},
            Condition.DATA_EXCEPTION,
            "invalid spatial component"),
    STATUS_25000(new GqlStatus("25000"), """
            """, new String[] {}, Condition.INVALID_TRANSACTION_STATE, ""),
    STATUS_25G01(
            new GqlStatus("25G01"),
            """
            """,
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "active GQL-transaction"),
    STATUS_25G02(
            new GqlStatus("25G02"),
            """
            """,
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "catalog and data statement mixing not supported"),
    STATUS_25G03(
            new GqlStatus("25G03"),
            """
            """,
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "read-only GQL-transaction"),
    STATUS_25G04(
            new GqlStatus("25G04"),
            """
            """,
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "accessing multiple graphs not supported"),
    STATUS_25N01(
            new GqlStatus("25N01"),
            """
            $query1' cannot be executed after '$query2'. It must be executed in a different transaction.""",
            new String[] {"query1", "query2"},
            Condition.INVALID_TRANSACTION_STATE,
            "invalid mix of queries"),
    STATUS_25N02(
            new GqlStatus("25N02"),
            """
            Unable to complete transaction. See debug log for details.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "unable to complete transaction"),
    STATUS_25N03(
            new GqlStatus("25N03"),
            """
            Transaction is being used concurrently by another request.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "concurrent access violation"),
    STATUS_25N04(
            new GqlStatus("25N04"),
            """
            Transaction `$transactionId` does not exist""",
            new String[] {"transactionId"},
            Condition.INVALID_TRANSACTION_STATE,
            "no such transcation"),
    STATUS_25N05(
            new GqlStatus("25N05"),
            """
            Transaction has been terminated or closed.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "transaction terminated or closed"),
    STATUS_25N06(
            new GqlStatus("25N06"),
            """
            Failed to start transaction. See debug log for details.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "transaction start failed"),
    STATUS_25N07(
            new GqlStatus("25N07"),
            """
            Failed to start constituent transaction. See debug log for details.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "constituent transaction start failed"),
    STATUS_25N08(
            new GqlStatus("25N08"),
            """
            The lease for the transaction is no longer valid.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "lease invalid"),
    STATUS_25N09(
            new GqlStatus("25N09"),
            """
            An unknown error occurred.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_STATE,
            "unexpected transaction failure"),
    STATUS_2D000(
            new GqlStatus("2D000"),
            """
            """,
            new String[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            ""),
    STATUS_2DN01(
            new GqlStatus("2DN01"),
            """
            Failed to commit transaction. See debug log for details.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "commit failed"),
    STATUS_2DN02(
            new GqlStatus("2DN02"),
            """
            Failed to commit constituent transaction. See debug log for details.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "constituent commit failed"),
    STATUS_2DN03(
            new GqlStatus("2DN03"),
            """
            Failed to terminate transaction. See debug log for details.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "termination failed"),
    STATUS_2DN04(
            new GqlStatus("2DN04"),
            """
            Failed to terminate constituent transaction. See debug log for details.""",
            new String[] {},
            Condition.INVALID_TRANSACTION_TERMINATION,
            "constituent termination failed"),
    STATUS_40000(new GqlStatus("40000"), """
            """, new String[] {}, Condition.TRANSACTION_ROLLBACK, ""),
    STATUS_40003(
            new GqlStatus("40003"),
            """
            """,
            new String[] {},
            Condition.TRANSACTION_ROLLBACK,
            "statement completion unknown"),
    STATUS_40N01(
            new GqlStatus("40N01"),
            """
            Failed to rollback transaction. See debug log for details.""",
            new String[] {},
            Condition.TRANSACTION_ROLLBACK,
            "rollback failed"),
    STATUS_40N02(
            new GqlStatus("40N02"),
            """
            Failed to rollback constituent transaction. See debug log for details.""",
            new String[] {},
            Condition.TRANSACTION_ROLLBACK,
            "constituent rollback failed"),
    STATUS_42000(
            new GqlStatus("42000"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            ""),
    STATUS_42001(
            new GqlStatus("42001"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid syntax"),
    STATUS_42002(
            new GqlStatus("42002"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid reference"),
    STATUS_42004(
            new GqlStatus("42004"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "use of visually confusable identifiers"),
    STATUS_42006(
            new GqlStatus("42006"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge labels below supported minimum"),
    STATUS_42007(
            new GqlStatus("42007"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge labels exceeds supported maximum"),
    STATUS_42008(
            new GqlStatus("42008"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge properties exceeds supported maximum"),
    STATUS_42009(
            new GqlStatus("42009"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node labels below supported minimum"),
    STATUS_42010(
            new GqlStatus("42010"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node labels exceeds supported maximum"),
    STATUS_42011(
            new GqlStatus("42011"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node properties exceeds supported maximum"),
    STATUS_42012(
            new GqlStatus("42012"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node type key labels below supported minimum"),
    STATUS_42013(
            new GqlStatus("42013"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of node type key labels exceeds supported maximum"),
    STATUS_42014(
            new GqlStatus("42014"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge type key labels below supported minimum"),
    STATUS_42015(
            new GqlStatus("42015"),
            """
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "number of edge type key labels exceeds supported maximum"),
    STATUS_42I00(
            new GqlStatus("42I00"),
            """
            `CASE` expressions must have the same number of `WHEN` and `THEN` operands.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid case expression"),
    STATUS_42I01(
            new GqlStatus("42I01"),
            """
            Invalid use of `$clause` inside `FOREACH`.""",
            new String[] {"clause"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid FOREACH"),
    STATUS_42I02(
            new GqlStatus("42I02"),
            """
            Failed to parse comment. A comment starting with `/*` must also have a closing `*/`.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid comment"),
    STATUS_42I03(
            new GqlStatus("42I03"),
            """
            A Cypher query has to contain at least one clause.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "empty request"),
    STATUS_42I04(
            new GqlStatus("42I04"),
            """
            `$expr ` cannot be used in a `$clause` clause.""",
            new String[] {"expr", "clause"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid expression"),
    STATUS_42I05(
            new GqlStatus("42I05"),
            """
            The `LOAD CSV` `FIELDTERMINATOR` can only be one character wide.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid FIELDTERMINATOR"),
    STATUS_42I06(
            new GqlStatus("42I06"),
            """
            Invalid input `$input`, expected: `$expectedInput`.""",
            new String[] {"input", "expectedInput"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid input"),
    STATUS_42I07(
            new GqlStatus("42I07"),
            """
            The given $integerType integer literal `$input` is invalid.""",
            new String[] {"integerType", "input"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid integer literal"),
    STATUS_42I08(
            new GqlStatus("42I08"),
            """
            The lower bound of the variable length relationship used in the `$shortestPathFunc()` function must be 0 or 1.""",
            new String[] {"shortestPathFunc"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid lower bound"),
    STATUS_42I09(
            new GqlStatus("42I09"),
            """
            Expected `MAP` to contain the same number of keys and values, but got keys `$keys` and values `$values`.""",
            new String[] {"keys", "values"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid map"),
    STATUS_42I10(
            new GqlStatus("42I10"),
            """
            Mixing label expression symbols (`|`, `&`, `!`, and `%`) with colon (`:`) between labels is not allowed. This expression could be expressed as $syntax.""",
            new String[] {"syntax"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid label expression"),
    STATUS_42I11(
            new GqlStatus("42I11"),
            """
            A `$typeOrLabelOrProp` name cannot be empty or contain any null-bytes: `$input`.""",
            new String[] {"typeOrLabelOrProp", "input"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid name"),
    STATUS_42I12(
            new GqlStatus("42I12"),
            """
            Quantified path patterns cannot be nested.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid nesting of quantified path patterns"),
    STATUS_42I13(
            new GqlStatus("42I13"),
            """
            The $procOrFun call does not provide the required number of arguments: expected `$expectedCount` got `$actualCount`.

            The $procOrFun `$procFun` has the signature: `$sig`.""",
            new String[] {"procOrFun", "expectedCount", "actualCount", "procOrFun", "procFun", "sig"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid number of procedure or function arguments"),
    STATUS_42I14(
            new GqlStatus("42I14"),
            """
            Exactly one relationship type must be specified for `$var`.""",
            new String[] {"var"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid number of relationship types"),
    STATUS_42I15(
            new GqlStatus("42I15"),
            """
            Expected exactly one statement per query but got: $actualCount.""",
            new String[] {"actualCount"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid number of statements"),
    STATUS_42I16(
            new GqlStatus("42I16"),
            """
            Map with keys `$keys` is not a valid `POINT`. Use either Cartesian or geographic coordinates.""",
            new String[] {"keys"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid point"),
    STATUS_42I17(
            new GqlStatus("42I17"),
            """
            A quantifier must not have a lower bound greater than the upper bound.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid quantifier"),
    STATUS_42I18(
            new GqlStatus("42I18"),
            """
            The aggregation column contains implicit grouping expressions referenced by the variables `$vars`. Implicit grouping expressions are variables not explicitly declared as grouping keys.""",
            new String[] {"vars"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid reference to implicitly grouped expressions"),
    STATUS_42I19(
            new GqlStatus("42I19"),
            """
            Failed to parse string literal. The query must contain an even number of non-escaped quotes.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid string literal"),
    STATUS_42I20(
            new GqlStatus("42I20"),
            """
            $labelOrRelType expressions cannot contain `$input`. To express a label disjunction use `$isOrColon$sanitizedLabelExpression` instead.""",
            new String[] {"labelOrRelType", "input", "isOrColon", "sanitizedLabelExpression"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid symbol in expression"),
    STATUS_42I22(
            new GqlStatus("42I22"),
            """
            The right hand side of a `UNION` clause must be a single query.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of UNION"),
    STATUS_42I23(
            new GqlStatus("42I23"),
            """
            The `$fun()` function cannot contain a quantified path pattern.""",
            new String[] {"fun"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid quantified path pattern in shortest path"),
    STATUS_42I24(
            new GqlStatus("42I24"),
            """
            Aggregate expressions are not allowed inside of $expr.
            """,
            new String[] {"expr"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of aggregate function"),
    STATUS_42I25(
            new GqlStatus("42I25"),
            """
            `CALL { ... } IN TRANSACTIONS` after a write clause is not supported.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of CALL IN TRANSACTIONS"),
    STATUS_42I26(
            new GqlStatus("42I26"),
            """
            The `DELETE` clause does not support removing labels from a node. Use `REMOVE`.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid DELETE"),
    STATUS_42I27(
            new GqlStatus("42I27"),
            """
            `DISTINCT` cannot be used with the '$fun()' function.""",
            new String[] {"fun"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of DISTINCT with non-aggregate function"),
    STATUS_42I28(
            new GqlStatus("42I28"),
            """
            Importing `WITH` can consist only of direct references to outside variables. `$input` is not allowed.""",
            new String[] {"input"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of importing WITH"),
    STATUS_42I29(
            new GqlStatus("42I29"),
            """
            The `IS` keyword cannot be used together with multiple labels in `$input`. Rewrite the expression as `$replacement`.""",
            new String[] {"input", "replacement"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of IS"),
    STATUS_42I30(
            new GqlStatus("42I30"),
            """
            Label expressions cannot be used in a $expr.""",
            new String[] {"expr"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of label expressions"),
    STATUS_42I31(
            new GqlStatus("42I31"),
            """
            A `MATCH` clause cannot directly follow an `OPTIONAL MATCH` clause. Use a `WITH` clause between them.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of MATCH"),
    STATUS_42I32(
            new GqlStatus("42I32"),
            """
            Node and relationship pattern predicates cannot be used in a `$clause` clause. They can only be used in a `MATCH` clause or inside a pattern comprehension.""",
            new String[] {"clause"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of node and relationship pattern predicate"),
    STATUS_42I33(
            new GqlStatus("42I33"),
            """
            Closed Dynamic Union types cannot be appended with `NOT NULL`, specify `NOT NULL` on inner types instead.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of NOT NULL"),
    STATUS_42I34(
            new GqlStatus("42I34"),
            """
            A pattern expression can only be used to test the existence of a pattern. Use a pattern comprehension instead.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of pattern expression"),
    STATUS_42I35(
            new GqlStatus("42I35"),
            """
            Relationship type expressions can only be used in a `MATCH` clause.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of relationship type expression"),
    STATUS_42I36(
            new GqlStatus("42I36"),
            """
            `REPORT STATUS` can only be used when specifying `ON ERROR CONTINUE` or `ON ERROR BREAK`.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of REPORT STATUS"),
    STATUS_42I37(
            new GqlStatus("42I37"),
            """
            `RETURN` * is not allowed when there are no variables in scope.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of RETURN *"),
    STATUS_42I38(
            new GqlStatus("42I38"),
            """
            A `RETURN` clause can only be used at the end of a query or subquery.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of RETURN"),
    STATUS_42I39(
            new GqlStatus("42I39"),
            """
            Mixing the `$fun` function with path selectors or explicit match modes is not allowed.""",
            new String[] {"fun"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of shortest path function"),
    STATUS_42I40(
            new GqlStatus("42I40"),
            """
            `UNION` and `UNION ALL` cannot be combined.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of UNION and UNION ALL"),
    STATUS_42I41(
            new GqlStatus("42I41"),
            """
            Variable length relationships cannot be used $expression.""",
            new String[] {"expression"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of variable length relationship"),
    STATUS_42I42(
            new GqlStatus("42I42"),
            """
            Cannot use `YIELD` on a void procedure.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of YIELD"),
    STATUS_42I43(
            new GqlStatus("42I43"),
            """
            `YIELD *` can only be used with a standalone procedure `CALL`.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of YIELD *"),
    STATUS_42I44(
            new GqlStatus("42I44"),
            """
            Cannot use a join hint for a single node pattern.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid joint hint"),
    STATUS_42I45(
            new GqlStatus("42I45"),
            """
            Multiple path patterns cannot be used in the same clause in combination with a selective path selector. $action""",
            new String[] {"action"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of multiple path patterns"),
    STATUS_42I46(
            new GqlStatus("42I46"),
            """
            Node pattern pairs are only supported for quantified path patterns.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of a node pattern pair"),
    STATUS_42I47(
            new GqlStatus("42I47"),
            """
            Parser Error, `$wrapped`.""",
            new String[] {"wrapped"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "parser error"),
    STATUS_42I48(
            new GqlStatus("42I48"),
            """
            Subqueries are not allowed in a `MERGE` clause.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of a subquery in MERGE"),
    STATUS_42I49(
            new GqlStatus("42I49"),
            """
            Unknown inequality operator '!='. The operator for inequality in Cypher is '<>'.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid inequality operator"),
    STATUS_42N00(
            new GqlStatus("42N00"),
            """
            No such database `$param1`""",
            new String[] {"param1"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such database"),
    STATUS_42N01(
            new GqlStatus("42N01"),
            """
            No such constituent graph `$param1` in composite database `$param2`""",
            new String[] {"param1", "param2"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such constituent graph in composite database"),
    STATUS_42N02(
            new GqlStatus("42N02"),
            """
            Writing in read access mode not allowed.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "writing in read access mode not allowed"),
    STATUS_42N03(
            new GqlStatus("42N03"),
            """
            Writing to multiple graphs in the same transaction is not allowed. Use CALL IN TRANSACTION or create separate transactions in your application.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "writing to multiple graphs in the same transaction is not allowed "),
    STATUS_42N04(
            new GqlStatus("42N04"),
            """
            Failed to access `$constituentOrComposite` while connected to `$sessionDb`. Connect to `$compositeDb` directly.""",
            new String[] {"constituentOrComposite", "sessionDb", "compositeDb"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported access of composite database"),
    STATUS_42N05(
            new GqlStatus("42N05"),
            """
            Failed to access `$db` while connected to `$compositeSessionDb`. Connect to `$db` directly or create an alias in the composite database.""",
            new String[] {"db", "compositeSessionDb", "db"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported access of standard database"),
    STATUS_42N06(
            new GqlStatus("42N06"),
            """
            $action is not supported on composite databases. $workaround""",
            new String[] {"action", "workaround"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported action on composite database"),
    STATUS_42N07(
            new GqlStatus("42N07"),
            """
            The variable `$varName` is shadowing a variable with the same name from the outer scope and needs to be renamed.""",
            new String[] {"varName"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable already defined"),
    STATUS_42N08(
            new GqlStatus("42N08"),
            """
            There is no `$procOrFunc` with the `$procOrFuncNameOrId` registered for this database instance.""",
            new String[] {"procOrFunc", "procOrFuncNameOrId"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such procedure or function"),
    STATUS_42N09(
            new GqlStatus("42N09"),
            """
            A user with the name `$name` does not exist.""",
            new String[] {"name"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such user"),
    STATUS_42N10(
            new GqlStatus("42N10"),
            """
            A role with the name `$name` does not exist.""",
            new String[] {"name"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "no such role"),
    STATUS_42N11(
            new GqlStatus("42N11"),
            """
            A [composite] database or alias with the name `$name` already exists.""",
            new String[] {"name"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "database already exists"),
    STATUS_42N12(
            new GqlStatus("42N12"),
            """
            A user with the name `$name` already exists.""",
            new String[] {"name"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "user already exists"),
    STATUS_42N13(
            new GqlStatus("42N13"),
            """
            A role with the name `$name` already exists.""",
            new String[] {"name"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "role already exists"),
    STATUS_42N14(
            new GqlStatus("42N14"),
            """
            `$clause` cannot be used together with `$command`.""",
            new String[] {"clause", "command"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of command"),
    STATUS_42N15(
            new GqlStatus("42N15"),
            """
            `$keyword` is a reserved keyword and cannot be used in this place.""",
            new String[] {"keyword"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of reserved keyword"),
    STATUS_42N16(
            new GqlStatus("42N16"),
            """
            Only single property $type are supported.""",
            new String[] {"type"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported index or constraint "),
    STATUS_42N17(
            new GqlStatus("42N17"),
            """
            This Cypher command cannot be executed against the database `$name`.""",
            new String[] {"name"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported request"),
    STATUS_42N18(
            new GqlStatus("42N18"),
            """
            The database is in read-only mode.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "read-only database"),
    STATUS_42N20(
            new GqlStatus("42N20"),
            """
            The list range operator `[ ]` cannot be empty.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "empty list range operator"),
    STATUS_42N21(
            new GqlStatus("42N21"),
            """
            Expression in `$clause` must be aliased (use `AS`).""",
            new String[] {"clause"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unaliased return item"),
    STATUS_42N22(
            new GqlStatus("42N22"),
            """
            A `COLLECT` subquery must end with a single return column.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "single return column required"),
    STATUS_42N23(
            new GqlStatus("42N23"),
            """
            The aggregating function must be included in the `$clause` clause to be used in the `ORDER BY`.""",
            new String[] {"clause"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing reference to aggregation function"),
    STATUS_42N24(
            new GqlStatus("42N24"),
            """
            A `WITH` clause is required between `$input1` and `$input2`.""",
            new String[] {"input1", "input2"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing WITH"),
    STATUS_42N25(
            new GqlStatus("42N25"),
            """
            Procedure call inside a query does not support naming results implicitly. Use `YIELD` instead.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing YIELD"),
    STATUS_42N26(
            new GqlStatus("42N26"),
            """
            Multiple join hints for the same variable `$var` are not supported.""",
            new String[] {"var"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "multiple join hints on same variable"),
    STATUS_42N28(
            new GqlStatus("42N28"),
            """
            Only statically inferrable patterns and variables are allowed in `$name`.""",
            new String[] {"name"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "patterns or variables not statically inferrable"),
    STATUS_42N29(
            new GqlStatus("42N29"),
            """
            Pattern expressions are not allowed to introduce new variables: '$var'.""",
            new String[] {"var"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unbound variables in pattern expression"),
    STATUS_42N31(
            new GqlStatus("42N31"),
            """
            Expected `$component` to be `$numberType` in the range `$lower` to `$upper` but found '$value'.
            """,
            new String[] {"component", "numberType", "lower", "upper", "value"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "out of range"),
    STATUS_42N32(
            new GqlStatus("42N32"),
            """
            Parameter maps cannot be used in `$keyword` patterns. Use a literal map instead.""",
            new String[] {"keyword"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of parameter map"),
    STATUS_42N34(
            new GqlStatus("42N34"),
            """
            Path cannot be bound in a quantified path pattern.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "path bound in quantified path pattern"),
    STATUS_42N35(
            new GqlStatus("42N35"),
            """
            The path selector `$selector` is not supported within $quantifiedOrParenthesized path patterns.""",
            new String[] {"selector", "quantifiedOrParenthesized"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported path selector in path pattern"),
    STATUS_42N36(
            new GqlStatus("42N36"),
            """
            Procedure call is missing parentheses.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "procedure call without parentheses"),
    STATUS_42N37(
            new GqlStatus("42N37"),
            """
            Relationship pattern predicates cannot be use in variable length relationships.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of relationship pattern predicates in variable length relationships"),
    STATUS_42N38(
            new GqlStatus("42N38"),
            """
            Return items must have unique names.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "duplicate return item name"),
    STATUS_42N39(
            new GqlStatus("42N39"),
            """
            All subqueries in a `UNION` clause must have the same return column names.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "incompatible return columns"),
    STATUS_42N40(
            new GqlStatus("42N40"),
            """
            The `$fun()` function must contain one relationship pattern.""",
            new String[] {"fun"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "single relationship pattern required"),
    STATUS_42N41(
            new GqlStatus("42N41"),
            """
            The `reduce()` function requires a `| expression` after the accumulator.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing |-expression"),
    STATUS_42N42(
            new GqlStatus("42N42"),
            """
            Sub-path assignment is not supported.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported sub-path binding"),
    STATUS_42N44(
            new GqlStatus("42N44"),
            """
            It is not possible to access the variable `$var` declared before the `$clause` clause when using `DISTINCT` or an aggregation.""",
            new String[] {"var", "clause"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "inaccessible variable"),
    STATUS_42N45(
            new GqlStatus("42N45"),
            """
            Unexpected end of input, expected `CYPHER`, `EXPLAIN`, `PROFILE` or a query.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unexpected end of input"),
    STATUS_42N46(
            new GqlStatus("42N46"),
            """
            `$input` is not a recognized Cypher type.
            """,
            new String[] {"input"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unexpected type"),
    STATUS_42N47(
            new GqlStatus("42N47"),
            """
            `CALL { ... } IN TRANSACTIONS` in a `UNION` is not supported.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of UNION"),
    STATUS_42N48(
            new GqlStatus("42N48"),
            """
            Unknown function `$fun`. Verify that the spelling is correct.""",
            new String[] {"fun"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unknown function"),
    STATUS_42N49(
            new GqlStatus("42N49"),
            """
            Unknown Normal Form: `$input`.""",
            new String[] {"input"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unknown normal form"),
    STATUS_42N50(
            new GqlStatus("42N50"),
            """
            Unknown procedure output: `$outputName`.""",
            new String[] {"outputName"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unknown procedure"),
    STATUS_42N52(
            new GqlStatus("42N52"),
            """
            `$type` is not a recognized Cypher type.""",
            new String[] {"type"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported type"),
    STATUS_42N53(
            new GqlStatus("42N53"),
            """
            The quantified path pattern may yield an infinite number of rows under match mode `REPEATABLE ELEMENTS`. Use a path selector or add an upper bound to the quantified path pattern.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsafe usage of repeatable elements"),
    STATUS_42N54(
            new GqlStatus("42N54"),
            """
            The match mode `$match_mode ` is not supported.""",
            new String[] {"match_mode"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported match mode"),
    STATUS_42N55(
            new GqlStatus("42N55"),
            """
            The path selector `$selector` is not supported.""",
            new String[] {"selector"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported path selector"),
    STATUS_42N56(
            new GqlStatus("42N56"),
            """
            Properties are not supported in the `$fun()` function.""",
            new String[] {"fun"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported use of properties"),
    STATUS_42N57(
            new GqlStatus("42N57"),
            """
            $expr cannot contain any updating clauses.""",
            new String[] {"expr"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of data-modifications in expressions"),
    STATUS_42N58(
            new GqlStatus("42N58"),
            """
            Nested `CALL { ... } IN TRANSACTIONS` is not supported.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unsupported use of nesting"),
    STATUS_42N59(
            new GqlStatus("42N59"),
            """
            Variable `$var` already declared.""",
            new String[] {"var"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable already defined"),
    STATUS_42N60(
            new GqlStatus("42N60"),
            """
            The variable `$var` within the quantified path pattern is in conflict with another bound variable.""",
            new String[] {"var"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable already bound"),
    STATUS_42N61(
            new GqlStatus("42N61"),
            """
            The $quantifiedOrParenthesized path pattern can only reference variables that are bound in a previous `MATCH` clause.
            """,
            new String[] {"quantifiedOrParenthesized"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable not bound"),
    STATUS_42N62(
            new GqlStatus("42N62"),
            """
            Variable `$var` not defined.""",
            new String[] {"var"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable not defined"),
    STATUS_42N63(
            new GqlStatus("42N63"),
            """
            All inner types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "inner type with different nullability"),
    STATUS_42N64(
            new GqlStatus("42N64"),
            """
            A $quantifiedOrParenthesized path pattern must have at least one $nodeOrRelationship pattern.""",
            new String[] {"quantifiedOrParenthesized", "nodeOrRelationship"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "at least one node or relationship required"),
    STATUS_42N65(
            new GqlStatus("42N65"),
            """
            The `$fun()` function requires bound node variables when it is not part of a `MATCH` clause.""",
            new String[] {"fun"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "node variable not bound"),
    STATUS_42N66(
            new GqlStatus("42N66"),
            """
            Bound relationships are not allowed in the `$fun()` function.""",
            new String[] {"fun"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "relationship variable already bound"),
    STATUS_42N67(
            new GqlStatus("42N67"),
            """
            Duplicated `$param` parameter.""",
            new String[] {"param"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "duplicate parameter"),
    STATUS_42N68(
            new GqlStatus("42N68"),
            """
            Variables cannot be defined more than once in a `$clause` clause.""",
            new String[] {"clause"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "duplicate variable definition"),
    STATUS_42N69(
            new GqlStatus("42N69"),
            """
            The `$fun()` function is only allowed as a top-level element and not inside a `$expr`.""",
            new String[] {"fun", "expr"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "function not allowed inside expression"),
    STATUS_42N70(
            new GqlStatus("42N70"),
            """
            The function `$fun()` requires a `WHERE` clause.""",
            new String[] {"fun"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "function without required WHERE clause"),
    STATUS_42N71(
            new GqlStatus("42N71"),
            """
            A query must conclude with a `RETURN` clause, a `FINISH` clause, an update clause, a unit subquery call, or a procedure call with no `YIELD`.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "incomplete query"),
    STATUS_42N72(
            new GqlStatus("42N72"),
            """
            $action is only supported on composite databases. $workaround""",
            new String[] {"action", "workaround"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "action only supported on composite databases"),
    STATUS_42N73(
            new GqlStatus("42N73"),
            """
            `USE` clause must be the first clause of a query or `UNION` part. In a `CALL` sub-query, it can also be the second clause if the first clause is an importing `WITH`.
            """,
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid placement of USE clause"),
    STATUS_42N74(
            new GqlStatus("42N74"),
            """
            Failed to access `$db1` and `db2`. Child `USE` clauses must target the same graph as their parent query. Run in separate (sub)queries instead.""",
            new String[] {"db1"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid nested USE clause"),
    STATUS_42N75(
            new GqlStatus("42N75"),
            """
            `$graphFunction` is only allowed at the first position of a `USE` clause.""",
            new String[] {"graphFunction"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "invalid use of graph function"),
    STATUS_42N76(
            new GqlStatus("42N76"),
            """
            The hint `$prettifiedHint` cannot be fulfilled.""",
            new String[] {"prettifiedHint"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unfullfillable hint"),
    STATUS_42N77(
            new GqlStatus("42N77"),
            """
            The hint `$prettifiedHint` cannot be fulfilled. The query does not contain a compatible predicate for `$entity` on `$variable`.""",
            new String[] {"prettifiedHint", "entity", "variable"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing hint predicate"),
    STATUS_42N78(
            new GqlStatus("42N78"),
            """
            "Node `$node` has already been bound and cannot be modified by the `$clause` clause.""",
            new String[] {"node", "clause"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "variable already bound"),
    STATUS_42N80(
            new GqlStatus("42N80"),
            """
            Policy definition for '$routing_policy' could not be found.""",
            new String[] {"routing_policy"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "unknown routing policy"),
    STATUS_42N81(
            new GqlStatus("42N81"),
            """
            Expected `$expectedParameter`, but found `$actualParameters`.""",
            new String[] {"expectedParameter", "actualParameters"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing parameter "),
    STATUS_42N82(
            new GqlStatus("42N82"),
            """
            The database `$param1` has one or more aliases. Drop the aliases `$param2` before dropping the database.""",
            new String[] {"param1", "param2"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "database alias without target not allowed"),
    STATUS_42N83(
            new GqlStatus("42N83"),
            """
            Cannot impersonate a user while password change required.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "impersonation disallowed while password change required"),
    STATUS_42N84(
            new GqlStatus("42N84"),
            """
            `WHERE` is not allowed by itself. Use `TERMINATE TRANSACTION ... YIELD ... WHERE ...` instead.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "missing YIELD clause"),
    STATUS_42N85(
            new GqlStatus("42N85"),
            """
            Allowed and denied database options are mutually exclusive.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "cannot specify both allowed and denied databases"),
    STATUS_42N86(
            new GqlStatus("42N86"),
            """
            `$failingQueryPart` failed. Parameterized database and graph names do not support wildcards.""",
            new String[] {"failingQueryPart"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "wildcard in parameter"),
    STATUS_42N87(
            new GqlStatus("42N87"),
            """
            The name `$name` conflicts with the name `$otherName` of an existing database or alias.""",
            new String[] {"name", "otherName"},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "name conflict"),
    STATUS_42NFC(
            new GqlStatus("42NFC"),
            """
            Authentication and/or authorization could not be validated. See security logs for details.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "auth info validation error"),
    STATUS_42NFD(
            new GqlStatus("42NFD"),
            """
            Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "credentials expired"),
    STATUS_42NFE(
            new GqlStatus("42NFE"),
            """
            Authorization info expired.
            Authentication info expired.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "auth info expired"),
    STATUS_42NFF(
            new GqlStatus("42NFF"),
            """
            Access denied, see the security logs for details.""",
            new String[] {},
            Condition.SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION,
            "permission/access denied"),
    STATUS_50N00(
            new GqlStatus("50N00"),
            """
            Internal exception raised `$param1`: $param2""",
            new String[] {"param1", "param2"},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "internal error"),
    STATUS_50N01(
            new GqlStatus("50N01"),
            """
            Remote execution by `$param1` raised `$param2`: $param3""",
            new String[] {"param1", "param2", "param3"},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "remote execution error"),
    STATUS_50N05(
            new GqlStatus("50N05"),
            """
            Deadlock detected while trying to acquire locks. See log for more details.""",
            new String[] {},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "deadlock detected"),
    STATUS_50N06(
            new GqlStatus("50N06"),
            """
            Unexpected error has occured during the query execution. See debug log for details.""",
            new String[] {},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "unexpected execution error"),
    STATUS_50N07(
            new GqlStatus("50N07"),
            """
            Execution failed. See cause and debug log for details.""",
            new String[] {},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "transaction terminated or closed"),
    STATUS_50N09(
            new GqlStatus("50N09"),
            """
            No such state exists.""",
            new String[] {},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "unknown server state"),
    STATUS_50N42(
            new GqlStatus("50N42"),
            """
            Unknown error""",
            new String[] {},
            Condition.GENERAL_PROCESSING_EXCEPTION,
            "unknown error"),
    STATUS_51N00(
            new GqlStatus("51N00"),
            """
            Failed to register procedure""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "procedure registration error"),
    STATUS_51N01(
            new GqlStatus("51N01"),
            """
            The field `$fieldName` in the class `$className` is annotated as a `@Context` field, but it is declared as static. `@Context` fields must be public, non-final and non-static.""",
            new String[] {"fieldName", "className"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class field annotation should be public, non-final, and non-static"),
    STATUS_51N02(
            new GqlStatus("51N02"),
            """
            Unable to set up injection for procedure `$java_class`. The field `$java_field` has type `$java_type` which is not a supported injectable component.""",
            new String[] {"java_class", "java_field", "java_type"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unsupported injectable component type"),
    STATUS_51N03(
            new GqlStatus("51N03"),
            """
            Unable to set up injection for `$java_class `, failed to access field `$java_field`.""",
            new String[] {"java_class", "java_field"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unable to access field"),
    STATUS_51N04(
            new GqlStatus("51N04"),
            """
            The field `$java_field` on `$java_class ` must be annotated as a `@Context` field in order to store its state.""",
            new String[] {"java_field", "java_class"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "missing class field annotation"),
    STATUS_51N05(
            new GqlStatus("51N05"),
            """
            The field `$java_field` on `$java_class ` must be declared non-final and public.""",
            new String[] {"java_field", "java_class"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class field should be public and non-final"),
    STATUS_51N06(
            new GqlStatus("51N06"),
            """
            The argument at position `$positionNum` in `$java_method` requires a `@Name` annotation and a non-empty name.""",
            new String[] {"positionNum", "java_method"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "missing argument name"),
    STATUS_51N07(
            new GqlStatus("51N07"),
            """
            The `$procFun` contains a non-default argument before a default argument. Non-default arguments are not allowed to be positioned after default arguments.""",
            new String[] {"procFun"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid ordering of default arguments"),
    STATUS_51N08(
            new GqlStatus("51N08"),
            """
            The class `$java_class` must contain exactly one '@UserAggregationResult' method and exactly one '@UserAggregationUpdate' method.""",
            new String[] {"java_class"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "exactly one @UserAggregationResult method and one @UserAggregationUpdate method required"),
    STATUS_51N09(
            new GqlStatus("51N09"),
            """
            The '@UserAggregationUpdate' method `$java_method` in $java_class must be public and have the return type 'void'.""",
            new String[] {"java_method", "java_class"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "@UserAggregationUpdate method must be public and void"),
    STATUS_51N10(
            new GqlStatus("51N10"),
            """
            The '$java_method' method `$methodName` in $java_class must be public.""",
            new String[] {"java_method", "methodName", "java_class"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "aggregation method not public"),
    STATUS_51N11(
            new GqlStatus("51N11"),
            """
            The class `$java_class` must be public.""",
            new String[] {"java_class"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class not public"),
    STATUS_51N12(
            new GqlStatus("51N12"),
            """
            The procedure `$proc` has zero output fields and must be defined as void.""",
            new String[] {"proc"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "class not void"),
    STATUS_51N13(
            new GqlStatus("51N13"),
            """
            Unable to register the $procOrFun `$procFun` because the name is already in use.""",
            new String[] {"procOrFun", "procFun"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "procedure or function name already in use"),
    STATUS_51N14(
            new GqlStatus("51N14"),
            """
            The procedure `$proc` has a duplicate $java_type field, `$java_field`.""",
            new String[] {"proc", "java_type", "java_field"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "duplicate field name"),
    STATUS_51N15(
            new GqlStatus("51N15"),
            """
            Type mismatch for map key. Required `STRING`, but found $type.""",
            new String[] {"type"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid map key type"),
    STATUS_51N16(
            new GqlStatus("51N16"),
            """
            Type mismatch for the default value. Required $type, but found $input.""",
            new String[] {"type", "input"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid default value type"),
    STATUS_51N17(
            new GqlStatus("51N17"),
            """
            Procedures and functions cannot be defined in the root namespace, or use a reserved namespace. Use the package name instead e.g. `org.example.com.$proc`.""",
            new String[] {"proc"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid procedure or function name"),
    STATUS_51N18(
            new GqlStatus("51N18"),
            """
            The method `$java_method` has an invalid return type. Procedures must return a stream of records, where each record is of a defined concrete class.""",
            new String[] {"java_method"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "invalid method return type"),
    STATUS_51N19(
            new GqlStatus("51N19"),
            """
            The field `$java_field` in the class `$java_class` cannot be accessed. Ensure the field is marked as public.""",
            new String[] {"java_field", "java_class"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "inaccessible field"),
    STATUS_51N20(
            new GqlStatus("51N20"),
            """
            The field `$java_field` is not injectable.Ensure the field is marked as public and non-final.""",
            new String[] {"java_field"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot inject field "),
    STATUS_51N21(
            new GqlStatus("51N21"),
            """
            The procedure registration failed because the procedure registry was busy. Try again.""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "procedure registry is busy"),
    STATUS_51N22(
            new GqlStatus("51N22"),
            """
            Finding the shortest path for the given pattern requires an exhaustive search. To enable exhaustive searches, set `cypher.forbid_exhaustive_shortestpath` to `false`.""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "exhaustive shortest path search disabled"),
    STATUS_51N23(
            new GqlStatus("51N23"),
            """
            Cannot find the shortest path when the start and end nodes are the same. To enable this behavior, set `dbms.cypher.forbid_shortestpath_common_nodes` to `false`.""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cyclic shortest path search disabled"),
    STATUS_51N24(
            new GqlStatus("51N24"),
            """
            Could not find a query plan within given time and space limits.""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "insufficient resources for plan search"),
    STATUS_51N25(
            new GqlStatus("51N25"),
            """
            Cannot compile query due to excessive updates to indexes and constraints.""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "database is busy"),
    STATUS_51N26(
            new GqlStatus("51N26"),
            """
            $thing is not supported in $context.

            $thing is not avaliable. This implementation of Cypher does not support $featureDescr.""",
            new String[] {"thing", "context", "thing", "featureDescr"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported in this version"),
    STATUS_51N27(
            new GqlStatus("51N27"),
            """
            $thing is not supported in $context.
            The administration command `$param` is not supported in Community Edition and Aura.""",
            new String[] {"thing", "context", "param"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported in this edition"),
    STATUS_51N28(
            new GqlStatus("51N28"),
            """
            This Cypher command must be executed against the database `system`.

            `$action` is not allowed on the system database.""",
            new String[] {"action"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported by this database"),
    STATUS_51N29(
            new GqlStatus("51N29"),
            """
            `$commandName` must be executed on the `LEADER` server.""",
            new String[] {"commandName"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported by this server"),
    STATUS_51N30(
            new GqlStatus("51N30"),
            """
            $thing is not supported in $context.
            Impersonation is not supported in a database with native auth disabled.""",
            new String[] {"thing", "context"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported with this configuration"),
    STATUS_51N31(
            new GqlStatus("51N31"),
            """
            $thing is not supported in $context.
            URL pattern is not supported in LOAD privileges.""",
            new String[] {"thing", "context"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "not supported"),
    STATUS_51N32(
            new GqlStatus("51N32"),
            """
            Server is in panic""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "server panic"),
    STATUS_51N33(
            new GqlStatus("51N33"),
            """
            This member failed to replicate transaction, try again.""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "replication error"),
    STATUS_51N34(
            new GqlStatus("51N34"),
            """
            Failed to write to the database due to a cluster leader change. Retrying your request at a later time may succeed.""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "write transaction failed due to leader change"),
    STATUS_51N35(
            new GqlStatus("51N35"),
            """
            The location of `$name` has changed while the transaction was running.""",
            new String[] {"name"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "database location changed"),
    STATUS_51N36(
            new GqlStatus("51N36"),
            """
            There is not enough memory to perform the current task""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "out of memory"),
    STATUS_51N37(
            new GqlStatus("51N37"),
            """
            There is not enough stack size to perform the current task.""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "stack overflow"),
    STATUS_51N38(
            new GqlStatus("51N38"),
            """
            """,
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "no threads available"),
    STATUS_51N39(
            new GqlStatus("51N39"),
            """
            Expected set of files not found on disk. Please restore from backup.""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "raft log corrupted"),
    STATUS_51N40(
            new GqlStatus("51N40"),
            """
            Database `$namedDatabaseId` failed to start. Try restarting it.""",
            new String[] {"namedDatabaseId"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unable to start database"),
    STATUS_51N41(
            new GqlStatus("51N41"),
            """
            Server or database admin operation not possible.""",
            new String[] {},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "admin operation failed "),
    STATUS_51N42(
            new GqlStatus("51N42"),
            """
            Unable to check if allocator '$allocator' is available.""",
            new String[] {"allocator"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "unknown allocator"),
    STATUS_51N43(
            new GqlStatus("51N43"),
            """
            Cannot deallocate server(s) $servers.""",
            new String[] {"servers"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot deallocate servers"),
    STATUS_51N44(
            new GqlStatus("51N44"),
            """
            Cannot drop server '$server'.""",
            new String[] {"server"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot drop server"),
    STATUS_51N45(
            new GqlStatus("51N45"),
            """
            Cannot cordon server '$server'.""",
            new String[] {"server"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot cordon server"),
    STATUS_51N46(
            new GqlStatus("51N46"),
            """
            Cannot alter server '$server'.""",
            new String[] {"server"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot alter server"),
    STATUS_51N47(
            new GqlStatus("51N47"),
            """
            Cannot rename server '$server'.""",
            new String[] {"server"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot rename server"),
    STATUS_51N48(
            new GqlStatus("51N48"),
            """
            Cannot enable server '$server'.""",
            new String[] {"server"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot enable server"),
    STATUS_51N49(
            new GqlStatus("51N49"),
            """
            Cannot alter database '$databaseName'.""",
            new String[] {"databaseName"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot alter database"),
    STATUS_51N50(
            new GqlStatus("51N50"),
            """
            Cannot recreate database '$databaseName'.""",
            new String[] {"databaseName"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot recreate database"),
    STATUS_51N51(
            new GqlStatus("51N51"),
            """
            Cannot create database '$databaseName'.""",
            new String[] {"databaseName"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot create database"),
    STATUS_51N52(
            new GqlStatus("51N52"),
            """
            Number of primaries '$number' may not exceed $MAX_PRIMARIES and needs to be at least 1""",
            new String[] {"number", "MAX_PRIMARIES"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot alter database topology"),
    STATUS_51N53(
            new GqlStatus("51N53"),
            """
            Number of secondaries '$number' may not exceed $MAX_SECONDARIES and needs to be at least 0""",
            new String[] {"number", "MAX_SECONDARIES"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot alter database topology"),
    STATUS_51N54(
            new GqlStatus("51N54"),
            """
            Failed to calculate reallocation for databases. $wrappedError""",
            new String[] {"wrappedError"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "cannot reallocate"),
    STATUS_51N55(
            new GqlStatus("51N55"),
            """
            Failed to create the database `$param1`. The limit of databases is reached. Either increase the limit using the config setting `$param2` or drop a database.""",
            new String[] {"param1", "param2"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "database limit reached"),
    STATUS_51N56(
            new GqlStatus("51N56"),
            """
            The number of primary constrained seeding servers '$primaryConstrainedServers', is larger than the desired number of primary allocations '$desiredPrimaries'.""",
            new String[] {"primaryConstrainedServers", "desiredPrimaries"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "too many primary seeds"),
    STATUS_51N57(
            new GqlStatus("51N57"),
            """
            The number of secondary constrained seeding servers '$secondaryConstrainedServers', is larger than the desired number of secondary allocations '$desiredSecondaries'.""",
            new String[] {"secondaryConstrainedServers", "desiredSecondaries"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "too many secondary seeds"),
    STATUS_51N58(
            new GqlStatus("51N58"),
            """
            The number of seeding servers '$nbrSeedingServers', is larger than the desired number of allocations '$desiredAllocations'.""",
            new String[] {"nbrSeedingServers", "desiredAllocations"},
            Condition.SYSTEM_CONFIGURATION_OR_OPERATION_EXCEPTION,
            "too many seeds"),
    STATUS_52N01(
            new GqlStatus("52N01"),
            """
            Execution of the procedure `$proc` timed out after $time $unit.""",
            new String[] {"proc", "time", "unit"},
            Condition.PROCEDURE_EXCEPTION,
            "procedure execution timeout"),
    STATUS_52N02(
            new GqlStatus("52N02"),
            """
            Execution of the procedure `$proc` failed.""",
            new String[] {"proc"},
            Condition.PROCEDURE_EXCEPTION,
            "procedure execution error"),
    STATUS_52N03(
            new GqlStatus("52N03"),
            """
            Execution of the procedure `$proc` failed due to an invalid specified execution mode `$xmode`.""",
            new String[] {"proc", "xmode"},
            Condition.PROCEDURE_EXCEPTION,
            "invalid procedure execution mode"),
    STATUS_52N04(
            new GqlStatus("52N04"),
            """
            Temporarily could not execute the procedure `$proc`. Please retry.""",
            new String[] {"proc"},
            Condition.PROCEDURE_EXCEPTION,
            "transient procedure execution error"),
    STATUS_52N05(
            new GqlStatus("52N05"),
            """
            Can't invoke procedure on this member because it is not a secondary for database '$namedDatabaseId'.""",
            new String[] {"namedDatabaseId"},
            Condition.PROCEDURE_EXCEPTION,
            "cannot invoke procedure on a primary"),
    STATUS_52N06(
            new GqlStatus("52N06"),
            """
            Unexpected number of parameters: should have 0-2 parameters, but was $nbr""",
            new String[] {"nbr"},
            Condition.PROCEDURE_EXCEPTION,
            "unexpected parameters to checkConnectivity"),
    STATUS_52N07(
            new GqlStatus("52N07"),
            """
            Unrecognised port name '$port'. Valid values are: $values""",
            new String[] {"port", "values"},
            Condition.PROCEDURE_EXCEPTION,
            "invalid parameter to checkConnectivity"),
    STATUS_52N08(
            new GqlStatus("52N08"),
            """
            Unable to parse server id '$serverIdString'""",
            new String[] {"serverIdString"},
            Condition.PROCEDURE_EXCEPTION,
            "invalid parameter to checkConnectivity"),
    STATUS_52N09(
            new GqlStatus("52N09"),
            """
            Cannot get routing table for $databaseAlias because Bolt is not enabled. Please update your configuration for 'server.bolt.enabled'""",
            new String[] {"databaseAlias"},
            Condition.PROCEDURE_EXCEPTION,
            "bolt is not enabled"),
    STATUS_52N10(
            new GqlStatus("52N10"),
            """
            An address key is included in the query string provided to the GetRoutingTableProcedure, but its value could not be parsed.""",
            new String[] {},
            Condition.PROCEDURE_EXCEPTION,
            "unknown address key"),
    STATUS_52N11(
            new GqlStatus("52N11"),
            """
            An unexpected error has occurred. Please refer to the server's debug log for more information.""",
            new String[] {},
            Condition.PROCEDURE_EXCEPTION,
            "generic topology procedure error"),
    STATUS_52N12(
            new GqlStatus("52N12"),
            """
            The previous default database $oldDatabaseName is still running.""",
            new String[] {"oldDatabaseName"},
            Condition.PROCEDURE_EXCEPTION,
            "cannot change default database"),
    STATUS_52N13(
            new GqlStatus("52N13"),
            """
            New default database $databaseName does not exist.""",
            new String[] {"databaseName"},
            Condition.PROCEDURE_EXCEPTION,
            "unknown default database"),
    STATUS_52N14(
            new GqlStatus("52N14"),
            """
            System database cannot be set as default""",
            new String[] {},
            Condition.PROCEDURE_EXCEPTION,
            "system cannot be default database"),
    STATUS_52N15(
            new GqlStatus("52N15"),
            """
            Provided allocator '$allocator' is not available or was not initialized!""",
            new String[] {"allocator"},
            Condition.PROCEDURE_EXCEPTION,
            "unknown allocator"),
    STATUS_52N16(
            new GqlStatus("52N16"),
            """
            Invalid arguments to procedure""",
            new String[] {},
            Condition.PROCEDURE_EXCEPTION,
            "invalid procedure arguments"),
    STATUS_52N17(
            new GqlStatus("52N17"),
            """
            Setting/removing the quarantine marker failed""",
            new String[] {},
            Condition.PROCEDURE_EXCEPTION,
            "quarantine change failed"),
    STATUS_52N18(
            new GqlStatus("52N18"),
            """
            The number of seeding servers '$nbrSeedingServers' is larger than the defined number of allocations '$nbrAllocations'.""",
            new String[] {"nbrSeedingServers", "nbrAllocations"},
            Condition.PROCEDURE_EXCEPTION,
            "too many seeders"),
    STATUS_52N19(
            new GqlStatus("52N19"),
            """
            The specified seeding server with id '$serverId' could not be found.""",
            new String[] {"serverId"},
            Condition.PROCEDURE_EXCEPTION,
            "unknown seeder"),
    STATUS_52N20(
            new GqlStatus("52N20"),
            """
            The recreation of a database is not supported when seed updating is not enabled.""",
            new String[] {},
            Condition.PROCEDURE_EXCEPTION,
            "seed updating not enabled"),
    STATUS_52U00(
            new GqlStatus("52U00"),
            """
            Execution of the procedure `$proc` failed due to `$class`: `$msg`""",
            new String[] {"proc", "class", "msg"},
            Condition.PROCEDURE_EXCEPTION,
            "custom procedure execution error cause"),
    STATUS_G1000(new GqlStatus("G1000"), """
            """, new String[] {}, Condition.DEPENDENT_OBJECT_ERROR, ""),
    STATUS_G1001(
            new GqlStatus("G1001"),
            """
            """,
            new String[] {},
            Condition.DEPENDENT_OBJECT_ERROR,
            "edges still exist"),
    STATUS_G1002(
            new GqlStatus("G1002"),
            """
            """,
            new String[] {},
            Condition.DEPENDENT_OBJECT_ERROR,
            "endpoint node is deleted"),
    STATUS_G1003(
            new GqlStatus("G1003"),
            """
            """,
            new String[] {},
            Condition.DEPENDENT_OBJECT_ERROR,
            "endpoint node not in current working graph"),
    STATUS_G2000(new GqlStatus("G2000"), """
            """, new String[] {}, Condition.GRAPH_TYPE_VIOLATION, "");

    private final GqlStatus gqlStatus;
    private final String message;
    private final String[] statusParameterKeys;
    private final String subCondition;
    private final Condition condition;

    GqlStatusInfoCodes(
            GqlStatus gqlStatus,
            String message,
            String[] statusParameterKeys,
            Condition condition,
            String subCondition) {
        this.gqlStatus = gqlStatus;
        this.message = message;
        this.statusParameterKeys = statusParameterKeys;
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
        String formattableMessage = toJavaFormattable(message);
        return String.format(formattableMessage, params.toArray());
    }

    @Override
    public String getSubCondition() {
        return subCondition;
    }

    public Condition getCondition() {
        return condition;
    }

    public String[] getStatusParameterKeys() {
        return statusParameterKeys;
    }
}
