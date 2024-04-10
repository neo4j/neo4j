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

public enum GqlStatusInfoNotifications implements GqlStatusInfo {
    STATUS_00N50(
            new GqlStatus("00N50"),
            "The database `%s` does not exist. Verify that the spelling is correct or create the database for the command to take effect."),
    STATUS_00N70(new GqlStatus("00N70"), "`%s` has no effect. The role already has the privilege."),
    STATUS_00N71(new GqlStatus("00N71"), "`%s` has no effect. The role does not have the privilege."),
    STATUS_00N72(new GqlStatus("00N72"), "`%s` has no effect. The user already has the role."),
    STATUS_00N73(new GqlStatus("00N73"), "`%s` has no effect. The user does not have the role."),
    STATUS_00N80(
            new GqlStatus("00N80"),
            "`ENABLE SERVER` has no effect. Server `%s` is already enabled. Verify that this is the intended server."),
    STATUS_00N81(
            new GqlStatus("00N81"),
            "`CORDON SERVER` has no effect. Server `%s` is already cordoned. Verify that this is the intended server."),
    STATUS_00N82(
            new GqlStatus("00N82"),
            "`REALLOCATE DATABASES` has no effect. No databases were reallocated. No better allocation is currently possible."),
    STATUS_00N83(
            new GqlStatus("00N83"),
            "Cordoned servers existed when making an allocation decision. Server(s) `%s` are cordoned. This can impact allocation decisions."),
    STATUS_00N84(
            new GqlStatus("00N84"),
            "`ALTER DATABASE` has no effect. The requested topology matched the current topology. No allocations were changed."),
    STATUS_00NA0(new GqlStatus("00NA0"), "`%s` has no effect. `%s` already exists."),
    STATUS_00NA1(new GqlStatus("00NA1"), "`%s` has no effect. `%s` does not exist."),
    STATUS_01N00(new GqlStatus("01N00"), "%s"),
    STATUS_01N01(new GqlStatus("01N01"), "`%s` is deprecated. It is replaced by `%s`."),
    STATUS_01N02(new GqlStatus("01N02"), "`%s` is deprecated and will be removed without a replacement."),
    STATUS_01N03(new GqlStatus("01N03"), "`%s` returned by procedure `%s` is deprecated."),
    STATUS_01N30(
            new GqlStatus("01N30"),
            "Unable to create a plan with `JOIN ON %s`. Try to change the join key(s) or restructure your query."),
    STATUS_01N31(new GqlStatus("01N31"), "Unable to create a plan with `%s` because the index does not exist."),
    STATUS_01N40(new GqlStatus("01N40"), "The query cannot be executed with `%s`, `%s` is used. Cause: `%s`."),
    STATUS_01N50(new GqlStatus("01N50"), "The label `%s` does not exist. Verify that the spelling is correct."),
    STATUS_01N51(
            new GqlStatus("01N51"), "The relationship type `%s` does not exist. Verify that the spelling is correct."),
    STATUS_01N52(new GqlStatus("01N52"), "The property `%s` does not exist. Verify that the spelling is correct."),
    STATUS_01N60(
            new GqlStatus("01N60"),
            "The query plan cannot be cached and is not executable without `EXPLAIN` due to the undefined parameter(s) `%s`. Provide the parameter(s)."),
    STATUS_01N61(
            new GqlStatus("01N61"),
            "The expression `%s` cannot be satisfied because relationships must have exactly one type."),
    STATUS_01N62(new GqlStatus("01N62"), "The procedure `%s` generates the warning `%s`."),
    STATUS_01N63(new GqlStatus("01N63"), "`%s` is repeated in `%s`, which leads to no results."),
    STATUS_01N70(
            new GqlStatus("01N70"),
            "`%s` has no effect. %s Make sure nothing is misspelled. This notification will become an error in a future major version."),
    STATUS_03N60(
            new GqlStatus("03N60"),
            "The variable `%s` in the subquery uses the same name as a variable from the outer query. Use `WITH %s` in the subquery to import the one from the outer scope unless you want it to be a new variable."),
    STATUS_03N90(
            new GqlStatus("03N90"),
            "The disconnected patterns `%s` build a cartesian product. A cartesian product may produce a large amount of data and slow down query processing."),
    STATUS_03N91(
            new GqlStatus("03N91"),
            "The provided pattern `%s` is unbounded. Shortest path with an unbounded pattern may result in long execution times. Use an upper limit (e.g. `[*..5]`) on the number of node hops in your pattern."),
    STATUS_03N92(
            new GqlStatus("03N92"),
            "The query runs with exhaustive shortest path due to the existential predicate(s) `%s`. It may be possible to use `WITH` to separate the `MATCH` from the existential predicate(s)."),
    STATUS_03N93(
            new GqlStatus("03N93"),
            "`LOAD CSV` in combination with `MATCH` or `MERGE` on a label that does not have an index may result in long execution times. Consider adding an index for label `%s`."),
    STATUS_03N94(
            new GqlStatus("03N94"),
            "The query execution plan contains the `Eager` operator. `LOAD CSV` in combination with `Eager` can consume a lot of memory."),
    STATUS_03N95(
            new GqlStatus("03N95"),
            "An index exists on label/type(s) `%s`. It is not possible to use indexes for dynamic properties. Consider using static properties.");

    private final GqlStatus gqlStatus;
    private final String message;

    GqlStatusInfoNotifications(GqlStatus gqlStatus, String message) {
        this.gqlStatus = gqlStatus;
        this.message = message;
    }

    public GqlStatus getGqlStatus() {
        return gqlStatus;
    }

    public String getMessage() {
        return message;
    }
}
