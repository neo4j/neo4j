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
            "The database `%s` does not exist. Verify that the spelling is correct or create the database for the command to take effect.",
            "home database not found",
            Condition.SUCCESSFUL_COMPLETION),
    STATUS_00N70(
            new GqlStatus("00N70"),
            "`%s` has no effect. The role or privilege is already assigned.",
            "role or privilege already assigned",
            Condition.SUCCESSFUL_COMPLETION),
    STATUS_00N71(
            new GqlStatus("00N71"),
            "`%s` has no effect. The role or privilege is not assigned.",
            "role or privilege not assigned",
            Condition.SUCCESSFUL_COMPLETION),
    STATUS_00N80(
            new GqlStatus("00N80"),
            "`ENABLE SERVER` has no effect. Server `%s` is already enabled. Verify that this is the intended server.",
            "server already enabled",
            Condition.SUCCESSFUL_COMPLETION),
    STATUS_00N81(
            new GqlStatus("00N81"),
            "`CORDON SERVER` has no effect. Server `%s` is already cordoned. Verify that this is the intended server.",
            "server already cordoned",
            Condition.SUCCESSFUL_COMPLETION),
    STATUS_00N82(
            new GqlStatus("00N82"),
            "`REALLOCATE DATABASES` has no effect. No databases were reallocated. No better allocation is currently possible.",
            "no databases reallocated",
            Condition.SUCCESSFUL_COMPLETION),
    STATUS_00N83(
            new GqlStatus("00N83"),
            "Cordoned servers existed when making an allocation decision. Server(s) `%s` are cordoned. This can impact allocation decisions.",
            "cordoned servers existed during allocation",
            Condition.SUCCESSFUL_COMPLETION),
    STATUS_00N84(
            new GqlStatus("00N84"),
            "`ALTER DATABASE` has no effect. The requested topology matched the current topology. No allocations were changed.",
            "requested topology matched current topology",
            Condition.SUCCESSFUL_COMPLETION),
    STATUS_00NA0(
            new GqlStatus("00NA0"),
            "`%s` has no effect. `%s` already exists.",
            "index or constraint already exists",
            Condition.SUCCESSFUL_COMPLETION),
    STATUS_00NA1(
            new GqlStatus("00NA1"),
            "`%s` has no effect. `%s` does not exist.",
            "index or constraint does not exist",
            Condition.SUCCESSFUL_COMPLETION),
    STATUS_01N00(new GqlStatus("01N00"), "%s", "feature deprecated", Condition.WARNING),
    STATUS_01N01(
            new GqlStatus("01N01"),
            "`%s` is deprecated. It is replaced by `%s`.",
            "feature deprecated with replacement",
            Condition.WARNING),
    STATUS_01N02(
            new GqlStatus("01N02"),
            "`%s` is deprecated and will be removed without a replacement.",
            "feature deprecated without replacement",
            Condition.WARNING),
    STATUS_01N03(
            new GqlStatus("01N03"),
            "`%s` returned by procedure `%s` is deprecated.",
            "procedure result column deprecated",
            Condition.WARNING),
    STATUS_01N30(
            new GqlStatus("01N30"),
            "Unable to create a plan with `JOIN ON %s`. Try to change the join key(s) or restructure your query.",
            "join hint unfulfillable",
            Condition.WARNING),
    STATUS_01N31(
            new GqlStatus("01N31"),
            "Unable to create a plan with `%s` because the index does not exist.",
            "hinted index not found",
            Condition.WARNING),
    STATUS_01N40(
            new GqlStatus("01N40"),
            "The query cannot be executed with `%s`, `%s` is used. Cause: `%s`.",
            "runtime unsupported",
            Condition.WARNING),
    STATUS_01N50(
            new GqlStatus("01N50"),
            "The label `%s` does not exist. Verify that the spelling is correct.",
            "unknown label",
            Condition.WARNING),
    STATUS_01N51(
            new GqlStatus("01N51"),
            "The relationship type `%s` does not exist. Verify that the spelling is correct.",
            "unknown relationship type",
            Condition.WARNING),
    STATUS_01N52(
            new GqlStatus("01N52"),
            "The property `%s` does not exist. Verify that the spelling is correct.",
            "unknown property key",
            Condition.WARNING),
    STATUS_01N60(
            new GqlStatus("01N60"),
            "The query plan cannot be cached and is not executable without `EXPLAIN` due to the undefined parameter(s) `%s`. Provide the parameter(s).",
            "parameter missing",
            Condition.WARNING),
    STATUS_01N61(
            new GqlStatus("01N61"),
            "The expression `%s` cannot be satisfied because relationships must have exactly one type.",
            "unsatisfiable relationship type expression",
            Condition.WARNING),
    STATUS_01N62(
            new GqlStatus("01N62"),
            "The procedure `%s` generates the warning `%s`.",
            "procedure execution warning",
            Condition.WARNING),
    STATUS_01N63(
            new GqlStatus("01N63"),
            "`%s` is repeated in `%s`, which leads to no results.",
            "repeated relationship reference",
            Condition.WARNING),
    STATUS_01N70(
            new GqlStatus("01N70"),
            "`%s` has no effect. %s Make sure nothing is misspelled. This notification will become an error in a future major version.",
            "impossible revoke command",
            Condition.WARNING),
    STATUS_03N60(
            new GqlStatus("03N60"),
            "The variable `%s` in the subquery uses the same name as a variable from the outer query. Use `WITH %s` in the subquery to import the one from the outer scope unless you want it to be a new variable.",
            "subquery variable shadowing",
            Condition.INFORMATION),
    STATUS_03N90(
            new GqlStatus("03N90"),
            "The disconnected patterns `%s` build a cartesian product. A cartesian product may produce a large amount of data and slow down query processing.",
            "cartesian product",
            Condition.INFORMATION),
    STATUS_03N91(
            new GqlStatus("03N91"),
            "The provided pattern `%s` is unbounded. Shortest path with an unbounded pattern may result in long execution times. Use an upper limit (e.g. `[*..5]`) on the number of node hops in your pattern.",
            "unbounded variable length pattern",
            Condition.INFORMATION),
    STATUS_03N92(
            new GqlStatus("03N92"),
            "The query runs with exhaustive shortest path due to the existential predicate(s) `%s`. It may be possible to use `WITH` to separate the `MATCH` from the existential predicate(s).",
            "exhaustive shortest path",
            Condition.INFORMATION),
    STATUS_03N93(
            new GqlStatus("03N93"),
            "`LOAD CSV` in combination with `MATCH` or `MERGE` on a label that does not have an index may result in long execution times. Consider adding an index for label `%s`.",
            "no applicable index",
            Condition.INFORMATION),
    STATUS_03N94(
            new GqlStatus("03N94"),
            "The query execution plan contains the `Eager` operator. `LOAD CSV` in combination with `Eager` can consume a lot of memory.",
            "eager operator",
            Condition.INFORMATION),
    STATUS_03N95(
            new GqlStatus("03N95"),
            "An index exists on label/type(s) `%s`. It is not possible to use indexes for dynamic properties. Consider using static properties.",
            "dynamic property",
            Condition.INFORMATION);

    private final GqlStatus gqlStatus;
    private final String message;
    private final String subCondition;
    private final Condition condition;

    GqlStatusInfoNotifications(GqlStatus gqlStatus, String message, String subCondition, Condition condition) {
        this.gqlStatus = gqlStatus;
        this.message = message;
        this.subCondition = subCondition;
        this.condition = condition;
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
    public String getSubCondition() {
        return subCondition;
    }

    public Condition getCondition() {
        return condition;
    }
}
