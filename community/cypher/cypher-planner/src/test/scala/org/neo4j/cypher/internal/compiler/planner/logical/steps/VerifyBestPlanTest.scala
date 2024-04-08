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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.when
import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingIndexHintType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingTextIndexType
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.RecordingNotificationLogger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.HintException
import org.neo4j.exceptions.IndexHintException
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.InvalidHintException
import org.neo4j.exceptions.JoinHintException
import org.neo4j.notifications.IndexHintUnfulfillableNotification
import org.neo4j.notifications.JoinHintUnfulfillableNotification

class VerifyBestPlanTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private def newNodeIndexHint(indexType: UsingIndexHintType = UsingAnyIndexType): Hint =
    UsingIndexHint(v"a", labelOrRelTypeName("User"), Seq(PropertyKeyName("name")(pos)), indexType = indexType) _

  private def newRelationshipIndexHint(indexType: UsingIndexHintType = UsingAnyIndexType): Hint =
    UsingIndexHint(v"r", labelOrRelTypeName("User"), Seq(PropertyKeyName("name")(pos)), indexType = indexType) _

  private def newJoinHint(): Hint = { UsingJoinHint(Seq(v"a")) _ }

  private def newQueryWithNodeIndexHint(
    indexType: UsingIndexHintType = UsingAnyIndexType,
    selections: Selections = Selections()
  ) = RegularSinglePlannerQuery(
    QueryGraph(
      patternNodes = Set(v"a", v"b")
    ).addHints(Set(newNodeIndexHint(indexType))).addSelections(selections)
  )

  private def newQueryWithRelationshipIndexHint(
    indexType: UsingIndexHintType = UsingAnyIndexType,
    selections: Selections = Selections()
  ) = RegularSinglePlannerQuery(
    QueryGraph(
      patternNodes = Set(v"a", v"b"),
      patternRelationships = Set(PatternRelationship(v"r", (v"a", v"b"), BOTH, Seq.empty, SimplePatternLength))
    ).addHints(Set(newRelationshipIndexHint(indexType))).addSelections(selections)
  )

  private def newQueryWithJoinHint() = RegularSinglePlannerQuery(
    QueryGraph(
      patternNodes = Set(v"a", v"b")
    ).addHints(Set(newJoinHint()))
  )

  private def getPlanContext(
    hasIndex: Boolean,
    hasTextIndex: Boolean = false
  ): PlanContext = {
    val planContext = newMockedPlanContext()
    when(planContext.rangeIndexExistsForLabelAndProperties(anyString(), any())).thenReturn(hasIndex)
    when(planContext.rangeIndexExistsForRelTypeAndProperties(anyString(), any())).thenReturn(hasIndex)
    when(planContext.textIndexExistsForLabelAndProperties(anyString(), any())).thenReturn(hasTextIndex)
    when(planContext.textIndexExistsForRelTypeAndProperties(anyString(), any())).thenReturn(hasTextIndex)
    planContext
  }

  private def getSimpleLogicalPlanWithAandB(
    context: LogicalPlanningContext,
    selections: Selections = Selections()
  ): LogicalPlan = {
    newMockedLogicalPlan(Set("a", "b"), context.staticComponents.planningAttributes, selections = selections)
  }

  private def getSimpleLogicalPlanWithAandBandR(
    context: LogicalPlanningContext,
    selections: Selections = Selections()
  ): LogicalPlan = {
    newMockedLogicalPlanWithPatterns(
      context.staticComponents.planningAttributes,
      Set("a", "b"),
      Set(PatternRelationship(v"r", (v"a", v"b"), BOTH, Seq.empty, SimplePatternLength)),
      selections = selections
    )
  }

  private def getSemanticTable: SemanticTable = {
    val semanticTable = newMockedSemanticTable
    val nodeTypeGetter = SemanticTable.TypeGetter(Some(CTNode.invariant))
    val relTypeGetter = SemanticTable.TypeGetter(Some(CTRelationship.invariant))
    val stringTypeGetter = SemanticTable.TypeGetter(Some(CTString.invariant))

    when(semanticTable.typeFor("a")).thenReturn(nodeTypeGetter)
    when(semanticTable.typeFor(v"a")).thenReturn(nodeTypeGetter)

    when(semanticTable.typeFor("b")).thenReturn(nodeTypeGetter)
    when(semanticTable.typeFor(v"b")).thenReturn(nodeTypeGetter)

    when(semanticTable.typeFor("r")).thenReturn(relTypeGetter)
    when(semanticTable.typeFor(v"r")).thenReturn(relTypeGetter)

    when(semanticTable.typeFor(StringLiteral("test")(InputPosition.NONE.withInputLength(0)))).thenReturn(
      stringTypeGetter
    )

    semanticTable
  }

  private val cannotUseTextIndexMessage =
    "Cannot use text index hint `USING TEXT INDEX a:User(name)` in this context: The hint specifies using a text index but no matching predicate was found. " +
      "For more information on when a text index is applicable, please consult the documentation on the use of text indexes."

  test("should throw when finding plan that does not solve all pattern nodes") {
    val query = RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set(v"a", v"b")
      )
    )
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())
    val plan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "b")

    a[InternalException] should be thrownBy {
      VerifyBestPlan(plan, query, context)
    }
  }

  test("should throw when finding plan that does not solve all pattern relationships") {
    val patternRel =
      PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, VarPatternLength.unlimited)
    val query = RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set(v"a", v"b"),
        patternRelationships = Set(patternRel)
      )
    )
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    a[InternalException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), query, context)
    }
  }

  test("should not throw when finding plan that does solve all pattern nodes") {
    val query = RegularSinglePlannerQuery(
      QueryGraph(
        patternNodes = Set(v"a", v"b")
      )
    )
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), query, context) // should not throw
  }

  test("should throw when finding plan that contains unfulfillable node index hint") {
    val context =
      newMockedLogicalPlanningContext(
        planContext = getPlanContext(hasIndex = false),
        useErrorsOverWarnings = true
      )

    the[IndexHintException] thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithNodeIndexHint(), context)
    } should have message "No such index: INDEX FOR (`a`:`User`) ON (`a`.`name`)"
  }

  test("should throw when finding plan that contains unfulfillable relationship index hint") {
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = false),
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = true
    )

    the[IndexHintException] thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandBandR(context), newQueryWithRelationshipIndexHint(), context)
    } should have message "No such index: INDEX FOR ()-[`r`:`User`]-() ON (`r`.`name`)"
  }

  test("should throw when finding plan that contains unfulfillable join hint") {
    val context =
      newMockedLogicalPlanningContext(planContext = getPlanContext(hasIndex = false), useErrorsOverWarnings = true)

    a[JoinHintException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithJoinHint(), context)
    }
  }

  test("should issue warning when finding plan that contains unfulfillable node index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = false),
      notificationLogger = notificationLogger,
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = false
    )

    VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithNodeIndexHint(), context) // should not throw
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification(
      "a",
      "User",
      Seq("name"),
      EntityType.NODE,
      UsingAnyIndexType
    ))
  }

  test("should issue warning when finding plan that contains unfulfillable relationship index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = false),
      notificationLogger = notificationLogger,
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = false
    )

    VerifyBestPlan(
      getSimpleLogicalPlanWithAandBandR(context),
      newQueryWithRelationshipIndexHint(),
      context
    ) // should not throw
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification(
      "r",
      "User",
      Seq("name"),
      EntityType.RELATIONSHIP,
      UsingAnyIndexType
    ))
  }

  test("should issue warning when finding plan that contains unfulfillable node text index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = true, hasTextIndex = false),
      notificationLogger = notificationLogger,
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = false
    )

    VerifyBestPlan(
      getSimpleLogicalPlanWithAandB(context, selections = textSelections("a")),
      newQueryWithNodeIndexHint(UsingTextIndexType, textSelections("a")),
      context
    ) // should not throw
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification(
      "a",
      "User",
      Seq("name"),
      EntityType.NODE,
      UsingTextIndexType
    ))
  }

  test("should issue warning when finding plan that contains unfulfillable relationship text index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = true, hasTextIndex = false),
      notificationLogger = notificationLogger,
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = false
    )

    VerifyBestPlan(
      getSimpleLogicalPlanWithAandBandR(context, selections = textSelections("r")),
      newQueryWithRelationshipIndexHint(UsingTextIndexType, textSelections("r")),
      context
    ) // should not throw
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification(
      "r",
      "User",
      Seq("name"),
      EntityType.RELATIONSHIP,
      UsingTextIndexType
    ))
  }

  test("should issue warning when finding plan that contains unfulfillable node range index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = false, hasTextIndex = true),
      notificationLogger = notificationLogger,
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = false
    )

    VerifyBestPlan(
      getSimpleLogicalPlanWithAandB(context),
      newQueryWithNodeIndexHint(UsingRangeIndexType),
      context
    ) // should not throw
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification(
      "a",
      "User",
      Seq("name"),
      EntityType.NODE,
      UsingRangeIndexType
    ))
  }

  test("should issue warning when finding plan that contains unfulfillable relationship range index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = false, hasTextIndex = true),
      notificationLogger = notificationLogger,
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = false
    )

    VerifyBestPlan(
      getSimpleLogicalPlanWithAandBandR(context),
      newQueryWithRelationshipIndexHint(UsingRangeIndexType),
      context
    ) // should not throw
    notificationLogger.notifications should contain(IndexHintUnfulfillableNotification(
      "r",
      "User",
      Seq("name"),
      EntityType.RELATIONSHIP,
      UsingRangeIndexType
    ))
  }

  test("should issue warning when finding plan that contains unfulfillable join hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = false),
      notificationLogger = notificationLogger,
      useErrorsOverWarnings = false
    )

    VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithJoinHint(), context) // should not throw
    val result = notificationLogger.notifications
    result should contain(JoinHintUnfulfillableNotification(Array("a")))
  }

  test("should succeed when finding plan that contains fulfillable node index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = true),
      notificationLogger = notificationLogger,
      useErrorsOverWarnings = false
    )
    val plan: LogicalPlan =
      newMockedLogicalPlan(
        Set("a", "b"),
        context.staticComponents.planningAttributes,
        hints = Set[Hint](newNodeIndexHint())
      )

    VerifyBestPlan(plan, newQueryWithNodeIndexHint(), context) // should not throw
    notificationLogger.notifications should be(empty)
  }

  test("should succeed when finding plan that contains fulfillable relationship index hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = true),
      notificationLogger = notificationLogger,
      useErrorsOverWarnings = false
    )
    val plan: LogicalPlan = newMockedLogicalPlanWithPatterns(
      context.staticComponents.planningAttributes,
      Set("a", "b"),
      Set(PatternRelationship(v"r", (v"a", v"b"), BOTH, Seq.empty, SimplePatternLength)),
      hints = Set[Hint](newRelationshipIndexHint())
    )

    VerifyBestPlan(plan, newQueryWithRelationshipIndexHint(), context) // should not throw
    notificationLogger.notifications should be(empty)
  }

  test("should succeed when finding plan that contains fulfillable join hint") {
    val notificationLogger = new RecordingNotificationLogger
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = true),
      notificationLogger = notificationLogger,
      useErrorsOverWarnings = false
    )
    val plan: LogicalPlan =
      newMockedLogicalPlan(Set("a", "b"), context.staticComponents.planningAttributes, hints = Set[Hint](newJoinHint()))

    VerifyBestPlan(plan, newQueryWithJoinHint(), context) // should not throw
    notificationLogger.notifications should be(empty)
  }

  test("should throw when finding plan that does not contain a fulfillable node index hint") {
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = true),
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = false
    )

    a[HintException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithNodeIndexHint(), context)
    }
  }

  test("should throw when finding plan that does not contain a fulfillable relationship index hint") {
    val context = newMockedLogicalPlanningContext(
      planContext = getPlanContext(hasIndex = true),
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = false
    )

    a[HintException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandBandR(context), newQueryWithRelationshipIndexHint(), context)
    }
  }

  test("should throw when finding plan that does not contain a fulfillable node text index hint") {
    val planContext = newMockedPlanContext()
    when(planContext.textIndexExistsForLabelAndProperties(any(), any())).thenReturn(true)

    val context = newMockedLogicalPlanningContext(
      planContext = planContext,
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = true
    )

    a[HintException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandB(context), newQueryWithNodeIndexHint(), context)
    }
  }

  test("should throw when finding plan that does not contain a fulfillable relationship text index hint") {
    val planContext = newMockedPlanContext()
    when(planContext.textIndexExistsForRelTypeAndProperties(any(), any())).thenReturn(true)

    val context = newMockedLogicalPlanningContext(
      planContext = planContext,
      semanticTable = getSemanticTable,
      useErrorsOverWarnings = true
    )

    a[HintException] should be thrownBy {
      VerifyBestPlan(getSimpleLogicalPlanWithAandBandR(context), newQueryWithRelationshipIndexHint(), context)
    }
  }

  test("should throw when finding unfulfillable index hint in a subquery") {
    def plannerQueryWithSubquery(hints: Set[Hint]): PlannerQuery = {
      RegularSinglePlannerQuery(
        horizon = CallSubqueryHorizon(
          callSubquery = RegularSinglePlannerQuery(
            QueryGraph(
              patternNodes = Set(v"a"),
              hints = hints
            )
          ),
          correlated = false,
          yielding = true,
          inTransactionsParameters = None
        )
      )
    }

    val expected = plannerQueryWithSubquery(Set(newNodeIndexHint()))
    val solved = plannerQueryWithSubquery(Set.empty)

    val context =
      newMockedLogicalPlanningContext(planContext = getPlanContext(hasIndex = false), useErrorsOverWarnings = true)
    val plan = newMockedLogicalPlanWithSolved(context.staticComponents.planningAttributes, Set("a"), solved)

    the[IndexHintException] thrownBy {
      VerifyBestPlan(plan, expected, context)
    } should have message "No such index: INDEX FOR (`a`:`User`) ON (`a`.`name`)"
  }

  test("should throw when finding unfulfillable text index hint in a subquery") {
    def plannerQueryWithSubquery(hints: Set[Hint]): PlannerQuery = {
      RegularSinglePlannerQuery(
        horizon = CallSubqueryHorizon(
          callSubquery = RegularSinglePlannerQuery(
            QueryGraph(
              patternNodes = Set(v"a"),
              hints = hints
            )
          ),
          correlated = false,
          yielding = true,
          inTransactionsParameters = None
        )
      )
    }

    val expected = plannerQueryWithSubquery(Set(newNodeIndexHint(indexType = UsingTextIndexType)))
    val solved = plannerQueryWithSubquery(Set.empty)

    val context =
      newMockedLogicalPlanningContext(planContext = getPlanContext(hasIndex = true), useErrorsOverWarnings = true)
    val plan = newMockedLogicalPlanWithSolved(context.staticComponents.planningAttributes, Set("a"), solved)

    the[InvalidHintException] thrownBy {
      VerifyBestPlan(plan, expected, context)
    } should have message cannotUseTextIndexMessage
  }

  test("should throw when finding unfulfillable text index hint in UNION") {
    def plannerUnionQuery(hints: Set[Hint]): PlannerQuery = {
      UnionQuery(
        rhs = RegularSinglePlannerQuery(
          QueryGraph(
            patternNodes = Set(v"a"),
            hints = hints
          )
        ),
        lhs = RegularSinglePlannerQuery(
          QueryGraph(patternNodes = Set(v"a"))
        ),
        distinct = true,
        unionMappings = List.empty
      )
    }

    val expected = plannerUnionQuery(Set(newNodeIndexHint(indexType = UsingTextIndexType)))
    val solved = plannerUnionQuery(Set.empty)

    val context =
      newMockedLogicalPlanningContext(planContext = getPlanContext(hasIndex = true), useErrorsOverWarnings = true)
    val plan = newMockedLogicalPlanWithSolved(context.staticComponents.planningAttributes, Set("a"), solved)

    the[InvalidHintException] thrownBy {
      VerifyBestPlan(plan, expected, context)
    } should have message cannotUseTextIndexMessage
  }

  test("should throw when finding unfulfillable text index hint in OPTIONAL MATCH") {
    def plannerQueryWithOptionalMatch(hints: Set[Hint]): PlannerQuery = {
      RegularSinglePlannerQuery(
        QueryGraph.empty.addOptionalMatch(
          QueryGraph(
            patternNodes = Set(v"a"),
            hints = hints
          )
        )
      )
    }

    val expected = plannerQueryWithOptionalMatch(Set(newNodeIndexHint(indexType = UsingTextIndexType)))
    val solved = plannerQueryWithOptionalMatch(Set.empty)

    val context =
      newMockedLogicalPlanningContext(planContext = getPlanContext(hasIndex = true), useErrorsOverWarnings = true)
    val plan = newMockedLogicalPlanWithSolved(context.staticComponents.planningAttributes, Set("a"), solved)

    the[InvalidHintException] thrownBy {
      VerifyBestPlan(plan, expected, context)
    } should have message cannotUseTextIndexMessage
  }
}
