/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.planning.notification

import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.when
import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.compiler.SuboptimalIndexForConstainsQueryNotification
import org.neo4j.cypher.internal.compiler.SuboptimalIndexForEndsWithQueryNotification
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexSeek.relationshipIndexSeek
import org.neo4j.cypher.internal.planner.spi
import org.neo4j.cypher.internal.planner.spi.IndexBehaviour
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType.Range
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.SlowContains
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class CheckForIndexBehaviourTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should notify for NodeIndexContainsScan backed by limited index") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForLabelAndProperties(anyString(), any())).thenReturn(Some(spi.IndexDescriptor.forLabel(Range, LabelId(1), Seq(PropertyKeyId(1))).withBehaviours(Set[IndexBehaviour](SlowContains))))
    val plan = nodeIndexSeek("id:label(prop CONTAINS 'tron')")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should equal(Set(SuboptimalIndexForConstainsQueryNotification("id", "label", Seq("prop"), EntityType.NODE)))
  }

  test("should notify for NodeIndexEndsWithScan backed by limited index") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForLabelAndProperties(anyString(), any())).thenReturn(Some(IndexDescriptor.forLabel(Range, LabelId(1), Seq(PropertyKeyId(1))).withBehaviours(Set[IndexBehaviour](SlowContains))))
    val plan = nodeIndexSeek("id:label(prop ENDS WITH 'tron')")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should equal(Set(SuboptimalIndexForEndsWithQueryNotification("id", "label", Seq("prop"), EntityType.NODE)))
  }

  test("should not notify for NodeIndexContainsScan backed by index with no limitations") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForLabelAndProperties(anyString(), any())).thenReturn(Some(IndexDescriptor.forLabel(Range, LabelId(1), Seq(PropertyKeyId(1))).withBehaviours(Set.empty[IndexBehaviour])))
    val plan = nodeIndexSeek("id:label(prop CONTAINS 'tron')")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should be(empty)
  }

  test("should not notify for NodeIndexEndsWithScan backed by index with no limitations") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForLabelAndProperties(anyString(), any())).thenReturn(Some(IndexDescriptor.forLabel(Range, LabelId(1), Seq(PropertyKeyId(1))).withBehaviours(Set.empty[IndexBehaviour])))
    val plan = nodeIndexSeek("id:label(prop ENDS WITH 'tron')")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should be(empty)
  }

  test("should notify for UndirectedRelationshipIndexContainsScan backed by limited index") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForRelTypeAndProperties(anyString(), any())).thenReturn(Some(spi.IndexDescriptor.forRelType(Range, RelTypeId(1), Seq(PropertyKeyId(1))).withBehaviours(Set[IndexBehaviour](SlowContains))))
    val plan = relationshipIndexSeek("(a)-[id:REL(prop CONTAINS 'tron')]-(b)")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should equal(Set(SuboptimalIndexForConstainsQueryNotification("id", "REL", Seq("prop"), EntityType.RELATIONSHIP)))
  }

  test("should notify for UndirectedRelationshipIndexEndsWithScan backed by limited index") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForRelTypeAndProperties(anyString(), any())).thenReturn(Some(spi.IndexDescriptor.forRelType(Range, RelTypeId(1), Seq(PropertyKeyId(1))).withBehaviours(Set[IndexBehaviour](SlowContains))))
    val plan = relationshipIndexSeek("(a)-[id:REL(prop ENDS WITH 'tron')]-(b)")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should equal(Set(SuboptimalIndexForEndsWithQueryNotification("id", "REL", Seq("prop"), EntityType.RELATIONSHIP)))
  }

  test("should not notify for UndirectedRelationshipIndexContainsScan backed index with no limitations") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForRelTypeAndProperties(anyString(), any())).thenReturn(Some(spi.IndexDescriptor.forRelType(Range, RelTypeId(1), Seq(PropertyKeyId(1))).withBehaviours(Set.empty[IndexBehaviour])))
    val plan = relationshipIndexSeek("(a)-[id:REL(prop CONTAINS 'tron')]-(b)")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should be(empty)
  }

  test("should not notify for UndirectedRelationshipIndexEndsWithScan backed index with no limitations") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForRelTypeAndProperties(anyString(), any())).thenReturn(Some(spi.IndexDescriptor.forRelType(Range, RelTypeId(1), Seq(PropertyKeyId(1))).withBehaviours(Set.empty[IndexBehaviour])))
    val plan = relationshipIndexSeek("(a)-[id:REL(prop ENDS WITH 'tron')]-(b)")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should be(empty)
  }

  test("should notify for DirectedRelationshipIndexContainsScan backed by limited index") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForRelTypeAndProperties(anyString(), any())).thenReturn(Some(spi.IndexDescriptor.forRelType(Range, RelTypeId(1), Seq(PropertyKeyId(1))).withBehaviours(Set[IndexBehaviour](SlowContains))))
    val plan = relationshipIndexSeek("(a)-[id:REL(prop CONTAINS 'tron')]->(b)")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should equal(Set(SuboptimalIndexForConstainsQueryNotification("id", "REL", Seq("prop"), EntityType.RELATIONSHIP)))
  }

  test("should notify for DirectedRelationshipIndexEndsWithScan backed by limited index") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForRelTypeAndProperties(anyString(), any())).thenReturn(Some(spi.IndexDescriptor.forRelType(Range, RelTypeId(1), Seq(PropertyKeyId(1))).withBehaviours(Set[IndexBehaviour](SlowContains))))
    val plan = relationshipIndexSeek("(a)-[id:REL(prop ENDS WITH 'tron')]->(b)")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should equal(Set(SuboptimalIndexForEndsWithQueryNotification("id", "REL", Seq("prop"), EntityType.RELATIONSHIP)))
  }

  test("should not notify for DirectedRelationshipIndexContainsScan backed index with no limitations") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForRelTypeAndProperties(anyString(), any())).thenReturn(Some(spi.IndexDescriptor.forRelType(Range, RelTypeId(1), Seq(PropertyKeyId(1))).withBehaviours(Set.empty[IndexBehaviour])))
    val plan = relationshipIndexSeek("(a)-[id:REL(prop CONTAINS 'tron')]->(b)")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should be(empty)
  }

  test("should not notify for DirectedRelationshipIndexEndsWithScan backed index with no limitations") {
    val planContext = mock[PlanContext]
    when(planContext.rangeIndexGetForRelTypeAndProperties(anyString(), any())).thenReturn(Some(spi.IndexDescriptor.forRelType(Range, RelTypeId(1), Seq(PropertyKeyId(1))).withBehaviours(Set.empty[IndexBehaviour])))
    val plan = relationshipIndexSeek("(a)-[id:REL(prop ENDS WITH 'tron')]->(b)")

    checkForSuboptimalIndexBehaviours(planContext)(plan) should be(empty)
  }
}
