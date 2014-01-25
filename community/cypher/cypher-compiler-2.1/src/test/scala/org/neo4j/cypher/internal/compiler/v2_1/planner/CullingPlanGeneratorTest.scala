package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.scalatest.FunSuite

class CullingPlanGeneratorTest extends FunSuite {
  val planGenerator = CullingPlanGenerator()

  test("empty plan returns empty") {
    val planTable = PlanTable.empty

    assert(cull(planTable) === planTable)
  }

  test("single plan is not culled") {
    val GIVEN = table(
      plan(Set(0), "plan1", 5)
    )

    val generatedPlanTable = cull(GIVEN)

    // Expected the same table back
    assert(generatedPlanTable === GIVEN)
  }

  test("two plans not covering each other are not culled") {
    val GIVEN = table(
      plan(Set(0), "plan1", 5),
      plan(Set(1), "plan2", 5)
    )

    val generatedPlanTable = cull(GIVEN)

    // Expected the same table back
    assert(generatedPlanTable === GIVEN)
  }

  test("two plans covering each other returns the cheaper of the two") {
    val GIVEN = table(
      plan(Set(0), "plan1", 10),
      plan(Set(0), "plan2", 5)
    )

    // When
    val generatedPlanTable = cull(GIVEN)

    // Then
    val EXPECTED = table(
      plan(Set(0), "plan2", 5)
    )

    assert(generatedPlanTable === EXPECTED)
  }

  test("two plans overlapping but not covering each other are not culled") {
    val GIVEN = table(
      plan(Set(0,1), "plan1", 5),
      plan(Set(1,2), "plan2", 10)
    )

    // When
    val generatedPlanTable = cull(GIVEN)

    // Then
    assert(generatedPlanTable === GIVEN)
  }

  test("plan table culls the expected plans") {
    /*Query: MATCH (a)-->(b)-->(c)*/
    val GIVEN = table(
      plan(Set(0),   "plan1", 5),
      plan(Set(1),   "plan2", 10),
      plan(Set(0,1), "plan3", 12),
      plan(Set(1,2), "plan4", 12),
      plan(Set(0,2), "plan5", 15),
      plan(Set(0,1), "plan6", 20)
    )

    // When
    val generatedPlanTable = cull(GIVEN)

    // Then
    val EXPECTED = table(
      plan(Set(0,1), "plan3", 12),
      plan(Set(1,2), "plan4", 12),
      plan(Set(0,2), "plan5", 15)
    )

    assert(generatedPlanTable === EXPECTED)
  }

  private def table(plans: Plan*): PlanTable = new PlanTable(plans)

  private def plan(ids: Set[Int], name: String, effort: Int) = Plan(ids.map(Id.apply), name, Cost(effort, 1))

  private def cull(planTable: PlanTable) = planGenerator.generatePlan(null, null, planTable)

  private case class Plan(coveredIds: Set[Id], name: String, effort: Cost) extends AbstractPlan {
    def lhs: Option[AbstractPlan] = ???

    def rhs: Option[AbstractPlan] = ???
  }
}