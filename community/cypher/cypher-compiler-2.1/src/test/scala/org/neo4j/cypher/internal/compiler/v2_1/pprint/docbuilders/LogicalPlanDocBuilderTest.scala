package org.neo4j.cypher.internal.compiler.v2_1.pprint.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.pprint._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{LogicalPlan, IdName, LogicalLeafPlan}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import scala.Some
import org.neo4j.cypher.internal.compiler.v2_1.pprint.PrintNewLine
import org.neo4j.cypher.internal.compiler.v2_1.pprint.PrintText

class LogicalPlanDocBuilderTest extends DocBuilderTestSuite[Any] {

  val docBuilder = plannerDocBuilder orElse simpleDocBuilder

  override def defaultFormatter = DocFormatters.pageFormatter(80)

  test("Prints leaf plans") {
    format(TestLeafPlan(12)) should equal("TestLeafPlan[a](12)")
  }

  test("Prints pipe plans") {
    val doc = docGen(TestPipePlan(TestLeafPlan(1)))
    val result = condense(defaultFormatter(doc))
    result should equal(Seq(
      PrintText("TestPipePlan[b]()"),
      PrintNewLine(0),
      PrintText("↳ TestLeafPlan[a](1)")
     ))
  }

  test("Prints long pipe plans") {
    val doc = docGen(TestPipePlan(TestPipePlan(TestLeafPlan(1))))
    val result = condense(defaultFormatter(doc))
    result should equal(Seq(
      PrintText("TestPipePlan[b]()"),
      PrintNewLine(0),
      PrintText("↳ TestPipePlan[b]()"),
      PrintNewLine(0),
      PrintText("↳ TestLeafPlan[a](1)")
    ))
  }

  test("Prints combo plans") {
    val doc = docGen(TestComboPlan(TestLeafPlan(1), TestLeafPlan(2)))
    val result = condense(defaultFormatter(doc))
    result should equal(Seq(
      PrintText("TestComboPlan[c, d]()"),
      PrintNewLine(2),
      PrintText("↳ left = TestLeafPlan[a](1)"),
      PrintNewLine(2),
      PrintText("↳ right = TestLeafPlan[a](2)")
    ))
  }

  case class TestLeafPlan(x: Int) extends LogicalLeafPlan {
    def availableSymbols = Set[IdName](IdName("a"))
  }

  case class TestPipePlan(left: LogicalPlan) extends LogicalPlan {
    def lhs = Some(left)
    def rhs = None
    def availableSymbols = Set[IdName](IdName("b"))
  }

  case class TestComboPlan(left: LogicalPlan, right: LogicalPlan) extends LogicalPlan {
    def lhs = Some(left)
    def rhs = Some(right)
    def availableSymbols = Set[IdName](IdName("c"), IdName("d"))
  }
}
