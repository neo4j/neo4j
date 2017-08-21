package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.expressions.ReferenceFromRegister
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, LongSlot, PipelineInformation, RefSlot}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.scalatest.mock.MockitoSugar

class UnwindRegisterPipeTest extends CypherFunSuite {

  private def unwindWithInput(data: Traversable[Map[String, Any]]) = {
    val inputPipeline = PipelineInformation
      .empty
      .newReference("x", nullable = false, CTAny)

    val outputPipeline = inputPipeline
      .deepClone()
      .newReference("y", nullable = true, CTAny)

    val x = inputPipeline.getReferenceOffsetFor("x")
    val y = outputPipeline.getReferenceOffsetFor("y")

    val source = FakeRegisterPipe(data.toIterator, inputPipeline)
    val unwindPipe = UnwindRegisterPipe(source, ReferenceFromRegister(x), y, outputPipeline)()
    unwindPipe.createResults(QueryStateHelper.empty).map {
      case c: PrimitiveExecutionContext =>
        Map("x" -> c.getRefAt(x), "y" -> c.getRefAt(y))
    }.toList
  }

  test("should unwind collection of numbers") {
    unwindWithInput(List(Map("x" -> List(1, 2)))) should equal(List(
      Map("y" -> 1, "x" -> List(1, 2)),
      Map("y" -> 2, "x" -> List(1, 2))))
  }

  test("should handle null") {
    unwindWithInput(List(Map("x" -> null))) should equal(List())
  }

  test("should handle collection of collections") {

    val listOfLists = List(
      List(1, 2, 3),
      List(4, 5, 6))

    unwindWithInput(List(Map(
      "x" -> listOfLists))) should equal(

      List(
        Map("y" -> List(1, 2, 3), "x" -> listOfLists),
        Map("y" -> List(4, 5, 6), "x" -> listOfLists)))
  }
}

case class FakeRegisterPipe(data: Iterator[Map[String, Any]], pipeline: PipelineInformation)
  extends Pipe with MockitoSugar {

  def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val result = PrimitiveExecutionContext(pipeline)

    data.map { values =>
      values foreach {
        case (key, value) =>
          pipeline(key) match {
            case LongSlot(offset, _, _, _) if value == null =>
              result.setLongAt(offset, -1)

            case LongSlot(offset, _, _, _) =>
              result.setLongAt(offset, value.asInstanceOf[Number].longValue())

            case RefSlot(offset, _, _, _) =>
              result.setRefAt(offset, value)
          }
      }
      result
    }
  }

  var id = new Id
}
