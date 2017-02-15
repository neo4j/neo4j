package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, IncomparableValuesException, NewPlannerTestSupport}

class HelpfulErrorMessagesTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should provide sensible error message when ordering by mixed types") {
    withEachPlanner { execute =>
      val exception = intercept[IncomparableValuesException](execute("UNWIND {things} AS thing RETURN thing ORDER BY thing", Seq("things" -> List("1", 2))))
      exception.getMessage should startWith("Cannot perform ORDER BY on mixed types.")
    }
  }

  test("should provide sensible error message when ordering by list values") {
    withEachPlanner { execute =>
      val exception = intercept[IncomparableValuesException](execute("UNWIND {things} AS thing RETURN thing ORDER BY thing", Seq("things" -> List(List("a"),List("b")))))
      exception.getMessage should startWith("Cannot perform ORDER BY on lists, consider using UNWIND.")
    }
  }

  test("should provide sensible error message when aggregating by min on mixed types") {
    withEachPlanner { execute =>
      val exception = intercept[IncomparableValuesException](execute("UNWIND {things} AS thing RETURN min(thing)", Seq("things" -> List("1", 2))))
      exception.getMessage should startWith("Cannot perform MIN on mixed types.")
    }
  }

  test("should provide sensible error message when aggregating by max on mixed types") {
    withEachPlanner { execute =>
      val exception = intercept[IncomparableValuesException](execute("UNWIND {things} AS thing RETURN max(thing)", Seq("things" -> List("1", 2))))
      exception.getMessage should startWith("Cannot perform MAX on mixed types.")
    }
  }
}
