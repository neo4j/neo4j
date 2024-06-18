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
package org.neo4j.cypher.internal.logical.plans.ordering

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.FullSatisfaction
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.NoSatisfaction
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder.Satisfaction
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ProvidedOrderTest extends CypherFunSuite with AstConstructionTestSupport {

  implicit val noPlan: Option[LogicalPlan] = None
  implicit val poFactory: ProvidedOrderFactory = DefaultProvidedOrderFactory

  test("should append provided order") {
    val left = ProvidedOrder.asc(varFor("a")).asc(varFor("b"))
    val right = ProvidedOrder.asc(varFor("c")).asc(varFor("d"))
    left.followedBy(right).columns should be(left.columns ++ right.columns)
  }

  test("should append empty provided order") {
    val left = ProvidedOrder.asc(varFor("a")).asc(varFor("b"))
    val right = ProvidedOrder.empty
    left.followedBy(right).columns should be(left.columns)
  }

  test("when provided order is empty the result combined provided order should always be empty") {
    val left = ProvidedOrder.empty
    val right = ProvidedOrder.asc(varFor("c")).asc(varFor("d"))
    val empty = ProvidedOrder.empty
    left.followedBy(right).columns should be(Seq.empty)
    left.followedBy(empty).columns should be(Seq.empty)
  }

  test("should trim provided order to before any matching function arguments") {
    val left = ProvidedOrder
      .asc(varFor("a"))
      .asc(varFor("b"))
      .asc(varFor("c"))
      .desc(prop("d", "prop"))
      .desc(add(literalInt(10), prop("e", "prop")))

    left.upToExcluding(Set(v"x")).columns should be(left.columns)

    left.upToExcluding(Set(v"a")).columns should be(Seq.empty)

    left.upToExcluding(Set(v"c")).columns should be(left.columns.slice(0, 2))
    left.upToExcluding(Set(v"d")).columns should be(left.columns.slice(0, 3))
    left.upToExcluding(Set(v"e")).columns should be(left.columns.slice(0, 4))

    ProvidedOrder.empty.upToExcluding(Set(v"c")).columns should be(Seq.empty)
  }

  test("should find common prefixes") {
    val mt = ProvidedOrder.empty
    val a = ProvidedOrder.asc(varFor("a"))
    val b = ProvidedOrder.asc(varFor("b"))
    val ab = a.asc(varFor("b"))
    val ac = a.asc(varFor("c"))
    val abc = ab.asc(varFor("c"))
    val abd = ab.asc(varFor("d"))

    mt.commonPrefixWith(mt) should be(mt)
    a.commonPrefixWith(mt) should be(mt)
    mt.commonPrefixWith(a) should be(mt)

    a.commonPrefixWith(a) should be(a)
    a.commonPrefixWith(b) should be(mt)
    a.commonPrefixWith(ab) should be(a)

    ab.commonPrefixWith(a) should be(a)
    ab.commonPrefixWith(ab) should be(ab)
    ab.commonPrefixWith(abc) should be(ab)
    ab.commonPrefixWith(ac) should be(a)

    abc.commonPrefixWith(abd) should be(ab)
    abc.commonPrefixWith(ab) should be(ab)
    abc.commonPrefixWith(ac) should be(a)
  }

  test("Required order is not satisfied by an empty provided order") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))

    ProvidedOrder.empty.satisfies(io) should matchPattern { case NoSatisfaction() => }
  }

  test("Required order is not satisfied by provided order in other direction") {
    val ioAsc = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
    val ioDesc = InterestingOrder.required(RequiredOrderCandidate.desc(prop("x", "foo")))

    val poAsc = ProvidedOrder.asc(prop("x", "foo"))
    val poDesc = ProvidedOrder.desc(prop("x", "foo"))

    poDesc.satisfies(ioAsc) should matchPattern { case NoSatisfaction() => }
    poAsc.satisfies(ioDesc) should matchPattern { case NoSatisfaction() => }
  }

  test("Required order should be satisfied by provided order on same property") {
    val ioAsc = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
    val ioDesc = InterestingOrder.required(RequiredOrderCandidate.desc(prop("x", "foo")))

    val poAsc = ProvidedOrder.asc(prop("x", "foo"))
    val poDesc = ProvidedOrder.desc(prop("x", "foo"))

    poAsc.satisfies(ioAsc) should matchPattern { case FullSatisfaction() => }
    poDesc.satisfies(ioDesc) should matchPattern { case FullSatisfaction() => }
  }

  test("Required order should be satisfied by provided order on same renamed property") {
    val projectionSeveralSteps = Map[LogicalVariable, Expression](v"x" -> varFor("y"), v"y" -> varFor("z"))
    val projectionOneStep = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))

    val ioAsc = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo"), projectionSeveralSteps))
    val ioDesc = InterestingOrder.required(RequiredOrderCandidate.desc(varFor("xfoo"), projectionOneStep))

    val poAsc = ProvidedOrder.asc(prop("z", "foo"))
    val poDesc = ProvidedOrder.desc(prop("x", "foo"))

    poAsc.satisfies(ioAsc) should matchPattern { case FullSatisfaction() => }
    poDesc.satisfies(ioDesc) should matchPattern { case FullSatisfaction() => }
  }

  test("Required order should not be satisfied by provided order on different property") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo")))
    val po = ProvidedOrder.asc(prop("y", "foo"))

    po.satisfies(io) should matchPattern { case NoSatisfaction() => }
  }

  test("Required order on renamed property should not be satisfied by provided order on different property") {
    val projection = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))

    val io = InterestingOrder.required(RequiredOrderCandidate.desc(varFor("xfoo"), projection))
    val po = ProvidedOrder.desc(prop("y", "foo"))

    po.satisfies(io) should matchPattern { case NoSatisfaction() => }
  }

  test("Required order on expression is not satisfied by provided order on property") {
    val projection =
      Map[LogicalVariable, Expression](v"add" -> Add(prop("x", "foo"), SignedDecimalIntegerLiteral("42")(pos))(pos))

    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("add"), projection))
    val po = ProvidedOrder.asc(prop("x", "foo"))

    po.satisfies(io) should matchPattern { case NoSatisfaction() => }
  }

  // Test partial satisfaction

  test("Empty required order satisfied by anything") {
    ProvidedOrder.empty.satisfies(InterestingOrder.empty) should matchPattern { case FullSatisfaction() => }
    ProvidedOrder.asc(varFor("x")).satisfies(InterestingOrder.empty) should matchPattern { case FullSatisfaction() =>
    }
    ProvidedOrder.desc(varFor("x")).satisfies(InterestingOrder.empty) should matchPattern { case FullSatisfaction() =>
    }
    ProvidedOrder.asc(varFor("x")).asc(varFor("y")).satisfies(InterestingOrder.empty) should matchPattern {
      case FullSatisfaction() =>
    }
    ProvidedOrder.desc(varFor("x")).desc(varFor("y")).satisfies(InterestingOrder.empty) should matchPattern {
      case FullSatisfaction() =>
    }
  }

  test("Single property required order satisfied by matching provided order") {
    ProvidedOrder.asc(varFor("x")).satisfies(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")))
    ) should matchPattern {
      case FullSatisfaction() =>
    }
  }

  test("Single property required order satisfied by longer provided order") {
    ProvidedOrder.asc(varFor("x")).asc(varFor("y")).satisfies(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")))
    ) should matchPattern {
      case FullSatisfaction() =>
    }
    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).satisfies(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")))
    ) should matchPattern {
      case FullSatisfaction() =>
    }
  }

  test("Single property required order not satisfied by mismatching provided order") {
    ProvidedOrder.asc(varFor("y")).satisfies(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")))
    ) should matchPattern {
      case NoSatisfaction() =>
    }
    ProvidedOrder.desc(varFor("x")).satisfies(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")))
    ) should matchPattern {
      case NoSatisfaction() =>
    }
    ProvidedOrder.asc(varFor("y")).asc(varFor("x")).satisfies(
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")))
    ) should matchPattern {
      case NoSatisfaction() =>
    }
  }

  test("Multi property required order can yield partial satisfaction") {
    val interestingOrder =
      InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")).desc(varFor("y")).asc(varFor("z")))

    ProvidedOrder.asc(varFor("x")).satisfies(interestingOrder) should be(Satisfaction(
      Seq(Asc(varFor("x"))),
      Seq(Desc(varFor("y")), Asc(varFor("z")))
    ))
    ProvidedOrder.desc(varFor("x")).satisfies(interestingOrder) should matchPattern { case NoSatisfaction() => }
    ProvidedOrder.asc(varFor("y")).satisfies(interestingOrder) should matchPattern { case NoSatisfaction() => }

    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).satisfies(interestingOrder) should be(Satisfaction(
      Seq(Asc(varFor("x")), Desc(varFor("y"))),
      Seq(Asc(varFor("z")))
    ))
    ProvidedOrder.asc(varFor("x")).asc(varFor("y")).satisfies(interestingOrder) should be(Satisfaction(
      Seq(Asc(varFor("x"))),
      Seq(Desc(varFor("y")), Asc(varFor("z")))
    ))
    ProvidedOrder.asc(varFor("x")).asc(varFor("z")).satisfies(interestingOrder) should be(Satisfaction(
      Seq(Asc(varFor("x"))),
      Seq(Desc(varFor("y")), Asc(varFor("z")))
    ))
    ProvidedOrder.desc(varFor("x")).desc(varFor("y")).satisfies(interestingOrder) should matchPattern {
      case NoSatisfaction() =>
    }

    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("z")).satisfies(interestingOrder) should matchPattern {
      case FullSatisfaction() =>
    }
    ProvidedOrder.asc(varFor("x")).desc(varFor("z")).asc(varFor("y")).satisfies(interestingOrder) should be(
      Satisfaction(Seq(Asc(varFor("x"))), Seq(Desc(varFor("y")), Asc(varFor("z"))))
    )
    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).desc(varFor("z")).satisfies(interestingOrder) should be(
      Satisfaction(Seq(Asc(varFor("x")), Desc(varFor("y"))), Seq(Asc(varFor("z"))))
    )
    ProvidedOrder.asc(varFor("x")).asc(varFor("y")).desc(varFor("z")).satisfies(interestingOrder) should be(
      Satisfaction(Seq(Asc(varFor("x"))), Seq(Desc(varFor("y")), Asc(varFor("z"))))
    )
    ProvidedOrder.asc(varFor("x")).asc(varFor("y")).asc(varFor("z")).satisfies(interestingOrder) should be(
      Satisfaction(Seq(Asc(varFor("x"))), Seq(Desc(varFor("y")), Asc(varFor("z"))))
    )
    ProvidedOrder.desc(varFor("x")).desc(varFor("y")).asc(varFor("z")).satisfies(interestingOrder) should matchPattern {
      case NoSatisfaction() =>
    }

    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("z")).asc(varFor("a")).satisfies(
      interestingOrder
    ) should matchPattern { case FullSatisfaction() => }
    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("a")).asc(varFor("z")).satisfies(
      interestingOrder
    ) should be(Satisfaction(
      Seq(Asc(varFor("x")), Desc(varFor("y"))),
      Seq(Asc(varFor("z")))
    ))
    ProvidedOrder.asc(varFor("a")).asc(varFor("x")).desc(varFor("y")).asc(varFor("z")).satisfies(
      interestingOrder
    ) should matchPattern { case NoSatisfaction() => }
  }

  test("Multi property required order (with projections) can yield partial satisfaction") {
    val projection = Map[LogicalVariable, Expression](v"newX" -> varFor("x"))
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("newX"), projection).desc(
      varFor("y"),
      projection
    ).asc(varFor("z"), projection))

    ProvidedOrder.asc(varFor("x")).satisfies(interestingOrder) should be(Satisfaction(
      Seq(Asc(varFor("newX"), projection)),
      Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
    ))
    ProvidedOrder.desc(varFor("x")).satisfies(interestingOrder) should matchPattern { case NoSatisfaction() => }
    ProvidedOrder.asc(varFor("y")).satisfies(interestingOrder) should matchPattern { case NoSatisfaction() => }

    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).satisfies(interestingOrder) should be(Satisfaction(
      Seq(Asc(varFor("newX"), projection), Desc(varFor("y"), projection)),
      Seq(Asc(varFor("z"), projection))
    ))
    ProvidedOrder.asc(varFor("x")).asc(varFor("y")).satisfies(interestingOrder) should be(Satisfaction(
      Seq(Asc(varFor("newX"), projection)),
      Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
    ))
    ProvidedOrder.asc(varFor("x")).asc(varFor("z")).satisfies(interestingOrder) should be(Satisfaction(
      Seq(Asc(varFor("newX"), projection)),
      Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
    ))
    ProvidedOrder.desc(varFor("x")).desc(varFor("y")).satisfies(interestingOrder) should matchPattern {
      case NoSatisfaction() =>
    }

    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("z")).satisfies(interestingOrder) should matchPattern {
      case FullSatisfaction() =>
    }
    ProvidedOrder.asc(varFor("x")).desc(varFor("z")).asc(varFor("y")).satisfies(interestingOrder) should be(
      Satisfaction(
        Seq(Asc(varFor("newX"), projection)),
        Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
      )
    )
    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).desc(varFor("z")).satisfies(interestingOrder) should be(
      Satisfaction(
        Seq(Asc(varFor("newX"), projection), Desc(varFor("y"), projection)),
        Seq(Asc(varFor("z"), projection))
      )
    )
    ProvidedOrder.asc(varFor("x")).asc(varFor("y")).desc(varFor("z")).satisfies(interestingOrder) should be(
      Satisfaction(
        Seq(Asc(varFor("newX"), projection)),
        Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
      )
    )
    ProvidedOrder.asc(varFor("x")).asc(varFor("y")).asc(varFor("z")).satisfies(interestingOrder) should be(
      Satisfaction(
        Seq(Asc(varFor("newX"), projection)),
        Seq(Desc(varFor("y"), projection), Asc(varFor("z"), projection))
      )
    )
    ProvidedOrder.desc(varFor("x")).desc(varFor("y")).asc(varFor("z")).satisfies(interestingOrder) should matchPattern {
      case NoSatisfaction() =>
    }

    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("z")).asc(varFor("a")).satisfies(
      interestingOrder
    ) should matchPattern { case FullSatisfaction() => }
    ProvidedOrder.asc(varFor("x")).desc(varFor("y")).asc(varFor("a")).asc(varFor("z")).satisfies(
      interestingOrder
    ) should be(Satisfaction(
      Seq(Asc(varFor("newX"), projection), Desc(varFor("y"), projection)),
      Seq(Asc(varFor("z"), projection))
    ))
    ProvidedOrder.asc(varFor("a")).asc(varFor("x")).desc(varFor("y")).asc(varFor("z")).satisfies(
      interestingOrder
    ) should matchPattern { case NoSatisfaction() => }
  }

  test("Function invocation rand should never satisfy interesting order") {
    val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(function("rand")))
    ProvidedOrder.asc(function("rand")).satisfies(interestingOrder) should matchPattern { case NoSatisfaction() => }
  }

}
