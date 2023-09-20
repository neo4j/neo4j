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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.Counter
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTGeometry
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.EMPTY_STRING
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.TRUE
import org.neo4j.values.storable.Values.doubleValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.Values.stringArray
import org.neo4j.values.storable.Values.stringValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.EMPTY_LIST
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.neo4j.values.virtual.VirtualValues.list
import org.neo4j.values.virtual.VirtualValues.nodeValue
import org.neo4j.values.virtual.VirtualValues.relationshipValue

import scala.language.postfixOps

class CoerceToTest extends CypherFunSuite {

  implicit val openCases: Counter = Counter()
  implicit val qtx: QueryContext = mock[QueryContext]
  implicit val state: QueryState = QueryStateHelper.emptyWith(query = qtx)

  val basicTypes: Set[CypherType] = Set(CTAny, CTBoolean, CTString, CTNumber, CTInteger, CTFloat, CTPoint)
  val graphTypes: Set[CypherType] = Set(CTNode, CTRelationship, CTPath)

  val level0Types: Set[CypherType] = basicTypes ++ graphTypes
  val level1Types: Set[CypherType] = level0Types.map(CTList).toSet + CTMap
  val level2Types: Set[CypherType] = (level1Types ++ level0Types).map(t => CTList(t))

  val testedTypes: Set[CypherType] = level2Types ++ level1Types ++ level0Types

  test("null") {
    testedTypes
      .coerce(NO_VALUE)
      .forRemainingTypes { typ => _.to(typ) unchanged }
  }

  test("POINT") {
    testedTypes
      .coerce(mock[PointValue])
      .to(CTAny).unchanged
      .to(CTPoint).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("BOOLEAN") {
    testedTypes
      .coerce(TRUE)
      .to(CTAny).unchanged
      .to(CTBoolean).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }

    testedTypes
      .coerce(TRUE)
      .to(CTAny).unchanged
      .to(CTBoolean).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("STRING") {
    testedTypes
      .coerce(EMPTY_STRING)
      .to(CTAny).unchanged
      .to(CTString).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }

    testedTypes
      .coerce(stringValue("Hello"))
      .to(CTAny).unchanged
      .to(CTString).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("NODE without properties") {
    testedTypes
      .coerce(nodeValue(11L, "n", stringArray("L"), EMPTY_MAP))
      .to(CTAny).unchanged
      .to(CTNode).unchanged
      .to(CTMap).changedButNotNull
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("NODE with properties") {
    testedTypes
      .coerce(nodeValue(11L, "n", stringArray("L"), VirtualValues.map(Array("prop"), Array(Values.longValue(44L)))))
      .to(CTAny).unchanged
      .to(CTNode).unchanged
      .to(CTMap).changedTo(VirtualValues.map(Array("prop"), Array(Values.longValue(44L))))
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("RELATIONSHIP without properties") {
    testedTypes
      .coerce(relationshipValue(
        11L,
        "r",
        nodeValue(11L, "n1", stringArray("L"), EMPTY_MAP),
        nodeValue(12L, "n2", stringArray("L"), EMPTY_MAP),
        stringValue("T"),
        EMPTY_MAP
      ))
      .to(CTAny).unchanged
      .to(CTRelationship).unchanged
      .to(CTMap).changedButNotNull
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("RELATIONSHIP with properties") {
    testedTypes
      .coerce(relationshipValue(
        11L,
        "r",
        nodeValue(11L, "n1", stringArray("L"), EMPTY_MAP),
        nodeValue(12L, "n2", stringArray("L"), EMPTY_MAP),
        stringValue("T"),
        VirtualValues.map(Array("prop"), Array(Values.longValue(44L)))
      ))
      .to(CTAny).unchanged
      .to(CTRelationship).unchanged
      .to(CTMap).changedTo(VirtualValues.map(Array("prop"), Array(Values.longValue(44L))))
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("INTEGER") {
    testedTypes
      .coerce(longValue(1L))
      .to(CTAny).unchanged
      .to(CTNumber).unchanged
      .to(CTInteger).unchanged
      .to(CTFloat).changedTo(doubleValue(1.0d))
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("FLOAT") {
    testedTypes
      .coerce(doubleValue(4.2d))
      .to(CTAny).unchanged
      .to(CTNumber).unchanged
      .to(CTInteger).changedTo(longValue(4L))
      .to(CTFloat).changedButNotNull
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("MAP") {
    Seq(
      EMPTY_MAP
    ).foreach { value =>
      testedTypes
        .coerce(value)
        .to(CTAny).unchanged
        .to(CTMap).changedTo(EMPTY_MAP)
        // TODO: IsCollection/IsMap behaviour - Discuss
        .to(CTList(CTAny)).changedTo(list(EMPTY_MAP))
        .to(CTList(CTMap)).changedTo(list(EMPTY_MAP))
        .to(CTList(CTList(CTAny))).changedTo(list(list(EMPTY_MAP)))
        .to(CTList(CTList(CTMap))).changedTo(list(list(EMPTY_MAP)))
        .forRemainingTypes { typ => _.notTo(typ) }
    }
  }

  test("LIST") {
    Seq(
      EMPTY_LIST
    ).foreach { value =>
      testedTypes
        .coerce(value)
        .to(CTAny).unchanged
        .to(CTList(CTAny)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTString))).changedTo(EMPTY_LIST)
        .to(CTList(CTString)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTBoolean))).changedTo(EMPTY_LIST)
        .to(CTList(CTBoolean)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTInteger))).changedTo(EMPTY_LIST)
        .to(CTList(CTInteger)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTFloat))).changedTo(EMPTY_LIST)
        .to(CTList(CTFloat)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTNumber))).changedTo(EMPTY_LIST)
        .to(CTList(CTNumber)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTPoint))).changedTo(EMPTY_LIST)
        .to(CTList(CTPoint)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTGeometry))).changedTo(EMPTY_LIST)
        .to(CTList(CTGeometry)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTNode))).changedTo(EMPTY_LIST)
        .to(CTList(CTNode)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTRelationship))).changedTo(EMPTY_LIST)
        .to(CTList(CTRelationship)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTPath))).changedTo(EMPTY_LIST)
        .to(CTList(CTPath)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTMap))).changedTo(EMPTY_LIST)
        .to(CTList(CTMap)).changedTo(EMPTY_LIST)
        .to(CTList(CTList(CTAny))).changedTo(EMPTY_LIST)
        .to(CTList(CTAny)).changedTo(EMPTY_LIST)
        .forRemainingTypes { typ => _.notTo(typ) }
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    openCases.reset()
  }

  override protected def afterEach(): Unit = {
    val left = openCases.counted
    if (left > 0) {
      throw new IllegalStateException(
        s"Did not properly finish coercion: Left $left test case(s) open by not calling unchanged(), changedTo(), or changedButNotNull()"
      )
    }
    super.afterEach()
  }

  implicit class RichTypes(allTypes: Set[CypherType]) {

    case class coerce(actualValue: AnyValue)(implicit counter: Counter) {
      self =>

      private var remaining: Set[CypherType] = allTypes

      def notTo(typ: CypherType) = {
        a[CypherTypeException] should be thrownBy {
          CoerceTo(TestExpression(actualValue), typ)(CypherRow.empty, state)
        }

        remaining -= typ

        self
      }

      case class TestExpression(in: AnyValue) extends Expression {

        override def rewrite(f: Expression => Expression): Expression = this

        override def arguments: Seq[Expression] = Seq.empty

        override def children: Seq[AstNode[_]] = Seq.empty

        override def apply(row: ReadableRow, state: QueryState): AnyValue = in

      }

      case class to(typ: CypherType) {
        private val coercedValue = CoerceTo(TestExpression(actualValue), typ)(CypherRow.empty, state)

        counter += 1
        remaining -= typ

        def changedButNotNull = {
          coercedValue should not equal null
          counter -= 1
          self
        }

        def unchanged = {
          coercedValue should equal(actualValue)
          counter -= 1
          self
        }

        def changedTo(expectedValue: Any) = {
          coercedValue should equal(expectedValue)
          counter -= 1
          self
        }
      }

      def forAllTypes(f: CypherType => coerce => coerce): coerce =
        forTypes(allTypes)(f)

      def forRemainingTypes(f: CypherType => coerce => coerce): coerce =
        forTypes(remaining)(f)

      def forTypes(types: Set[CypherType])(f: CypherType => coerce => coerce): coerce =
        types.foldLeft(self) {
          case (coercer, elt) => f(elt)(coercer)
        }
    }
  }
}
