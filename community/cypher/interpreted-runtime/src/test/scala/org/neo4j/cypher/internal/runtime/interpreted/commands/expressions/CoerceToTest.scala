/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import java.util.{ArrayList => JavaList, HashMap => JavaMap}

import org.neo4j.cypher.internal.runtime.{Counter, QueryContext}
import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, QueryStateHelper}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values._
import org.neo4j.values.virtual.PointValue
import org.neo4j.values.virtual.VirtualValues._

import scala.language.postfixOps

class CoerceToTest extends CypherFunSuite {

  implicit var openCases: Counter = Counter()
  implicit val qtx = mock[QueryContext]
  implicit val state = QueryStateHelper.emptyWith(query = qtx)

  val basicTypes = Set(CTAny, CTBoolean, CTString, CTNumber, CTInteger, CTFloat, CTPoint)
  val graphTypes = Set(CTNode, CTRelationship, CTPath)

  val level0Types = basicTypes ++ graphTypes
  val level1Types = level0Types.map(CTList).toSet + CTMap
  val level2Types = (level1Types ++ level0Types).map(CTList)

  val testedTypes = level2Types ++ level1Types ++ level0Types

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

  test("NODE") {
    testedTypes
      .coerce(nodeValue(11L, stringArray("L"), EMPTY_MAP))
      .to(CTAny).unchanged
      .to(CTNode).unchanged
      // TODO: IsCollection/IsMap behaviour - Discuss
      .to(CTMap).changedButNotNull
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("RELATIONSHIP") {
    testedTypes
      .coerce(edgeValue(11L, nodeValue(11L, stringArray("L"), EMPTY_MAP), nodeValue(12L, stringArray("L"), EMPTY_MAP),
                        stringValue("T"), EMPTY_MAP))
      .to(CTAny).unchanged
      .to(CTRelationship).unchanged
      // TODO: IsCollection/IsMap behaviour - Discuss
      .to(CTMap).changedButNotNull
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
          CoerceTo(TestExpression(actualValue), typ)(ExecutionContext.empty, state)
        }

        remaining -= typ

        self
      }

      case class TestExpression(in: AnyValue) extends Expression {

        override def rewrite(f: (Expression) => Expression): Expression = this

        override def arguments: Seq[Expression] = Seq.empty

        override def symbolTableDependencies: Set[String] = Set.empty
        def apply(ctx: ExecutionContext, state: QueryState): AnyValue = in

      }

      case class to(typ: CypherType) {
        private val coercedValue = CoerceTo(TestExpression(actualValue), typ)(ExecutionContext.empty, state)

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
