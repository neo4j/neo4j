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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import java.util.{ArrayList => JavaList, HashMap => JavaMap}

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.Counter
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.graphdb.spatial.{Geometry, Point}
import org.neo4j.graphdb.{Node, Relationship}

import scala.language.postfixOps

class CoerceToTest extends CypherFunSuite {

  implicit var openCases: Counter = Counter()
  implicit val qtx = mock[QueryContext]
  implicit val state = QueryStateHelper.emptyWith(qtx)

  val basicTypes = Set(CTAny, CTBoolean, CTString, CTNumber, CTInteger, CTFloat, CTPoint)
  val graphTypes = Set(CTNode, CTRelationship, CTPath)

  val level0Types = basicTypes ++ graphTypes
  val level1Types = level0Types.map(CTList).toSet + CTMap
  val level2Types = (level1Types ++ level0Types).map(CTList)

  val testedTypes = level2Types ++ level1Types ++ level0Types

  test("null") {
    testedTypes
      .coerce(null)
      .forRemainingTypes { typ => _.to(typ) unchanged }
  }

  test("POINT") {
    testedTypes
      .coerce(mock[Point])
      .to(CTAny).unchanged
      .to(CTPoint).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("GEOMETRY") {
    testedTypes
      .coerce(mock[Geometry])
      .to(CTAny).unchanged
      .to(CTGeometry).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("BOOLEAN") {
    testedTypes
      .coerce(true)
      .to(CTAny).unchanged
      .to(CTBoolean).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }

    testedTypes
      .coerce(true)
      .to(CTAny).unchanged
      .to(CTBoolean).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("STRING") {
    testedTypes
      .coerce("")
      .to(CTAny).unchanged
      .to(CTString).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }

    testedTypes
      .coerce("Hello")
      .to(CTAny).unchanged
      .to(CTString).unchanged
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("NODE") {
    testedTypes
      .coerce(mock[Node])
      .to(CTAny).unchanged
      .to(CTNode).unchanged
      // TODO: IsCollection/IsMap behaviour - Discuss
      .to(CTMap).changedButNotNull
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("RELATIONSHIP") {
    testedTypes
      .coerce(mock[Relationship])
      .to(CTAny).unchanged
      .to(CTRelationship).unchanged
      // TODO: IsCollection/IsMap behaviour - Discuss
      .to(CTMap).changedButNotNull
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("INTEGER") {
    testedTypes
      .coerce(1L)
      .to(CTAny).unchanged
      .to(CTNumber).unchanged
      .to(CTInteger).unchanged
      .to(CTFloat).changedTo(1.0d)
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("FLOAT") {
    testedTypes
      .coerce(4.2d)
      .to(CTAny).unchanged
      .to(CTNumber).unchanged
      .to(CTInteger).changedTo(4L)
      .to(CTFloat).changedButNotNull
      .forRemainingTypes { typ => _.notTo(typ) }
  }

  test("MAP") {
    Seq(
      new JavaMap[String, Any](),
      Map.empty[String, Any]
    ).foreach { value =>
      testedTypes
        .coerce(value)
        .to(CTAny).unchanged
        .to(CTMap).changedTo(Map.empty)
        // TODO: IsCollection/IsMap behaviour - Discuss
        .to(CTList(CTAny)).changedTo(List(Map.empty))
        .to(CTList(CTMap)).changedTo(List(Map.empty))
        .to(CTList(CTList(CTAny))).changedTo(List(List(Map.empty)))
        .to(CTList(CTList(CTMap))).changedTo(List(List(Map.empty)))
        .forRemainingTypes { typ => _.notTo(typ) }
    }
  }

  test("LIST") {
    Seq(
      new JavaList[Any](),
      List.empty[Any]
    ).foreach { value =>
      testedTypes
        .coerce(value)
        .to(CTAny).unchanged
        .to(CTList(CTAny)).changedTo(List.empty)
        .to(CTList(CTList(CTString))).changedTo(List.empty)
        .to(CTList(CTString)).changedTo(List.empty)
        .to(CTList(CTList(CTBoolean))).changedTo(List.empty)
        .to(CTList(CTBoolean)).changedTo(List.empty)
        .to(CTList(CTList(CTInteger))).changedTo(List.empty)
        .to(CTList(CTInteger)).changedTo(List.empty)
        .to(CTList(CTList(CTFloat))).changedTo(List.empty)
        .to(CTList(CTFloat)).changedTo(List.empty)
        .to(CTList(CTList(CTNumber))).changedTo(List.empty)
        .to(CTList(CTNumber)).changedTo(List.empty)
        .to(CTList(CTList(CTPoint))).changedTo(List.empty)
        .to(CTList(CTPoint)).changedTo(List.empty)
        .to(CTList(CTList(CTGeometry))).changedTo(List.empty)
        .to(CTList(CTGeometry)).changedTo(List.empty)
        .to(CTList(CTList(CTNode))).changedTo(List.empty)
        .to(CTList(CTNode)).changedTo(List.empty)
        .to(CTList(CTList(CTRelationship))).changedTo(List.empty)
        .to(CTList(CTRelationship)).changedTo(List.empty)
        .to(CTList(CTList(CTPath))).changedTo(List.empty)
        .to(CTList(CTPath)).changedTo(List.empty)
        .to(CTList(CTList(CTMap))).changedTo(List.empty)
        .to(CTList(CTMap)).changedTo(List.empty)
        .to(CTList(CTList(CTAny))).changedTo(List.empty)
        .to(CTList(CTAny)).changedTo(List.empty)
        .forRemainingTypes { typ => _.notTo(typ) }
    }
  }

  test("Nested LISTs with mixed type values") {
    testedTypes.coerce(List(1.1, 2.0)).to(CTList(CTInteger)).changedTo(List(1, 2))
    testedTypes.coerce(List(1, 2)).to(CTList(CTFloat)).changedTo(List(1.0, 2.0))
    testedTypes.coerce(List(3.0)).to(CTList(CTInteger)).changedTo(List(3))
    testedTypes.coerce(List(3.0)).to(CTList(CTFloat)).changedTo(List(3.0))
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

    case class coerce(actualValue: Any)(implicit counter: Counter) {
      self =>

      private var remaining: Set[CypherType] = allTypes

      def notTo(typ: CypherType) = {
        a[CypherTypeException] should be thrownBy {
          CoerceTo(Literal(actualValue), typ)(ExecutionContext.empty)
        }

        remaining -= typ

        self
      }

      case class to(typ: CypherType) {
        private val coercedValue = CoerceTo(Literal(actualValue), typ)(ExecutionContext.empty)

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
