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
package org.neo4j.cypher.internal.options

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OptionDefaultTest extends CypherFunSuite {

  case class MyOuter(inner: MyInner, someInt: Int)
  case class MyInner(someString: String, anotherString: String)

  implicit val defaultInt: OptionDefault[Int] = OptionDefault.create(123)
  implicit val defaultString: OptionDefault[String] = OptionDefault.create("foo")
  implicit val defaultInner: OptionDefault[MyInner] = OptionDefault.derive[MyInner]
  implicit val defaultOuter: OptionDefault[MyOuter] = OptionDefault.derive[MyOuter]

  test("Can create default value for any case class") {
    defaultOuter.default shouldEqual MyOuter(MyInner("foo", "foo"), 123)
  }

}
