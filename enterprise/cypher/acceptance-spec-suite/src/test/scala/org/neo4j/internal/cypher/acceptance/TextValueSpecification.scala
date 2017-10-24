/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import java.lang.Integer.signum
import java.nio.charset.StandardCharsets

import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values.{stringValue, utf8Value}
import org.scalacheck.{Properties, _}

object TextValueSpecification extends Properties("TextValue") {

  import Prop.forAll

  private val substringGen = for {
    string <- Arbitrary.arbitrary[String]
    max = string.codePointCount(0, string.length)
    start <- Gen.chooseNum[Int](0, max)
    end <-  Gen.chooseNum[Int](start + 1, max)
  } yield (string, start, end)

  property("equals") = forAll { (x: String) =>
    stringValue(x).equals(utf8Value(x.getBytes(StandardCharsets.UTF_8)))
  }

  property("length") = forAll { (x: String) =>
    stringValue(x).length() == utf8Value(x.getBytes(StandardCharsets.UTF_8)).length()
  }

  property("hashCode") = forAll { (x: String) =>
    stringValue(x).hashCode() == utf8Value(x.getBytes(StandardCharsets.UTF_8)).hashCode()
  }

  property("trim") = forAll { (x: String) =>
    equivalent(stringValue(x).trim(), utf8Value(x.getBytes(StandardCharsets.UTF_8)).trim())
  }

  property("trim") = forAll { (x: String) =>
    equivalent(stringValue(x), utf8Value(x.getBytes(StandardCharsets.UTF_8)).trim())
  }

  property("compareTo") = forAll { (x: String, y: String) =>
    val stringX = stringValue(x)
    val stringY = stringValue(y)
    val utf8X = utf8Value(x.getBytes(StandardCharsets.UTF_8))
    val utf8Y = utf8Value(y.getBytes(StandardCharsets.UTF_8))
    val compare = signum(stringX.compareTo(stringY))
    compare == signum(stringX.compareTo(utf8Y)) &&
      compare == signum(utf8X.compareTo(stringY)) &&
      compare == signum(utf8X.compareTo(utf8Y))
  }

  property("compareTo") = forAll { (x: String) =>
    val stringX = stringValue(x)
    val utf8X = utf8Value(x.getBytes(StandardCharsets.UTF_8))
    stringX.compareTo(stringX) == 0 &&
    stringX.compareTo(utf8X) == 0 &&
    utf8X.compareTo(stringX) == 0 &&
    utf8X.compareTo(utf8X) == 0
  }

  property("substring") = forAll(substringGen) {
    case (string, start, end) =>
      equivalent(stringValue(string).substring(start, end),
                utf8Value(string.getBytes(StandardCharsets.UTF_8)).substring(start, end))
  }

  private def equivalent(t1: TextValue, t2: TextValue) =
    t1.length() == t2.length() && t1 == t2 && t1.hashCode() == t2.hashCode()
}
