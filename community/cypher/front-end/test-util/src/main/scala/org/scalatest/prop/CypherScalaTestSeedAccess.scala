/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scalatest.prop

/**
 * A workaround to access private [[org.scalatest.prop.Seed.configuredRef]].
 * It is used by [[org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks]] to generate initial random seed for property tests.
 * Being able to set it allows us to reproduce test failures.
 */
object CypherScalaTestSeedAccess {

  def setSeed(s: Long): Unit = {
    Seed.configuredRef.set(Some(s))
  }
}
