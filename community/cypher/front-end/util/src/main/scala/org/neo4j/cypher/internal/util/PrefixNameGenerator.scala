/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.nameGeneratorRegex

object FreshIdNameGenerator extends PrefixNameGeneratorCompanion("FRESHID")
class FreshIdNameGenerator() extends PrefixNameGenerator(FreshIdNameGenerator.prefix)

object AggregationNameGenerator extends PrefixNameGeneratorCompanion("AGGREGATION")
class AggregationNameGenerator() extends PrefixNameGenerator(AggregationNameGenerator.prefix)

object NodeNameGenerator extends PrefixNameGeneratorCompanion("NODE")
class NodeNameGenerator() extends PrefixNameGenerator(NodeNameGenerator.prefix)

object RelNameGenerator extends PrefixNameGeneratorCompanion("REL")
class RelNameGenerator() extends PrefixNameGenerator(RelNameGenerator.prefix)

object PathNameGenerator extends PrefixNameGeneratorCompanion("PATH")
class PathNameGenerator() extends PrefixNameGenerator(PathNameGenerator.prefix)

object RollupCollectionNameGenerator extends PrefixNameGeneratorCompanion("ROLLUP")
class RollupCollectionNameGenerator() extends PrefixNameGenerator(RollupCollectionNameGenerator.prefix)

object UnNamedNameGenerator extends PrefixNameGeneratorCompanion("UNNAMED"){
    implicit class NameString(name: String) {
      def isNamed: Boolean = UnNamedNameGenerator.isNamed(name)
      def unnamed: Boolean = UnNamedNameGenerator.notNamed(name)
    }
}
class UnNamedNameGenerator() extends PrefixNameGenerator(UnNamedNameGenerator.prefix)

object AllNameGenerators {
  val generators = Seq(
    FreshIdNameGenerator,
    AggregationNameGenerator,
    NodeNameGenerator,
    RelNameGenerator,
    PathNameGenerator,
    RollupCollectionNameGenerator,
    UnNamedNameGenerator,
  )

  def isNamed(x: String): Boolean = {
    generators.forall(_.isNamed(x))
  }
}

case class PrefixNameGeneratorCompanion(generatorName: String) {
  val prefix =  s"  $generatorName"

  def isNamed(x: String): Boolean = !notNamed(x)
  def notNamed(x: String): Boolean = x.startsWith(prefix)

  def unapply(v: Any): Option[String] = v match {
    case str: String =>
      val regex = nameGeneratorRegex(generatorName)
      regex.findPrefixMatchOf(str).map(_ group 2)
    case _ => None
  }
}

object PrefixNameGenerator {
  def namePrefix(prefix: String) = s"  $prefix"
}

class PrefixNameGenerator(prefix: String) {
  private var counter = 0

  def nextName: String = {
    val result = s"$prefix$counter"
    counter += 1
    result
  }
}

class AllNameGenerators() {
  val freshIdNameGenerator = new FreshIdNameGenerator()
  val aggregationNameGenerator = new AggregationNameGenerator()
  val nodeNameGenerator = new NodeNameGenerator()
  val relNameGenerator = new RelNameGenerator()
  val pathNameGenerator = new PathNameGenerator()
  val rollupCollectionNameGenerator = new RollupCollectionNameGenerator()
  val unNamedNameGenerator = new UnNamedNameGenerator()
}
