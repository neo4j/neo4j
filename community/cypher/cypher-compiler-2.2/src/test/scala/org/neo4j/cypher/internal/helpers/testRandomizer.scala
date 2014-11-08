package org.neo4j.cypher.internal.helpers

import scala.util.Random

case object testRandomizer extends Random {
  // By randomizing, we will test more variants of the data. The formulas expressed should still give correct results
  // We print the seed used for the Random object so that test failures can easily be reproduced when encountered
  val seed = System.currentTimeMillis()
  setSeed(seed)
  println("seed: " + seed)
}
