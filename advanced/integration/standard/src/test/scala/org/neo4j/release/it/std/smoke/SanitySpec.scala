package org.neo4j.release.it.std.smoke

import org.specs._

/**
 * A sanity check that scala specs can
 * be resolved and runs normally.
 */
class SanitySpec extends SpecificationWithJUnit {

  "scala specs" should {
    "sanely confirm truth" in {
      true must beTrue
    }
  }
}