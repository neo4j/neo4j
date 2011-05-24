package org.neo4j.lab.cypher

import commands._
import org.junit.Test
import org.junit.Assert._

/**
 * Created by Andres Taylor
 * Date: 5/24/11
 * Time: 15:53 
 */

class FilterTests {
  @Test def andsDoesntHaveOrs() {
    val x = And(
      StringEquals("a", "b", "c"),
      StringEquals("a", "b", "c"))
    assertFalse("Should not claim it has any ors", x.hasOrs)
  }

  @Test def nestedStuffHasOrs() {
    val x =
      And(
        StringEquals("a", "b", "c"),
        Or(
          StringEquals("a", "b", "c"),
          StringEquals("a", "b", "c")))

    assertTrue("Does have an or, but didn't say it.", x.hasOrs)
  }
}