package org.neo4j.cypher.internal.compiler.v2_2.perty.ops

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.perty.{DocGen, DocOps, BaseDocOps, Extractor}

class expandDocOpsTest extends CypherFunSuite {

  import DocOps._
  import Extractor._

  test("passes through plain doc ops") {
    import DocOps._

    val doc = DocOps("x" :/: "y")
    val result = expandDocOps(Extractor.empty).apply(doc)

    result should equal(doc)
  }

  test("replaces content in doc ops") {
    val doc = DocOps(content(1) :/: "y")
    val result = expandDocOps[Any](pick {
      case (a: Int) => DocOps("1")
    }).apply(doc)

    result should equal(DocOps("1" :/: "y"))
  }
}
