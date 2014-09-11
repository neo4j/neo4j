package org.neo4j.cypher.internal.compiler.v2_2.perty.bling

import scala.reflect.runtime.universe._

abstract class DrillHandler[O : TypeTag] {
  def mapExtractor(extractor: Extractor[Any, O]): Extractor[Any, O]

  def recover[I : TypeTag](original: Drill[I, O]): Drill[I, O] =
    (extractor: Extractor[Any, O]) => original(mapExtractor(extractor))
}

object DrillHandler {
  def identity[O : TypeTag] = new DrillHandler[O] {
    def mapExtractor(extractor: Extractor[Any, O]) = extractor
  }
}
