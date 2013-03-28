package org.neo4j.cypher.internal.executionplan.verifiers

import org.neo4j.cypher.internal.commands.Query
import org.neo4j.cypher.PatternException
import org.neo4j.cypher.internal.commands.ShortestPath

object ShortestPathWithoutStartVerifier extends Verifier {
  def verifyFunction = {
    case Query(_, start, _, patterns, _, _, _, _, _, _, _, _)
      if start.isEmpty && patterns.exists(_.isInstanceOf[ShortestPath])=>
        throw new PatternException("Can't use shortest path without explicit START clause.")
  }

}