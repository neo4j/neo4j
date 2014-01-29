package org.neo4j.cypher

import org.junit.Test

class NullAcceptanceTest extends ExecutionEngineHelper {

  @Test def null_nodes_should_be_silently_ignored() {
    // Given empty database

    // When
    val result = execute("optional match (a:DoesNotExist) set a.prop = 42 return a")

    // Then doesn't throw
    result.toList
  }

}