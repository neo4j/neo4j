package org.neo4j.cypher.internal.pipes

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.commands.values.UnboundValue
import org.neo4j.cypher.internal.ExecutionContext

class OptionalsBindingPipeTest extends Assertions {

  @Test
  def should_map_all_optionals_to_null_but_nothing_else() {
    // given
    val source = new FakePipe(List(Map("a" -> UnboundValue, "b" -> 12, "c" -> null)))
    val pipe = new OptionalsBindingPipe(source)

    // when
    val result = pipe.createResults(QueryStateHelper.empty).toSet

    // then
    assert( Set(ExecutionContext.from("a" -> null, "b" -> 12, "c" -> null)) === result )
  }
}