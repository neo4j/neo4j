package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.InterpretedSideEffect
import org.neo4j.cypher.internal.util.attribution.Id

class MergePipe(src: Pipe,
                createOps: Seq[InterpretedSideEffect],
                onMatchSetOps: Seq[SetOperation],
                onCreateSetOps: Seq[SetOperation])(val id: Id = Id.INVALID_ID) extends PipeWithSource(src) {
  override protected def internalCreateResults(input: ClosingIterator[CypherRow],
                                               state: QueryState): ClosingIterator[CypherRow] = {
    if (input.hasNext) {
      input.map(r => {
        onMatchSetOps.foreach(op => op.set(r, state))
        r
      })
    } else {
      val row = state.newRowWithArgument(rowFactory)
      createOps.foreach(op => op.execute(row, state))
      onCreateSetOps.foreach(op => op.set(row, state))
      ClosingIterator.single(row)
    }
  }
}
