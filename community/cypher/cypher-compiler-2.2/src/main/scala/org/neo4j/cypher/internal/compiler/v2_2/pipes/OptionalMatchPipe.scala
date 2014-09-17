package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.builders.{IfElseIterator, QueryStateSettingIterator}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{QueryState, PipeWithSource, PipeMonitor, Pipe}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.{TwoChildren, PlanDescriptionImpl, PlanDescription}
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable

/**
 * This pipe does optional matches by making sure that the match pipe either finds a match for a start context,
 * or an execution context where all the introduced identifiers are now bound to null.
 */
case class OptionalMatchPipe(source: Pipe,
                             matchPipe: Pipe,
                             symbols: SymbolTable)
                            (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val listeningIterator = new QueryStateSettingIterator(input, state)

    new IfElseIterator(input = listeningIterator,
      ifClause = doMatch(state),
      elseClause = createNulls,
      finallyClause = () => state.initialContext = None)
  }

  def planDescription: PlanDescription =
    PlanDescriptionImpl(this, "OptionalMatch", TwoChildren(source.planDescription, matchPipe.planDescription), Seq.empty)

  val identifiersBeforeMatch = matchPipe.symbols.identifiers.map(_._1).toSet
  val identifiersAfterMatch = source.symbols.identifiers.map(_._1).toSet
  val introducedIdentifiers = identifiersBeforeMatch -- identifiersAfterMatch
  val nulls: Map[String, Any] = introducedIdentifiers.map(_ -> null).toMap

  private def createNulls(in: ExecutionContext): Iterator[ExecutionContext] = {
    Iterator(in.newWith(nulls))
  }

  override def localEffects: Effects = Effects.NONE

  def doMatch(state: QueryState)(ctx: ExecutionContext) = matchPipe.createResults(state)

  def dup(sources: List[Pipe]): Pipe = {
    val (l :: r :: Nil) = sources
    copy(source = l, matchPipe = r)
  }

  override val sources: Seq[Pipe] = Seq(source, matchPipe)
}
