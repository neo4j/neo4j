package org.neo4j.cypher.internal.pipes

import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.graphdb.Path
import org.neo4j.cypher.internal.commands.values.LabelValue
import org.neo4j.cypher.internal.commands.values.LabelName

class ResultValueMapperPipe(source: Pipe) extends PipeWithSource(source) {

  def throwIfSymbolsMissing(symbols: SymbolTable) {}

  def createResults(state: QueryState): Iterator[ExecutionContext] = {
    def mapValue(in: Any): Any = in match {
      case p: Path => p
      case l: LabelName => l.name
      case l: LabelValue => l.resolve(state.queryContext).name
      case i: Traversable[_] => i.map(mapValue)
      case x => x
    }

    val result: Iterator[ExecutionContext] = source.createResults(state).map {
      (ctx: ExecutionContext) =>
        val newMap = ctx.transform {
          (_, v: Any) => mapValue(v)
        }
        newMap.asInstanceOf[ExecutionContext] // Jetbraaaaiiinz
    }

    result
  }

  def executionPlanDescription() = source.executionPlanDescription() + "\nResultValueMapping()"

  def symbols:SymbolTable = new SymbolTable(source.symbols.identifiers.mapValues { (t: CypherType) =>
    t.rewrite {
      case _: LabelType => StringType()
      case x => x
    }
  })
}