package org.neo4j.cypher.internal.compatibility.v3_1

import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows}
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.{Argument, InternalPlanDescription, PlanDescriptionArgumentSerializer}
import org.neo4j.cypher.internal.compiler.v3_1.{PlannerName, RuntimeName}
import org.neo4j.cypher.internal.javacompat.{PlanDescription, ProfilerStatistics}
import org.neo4j.cypher.{CypherVersion, InternalException}

import scala.collection.JavaConverters._

case class CompatibilityPlanDescription(inner: InternalPlanDescription, version: CypherVersion,
                                        planner: PlannerName, runtime: RuntimeName)
  extends org.neo4j.cypher.internal.PlanDescription {

  self =>

  def children = exceptionHandler.runSafely {
    inner.children.toIndexedSeq.map(CompatibilityPlanDescription.apply(_, version, planner, runtime))
  }

  def arguments: Map[String, AnyRef] = exceptionHandler.runSafely {
    inner.arguments.map { arg => arg.name -> PlanDescriptionArgumentSerializer.serialize(arg) }.toMap
  }

  def identifiers = exceptionHandler.runSafely {
    inner.orderedVariables.toSet
  }

  override def hasProfilerStatistics = exceptionHandler.runSafely {
    inner.arguments.exists(_.isInstanceOf[DbHits])
  }

  def name = exceptionHandler.runSafely {
    inner.name
  }

  def asJava: PlanDescription = exceptionHandler.runSafely {
    asJava(self)
  }

  override def toString: String = {
    val NL = System.lineSeparator()
    exceptionHandler.runSafely {
      s"Compiler CYPHER ${version.name}$NL${NL}Planner ${planner.toTextOutput.toUpperCase}$NL${NL}Runtime ${runtime.toTextOutput.toUpperCase}$NL$NL$inner"
    }
  }

  def asJava(in: org.neo4j.cypher.internal.PlanDescription): PlanDescription = new PlanDescription {
    def getProfilerStatistics: ProfilerStatistics = new ProfilerStatistics {
      def getDbHits: Long = extract { case DbHits(count) => count }

      def getRows: Long = extract { case Rows(count) => count }

      private def extract(f: PartialFunction[Argument, Long]): Long =
        inner.arguments.collectFirst(f).getOrElse(throw new InternalException("Don't have profiler stats"))
    }

    def getName: String = name

    def hasProfilerStatistics: Boolean = self.hasProfilerStatistics

    def getArguments: java.util.Map[String, AnyRef] = arguments.asJava

    def getIdentifiers: java.util.Set[String] = identifiers.asJava

    def getChildren: java.util.List[PlanDescription] = in.children.toList.map(_.asJava).asJava

    override def toString: String = self.toString
  }
}
