package org.neo4j.cypher.internal.plan_description

import org.neo4j.cypher.internal.ir.v4_0.ProvidedOrder
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.logical.plans.{QualifiedName, SeekableArgs}
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType
import org.neo4j.cypher.internal.v4_0.{expressions => ast}

sealed abstract class Argument extends Product {

  def name = productPrefix
}

object Arguments {

  case class Time(value: Long) extends Argument

  case class Rows(value: Long) extends Argument

  case class DbHits(value: Long) extends Argument

  case class Order(order: ProvidedOrder) extends Argument

  case class PageCacheHits(value: Long) extends Argument

  case class PageCacheMisses(value: Long) extends Argument

  case class PageCacheHitRatio(value: Double) extends Argument

  case class ColumnsLeft(value: Seq[String]) extends Argument

  case class Expression(value: ast.Expression) extends Argument

  case class Expressions(expressions: Map[String, ast.Expression]) extends Argument

  case class UpdateActionName(value: String) extends Argument

  case class MergePattern(startPoint: String) extends Argument

  case class Index(label: String, propertyKeys: Seq[String]) extends Argument

  case class PrefixIndex(label: String, propertyKey: String, prefix: ast.Expression) extends Argument

  case class InequalityIndex(label: String, propertyKey: String, bounds: Seq[String]) extends Argument

  case class PointDistanceIndex(label: String, propertyKey: String, point: String, distance: String, inclusive: Boolean) extends Argument

  case class LabelName(label: String) extends Argument

  case class KeyNames(keys: Seq[String]) extends Argument

  case class KeyExpressions(expressions: Seq[Expression]) extends Argument

  case class EntityByIdRhs(value: SeekableArgs) extends Argument

  case class EstimatedRows(value: Double) extends Argument

  case class Signature(procedureName: QualifiedName,
                       args: Seq[ast.Expression],
                       results: Seq[(String, CypherType)]) extends Argument

  // This is the version of cypher and will equal the planner version
  // that is being used.
  case class Version(value: String) extends Argument {

    override def name = "version"
  }

  case class RuntimeVersion(value: String) extends Argument {

    override def name = "runtime-version"
  }

  case class Planner(value: String) extends Argument {

    override def name = "planner"
  }

  case class PlannerImpl(value: String) extends Argument {

    override def name = "planner-impl"
  }

  case class PlannerVersion(value: String) extends Argument {

    override def name = "planner-version"
  }

  case class Runtime(value: String) extends Argument {

    override def name = "runtime"
  }

  case class RuntimeImpl(value: String) extends Argument {

    override def name = "runtime-impl"
  }

  case class ExpandExpression(from: String, relName: String, relTypes: Seq[String], to: String,
                              direction: SemanticDirection, minLength: Int, maxLength: Option[Int]) extends Argument

  case class CountNodesExpression(ident: String, labels: List[Option[String]]) extends Argument

  case class CountRelationshipsExpression(ident: String, startLabel: Option[String],
                                          typeNames: Seq[String], endLabel: Option[String]) extends Argument

  case class SourceCode(className: String, sourceCode: String) extends Argument {

    override def name = "source:" + className
  }

  case class ByteCode(className: String, disassembly: String) extends Argument {

    override def name = "bytecode:" + className
  }
}
