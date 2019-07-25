/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.InputPosition

/**
  * Helpers for creating ast nodes
  * Implicit conversions that keeps the test code small
  * Input position line numbers taken from scala source to aid debugging
  */
trait AstConstructionHelp extends AstConstructionTestSupport {

  def v(n: String)(implicit p: InputPosition): Variable =
    Variable(n)(p)

  def i(i: String)(implicit p: InputPosition): SignedDecimalIntegerLiteral =
    SignedDecimalIntegerLiteral(i)(p)

  def list(es: Expression*)(implicit p: InputPosition): ListLiteral =
    ListLiteral(es)(p)

  def prop(map: Expression, key: String)(implicit p: InputPosition): Property =
    Property(map, PropertyKeyName(key)(p))(p)

  def f(name: String, args: Expression*)(implicit p: InputPosition): FunctionInvocation =
    FunctionInvocation(FunctionName(name)(p), false, args.toIndexedSeq)(p)

  def f(name: Seq[String], args: Expression*)(implicit p: InputPosition): FunctionInvocation =
    FunctionInvocation(Namespace(name.init.toList)(p), FunctionName(name.last)(p), false, args.toIndexedSeq)(p)

  def query(cs: Clause*)(implicit p: InputPosition): Query =
    Query(None, SingleQuery(cs)(p))(p)

  def singleQuery(cs: Clause*)(implicit p: InputPosition): SingleQuery =
    SingleQuery(cs)(p)

  def subQuery(cs: Clause*)(implicit p: InputPosition): SubQuery =
    SubQuery(SingleQuery(cs)(p))(p)

  def create(pattern: PatternElement, where: Option[Where] = None)(implicit p: InputPosition): Create =
    Create(Pattern(Seq(EveryPath(pattern)))(p))(p)

  def match_(pattern: PatternElement, where: Option[Where] = None)(implicit p: InputPosition): Match =
    Match(false, Pattern(Seq(EveryPath(pattern)))(p), Seq(), where)(p)

  def node(name: String)(implicit p: InputPosition): NodePattern =
    NodePattern(Some(Variable(name)(p)), Seq(), None)(p)

  def node(name: String, labels: String*)(implicit p: InputPosition): NodePattern =
    NodePattern(Some(Variable(name)(p)), labels.map(LabelName(_)(p)), None)(p)

  def with_(items: ReturnItem*)(implicit p: InputPosition): With =
    With(ReturnItems(false, items)(p))(p)

  def return_(items: ReturnItem*)(implicit p: InputPosition): Return =
    Return(ReturnItems(false, items)(p))(p)

  def return_(ob: OrderBy, items: ReturnItem*)(implicit p: InputPosition): Return =
    Return(false, ReturnItems(false, items)(p), Some(ob), None, None)(p)

  def orderBy(items: SortItem*)(implicit p: InputPosition): OrderBy =
    OrderBy(items)(p)

  def sortItem(e: Expression)(implicit p: InputPosition): AscSortItem =
    AscSortItem(e)(p)

  def unwind(e: Expression, v: Variable)(implicit p: InputPosition): Unwind =
    Unwind(e, v)(p)

  def call(ns: Seq[String], name: String)(implicit p: InputPosition): UnresolvedCall =
    UnresolvedCall(Namespace(ns.toList)(p),
      ProcedureName(name)(p),
      Some(Vector()), None
    )(p)

  implicit def returnItemFromVariable(v: LogicalVariable)(implicit p: InputPosition): AliasedReturnItem =
    AliasedReturnItem(v, v)(p)

  implicit def returnItemFromInt(i: DecimalIntegerLiteral)(implicit p: InputPosition): UnaliasedReturnItem =
    UnaliasedReturnItem(i, i.stringVal)(p)

  implicit def returnItemFromTuple(ev: (Expression, LogicalVariable))(implicit p: InputPosition): AliasedReturnItem =
    AliasedReturnItem(ev._1, ev._2)(p)

  implicit def sortItemFromExpression(e: Expression)(implicit p: InputPosition): AscSortItem =
    AscSortItem(e)(p)

  implicit def scalaPos(implicit l: sourcecode.Line): InputPosition =
    InputPosition(0, l.value, 0)
}
