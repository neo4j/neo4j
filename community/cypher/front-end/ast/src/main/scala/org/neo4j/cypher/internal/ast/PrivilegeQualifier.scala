/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable

sealed trait PrivilegeQualifier extends Rewritable {
  def simplify: Seq[PrivilegeQualifier] = Seq(this)
}

// Graph qualifiers

sealed trait GraphPrivilegeQualifier extends PrivilegeQualifier

final case class LabelQualifier(label: String)(val position: InputPosition) extends GraphPrivilegeQualifier {

  override def dup(children: Seq[AnyRef]): LabelQualifier.this.type = {
    LabelQualifier(
      children.head.asInstanceOf[String]
    )(position).asInstanceOf[this.type]
  }
}

final case class LabelAllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): LabelAllQualifier.this.type = this
}

final case class RelationshipQualifier(reltype: String)(val position: InputPosition) extends GraphPrivilegeQualifier {

  override def dup(children: Seq[AnyRef]): RelationshipQualifier.this.type = {
    RelationshipQualifier(
      children.head.asInstanceOf[String]
    )(position).asInstanceOf[this.type]
  }
}

final case class RelationshipAllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): RelationshipAllQualifier.this.type = this
}

final case class ElementQualifier(value: String)(val position: InputPosition) extends GraphPrivilegeQualifier {

  override def dup(children: Seq[AnyRef]): ElementQualifier.this.type = {
    ElementQualifier(
      children.head.asInstanceOf[String]
    )(position).asInstanceOf[this.type]
  }

  override def simplify: Seq[GraphPrivilegeQualifier] =
    Seq(LabelQualifier(value)(position), RelationshipQualifier(value)(position))
}

final case class ElementsAllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): ElementsAllQualifier.this.type = this

  override def simplify: Seq[PrivilegeQualifier] =
    Seq(LabelAllQualifier()(position), RelationshipAllQualifier()(position))
}

final case class PatternQualifier(
  labelQualifiers: Seq[PrivilegeQualifier],
  variable: Option[Variable],
  expression: Expression
) extends GraphPrivilegeQualifier {

  override def dup(children: Seq[AnyRef]): PatternQualifier.this.type = {
    PatternQualifier(
      children.head.asInstanceOf[Seq[PrivilegeQualifier]],
      children(1).asInstanceOf[Option[Variable]],
      children(2).asInstanceOf[Expression]
    ).asInstanceOf[this.type]
  }
}

final case class AllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): AllQualifier.this.type = this
}

// Database qualifiers

sealed trait DatabasePrivilegeQualifier extends PrivilegeQualifier

final case class AllDatabasesQualifier()(val position: InputPosition) extends DatabasePrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): AllDatabasesQualifier.this.type = this
}

final case class UserAllQualifier()(val position: InputPosition) extends DatabasePrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): UserAllQualifier.this.type = this
}

final case class UserQualifier(username: Expression)(val position: InputPosition)
    extends DatabasePrivilegeQualifier {

  override def dup(children: Seq[AnyRef]): UserQualifier.this.type =
    this.copy(children.head.asInstanceOf[Expression])(position).asInstanceOf[this.type]
}

// Dbms qualifiers

sealed trait ExecutePrivilegeQualifier extends PrivilegeQualifier

sealed trait ProcedurePrivilegeQualifier extends ExecutePrivilegeQualifier

final case class ProcedureQualifier(glob: String)(val position: InputPosition) extends ProcedurePrivilegeQualifier {

  override def dup(children: Seq[AnyRef]): ProcedureQualifier.this.type = {
    ProcedureQualifier(
      children.head.asInstanceOf[String]
    )(position).asInstanceOf[this.type]
  }

  override def simplify: Seq[ProcedurePrivilegeQualifier] = glob match {
    case "*" => Seq(ProcedureAllQualifier()(position))
    case _   => Seq(this)
  }
}

final case class ProcedureAllQualifier()(val position: InputPosition) extends ProcedurePrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): ProcedureAllQualifier.this.type = this
}

sealed trait FunctionPrivilegeQualifier extends ExecutePrivilegeQualifier

final case class FunctionQualifier(glob: String)(val position: InputPosition) extends FunctionPrivilegeQualifier {

  override def dup(children: Seq[AnyRef]): FunctionQualifier.this.type = {
    FunctionQualifier(
      children.head.asInstanceOf[String]
    )(position).asInstanceOf[this.type]
  }

  override def simplify: Seq[FunctionPrivilegeQualifier] = glob match {
    case "*" => Seq(FunctionAllQualifier()(position))
    case _   => Seq(this)
  }
}

final case class FunctionAllQualifier()(val position: InputPosition) extends FunctionPrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): FunctionAllQualifier.this.type = this
}

sealed trait SettingPrivilegeQualifier extends PrivilegeQualifier

final case class SettingQualifier(glob: String)(val position: InputPosition)
    extends SettingPrivilegeQualifier {

  override def dup(children: Seq[AnyRef]): SettingQualifier.this.type = {
    SettingQualifier(
      children.head.asInstanceOf[String]
    )(position).asInstanceOf[this.type]
  }

  override def simplify: Seq[SettingPrivilegeQualifier] = glob match {
    case "*" => Seq(SettingAllQualifier()(position))
    case _   => Seq(this)
  }
}

final case class SettingAllQualifier()(val position: InputPosition) extends SettingPrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): SettingAllQualifier.this.type = this
}

// Load qualifiers

sealed trait LoadPrivilegeQualifier extends PrivilegeQualifier

final case class LoadAllQualifier()(val position: InputPosition) extends LoadPrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): LoadAllQualifier.this.type = this
}

final case class LoadCidrQualifier(cidr: Either[String, Parameter])(val position: InputPosition)
    extends LoadPrivilegeQualifier {

  override def dup(children: Seq[AnyRef]): LoadCidrQualifier.this.type = {
    LoadCidrQualifier(
      children.head.asInstanceOf[Either[String, Parameter]]
    )(position).asInstanceOf[this.type]
  }
}

final case class LoadUrlQualifier(url: Either[String, Parameter])(val position: InputPosition)
    extends LoadPrivilegeQualifier {

  override def dup(children: Seq[AnyRef]): LoadUrlQualifier.this.type = {
    LoadUrlQualifier(
      children.head.asInstanceOf[Either[String, Parameter]]
    )(position).asInstanceOf[this.type]
  }
}
