/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable

sealed trait PrivilegeQualifier extends Rewritable {
  def simplify: Seq[PrivilegeQualifier] = Seq(this)

  override def dup(children: Seq[AnyRef]): PrivilegeQualifier.this.type = this
}

// Graph qualifiers

sealed trait GraphPrivilegeQualifier extends PrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): GraphPrivilegeQualifier.this.type = this
}

final case class LabelQualifier(label: String)(val position: InputPosition) extends GraphPrivilegeQualifier

final case class RelationshipQualifier(reltype: String)(val position: InputPosition) extends GraphPrivilegeQualifier

final case class ElementQualifier(value: String)(val position: InputPosition) extends GraphPrivilegeQualifier {
  override def simplify: Seq[GraphPrivilegeQualifier] = Seq(LabelQualifier(value)(position), RelationshipQualifier(value)(position))
}

final case class ElementsAllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier {
  override def simplify: Seq[PrivilegeQualifier] = Seq(LabelAllQualifier()(position), RelationshipAllQualifier()(position))
}

final case class AllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier

final case class LabelAllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier

final case class RelationshipAllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier

// Database qualifiers

sealed trait DatabasePrivilegeQualifier extends PrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): DatabasePrivilegeQualifier.this.type = this
}

final case class AllDatabasesQualifier()(val position: InputPosition) extends DatabasePrivilegeQualifier

final case class UserAllQualifier()(val position: InputPosition) extends DatabasePrivilegeQualifier

final case class UserQualifier(username: Either[String, Parameter])(val position: InputPosition) extends DatabasePrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): UserQualifier.this.type =
    this.copy(children.head.asInstanceOf[Either[String, Parameter]])(position).asInstanceOf[this.type]
}

// Dbms qualifiers

sealed trait ExecutePrivilegeQualifier extends PrivilegeQualifier

sealed trait ProcedurePrivilegeQualifier extends ExecutePrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): ProcedurePrivilegeQualifier.this.type = this
}

final case class ProcedureQualifier(glob: String)(val position: InputPosition) extends ProcedurePrivilegeQualifier {
  override def simplify: Seq[ProcedurePrivilegeQualifier] = glob match {
    case "*" => Seq(ProcedureAllQualifier()(position))
    case _ => Seq(this)
  }
}

final case class ProcedureAllQualifier()(val position: InputPosition) extends ProcedurePrivilegeQualifier

sealed trait FunctionPrivilegeQualifier extends ExecutePrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): FunctionPrivilegeQualifier.this.type = this
}

final case class FunctionQualifier(glob: String)(val position: InputPosition) extends FunctionPrivilegeQualifier {
  override def simplify: Seq[FunctionPrivilegeQualifier] = glob match {
    case "*" => Seq(FunctionAllQualifier()(position))
    case _ => Seq(this)
  }
}

final case class FunctionAllQualifier()(val position: InputPosition) extends FunctionPrivilegeQualifier
