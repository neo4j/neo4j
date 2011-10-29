/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.commands

import org.neo4j.graphdb.PropertyContainer

abstract class Clause {
  def ++(other: Clause): Clause = And(this, other)


  def isMatch(m: Map[ String, Any ]): Boolean


  // We need this information to place the filtering as close to the
  // source pipes as possible. The earlier we can filter stuff out,
  // the faster we can be
  def dependsOn: Set[ String ]


  // This is the un-dividable list of clauses. They can all be ANDed
  // together
  def atoms: Seq[ Clause ]
}

case class And(a: Clause, b: Clause) extends Clause {
  def isMatch(m: Map[ String, Any ]): Boolean = a.isMatch(m) && b.isMatch(m)

  def atoms: Seq[ Clause ] = a.atoms ++ b.atoms

  def dependsOn: Set[ String ] = a.dependsOn ++ b.dependsOn
}

case class Or(a: Clause, b: Clause) extends Clause {
  def isMatch(m: Map[ String, Any ]): Boolean = a.isMatch(m) || b.isMatch(m)

  def atoms: Seq[ Clause ] = Seq(this)

  def dependsOn: Set[ String ] = a.dependsOn ++ b.dependsOn
}

case class Not(a: Clause) extends Clause {
  def isMatch(m: Map[ String, Any ]): Boolean = !a.isMatch(m)

  def atoms: Seq[ Clause ] = a.atoms.map(Not(_))

  def dependsOn: Set[ String ] = a.dependsOn
}

case class IsNull(value: Value) extends Clause {
  def isMatch(m: Map[ String, Any ]): Boolean = value(m) == null

  def dependsOn: Set[ String ] = value match {
    case x: EntityValue => Set("I depened on something that should never exist as an identifier!")
    case x              => x.dependsOn
  }

  def atoms: Seq[ Clause ] = Seq(this)
}

case class True() extends Clause {
  def isMatch(m: Map[ String, Any ]): Boolean = true

  def dependsOn: Set[ String ] = Set()

  def atoms: Seq[ Clause ] = Seq(this)
}

case class Has(property: PropertyValue) extends Clause {
  def isMatch(m: Map[ String, Any ]): Boolean = property match {
    case PropertyValue(identifier, propertyName) => {
      val propContainer = m(identifier).asInstanceOf[ PropertyContainer ]
      propContainer.hasProperty(propertyName)
    }
  }

  def dependsOn: Set[ String ] = Set(property.entity)

  def atoms: Seq[ Clause ] = Seq(this)
}

case class RegularExpression(a: Value, regex: Value) extends Clause {
  def isMatch(m: Map[ String, Any ]): Boolean = {
    val value = a.apply(m).asInstanceOf[ String ]
    regex(m).toString.r.pattern.matcher(value).matches()
  }

  def dependsOn: Set[ String ] = a.dependsOn

  def atoms: Seq[ Clause ] = Seq(this)
}
