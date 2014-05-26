/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pprint

import org.neo4j.cypher.internal.compiler.v2_1.pprint.docbuilders.defaultDocBuilder

trait Pretty {
  def toDoc: Doc

  def format(docFormatter: DocFormatter)
            (implicit docGenerator: DocGenerator[this.type]) =
    pformat(this, docFormatter)
}

trait HasFormatter {
  def docFormatter: DocFormatter = DocFormatters.defaultFormatter
}

trait HasLineFormatter extends HasFormatter {
  override def docFormatter: DocFormatter = DocFormatters.defaultLineFormatter
}

trait HasPageFormatter extends HasFormatter {
  override def docFormatter: DocFormatter = DocFormatters.defaultPageFormatter
}

trait PlainlyPretty extends Pretty with HasFormatter {
  override def toString = printToString(docFormatter(toDoc))

  override def format(docFormatter: DocFormatter = docFormatter)
                     (implicit docGenerator: DocGenerator[this.type]) =
    super.format(docFormatter)
}

trait GeneratedPretty extends Pretty with HasFormatter {
  def toDoc = docGenerator(this)

  override def format(docFormatter: DocFormatter = docFormatter)
                     (implicit docGenerator: DocGenerator[this.type] = docGenerator) =
    super.format(docFormatter)

  def docGenerator: DocGenerator[this.type] = defaultDocBuilder.docGenerator
}
