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
package org.neo4j.cypher.internal.compiler.v2_0

object Rewritable {
  import Foldable._

  implicit class DuplicatableAny(val any: Any) extends AnyVal {
    def dup(rewriter: Any => Any): Any = any match {
      case p: Product =>
        val terms = p.children
        val rewrittenTerms = terms.map(rewriter)
        if (terms == rewrittenTerms)
          p
        else
          p.dup(rewrittenTerms)
      case s: IndexedSeq[_] =>
        s.map(rewriter)
      case s: Seq[_] =>
        s.map(rewriter)
      case t =>
        t
    }
  }

  implicit class DuplicatableProduct(val product: Product) extends AnyVal {
    def dup(children: IndexedSeq[Any]): Product = product match {
      case a: Rewritable =>
        a.dup(children)
      case _ =>
        val constructor = product.getClass.getMethods.find(_.getName == "copy").get
        constructor.invoke(product, children.map(_.asInstanceOf[AnyRef]): _*).asInstanceOf[Product]
    }
  }

  implicit class RewritableAny(val any: Any) extends AnyVal {
    def rewriteTopDown(fs: PartialFunction[Any, Any]*): Any =
      fs.indexWhere(_.isDefinedAt(any)) match {
        case -1 => any.dup(_.rewriteTopDown(fs:_*))
        case idx => fs.drop(idx+1).foldLeft(fs(idx).apply(any)) {
          (t, f) => if (f.isDefinedAt(t))
            f.apply(t)
          else
            t
        }
      }

    def rewriteBottomUp(fs: PartialFunction[Any, Any]*): Any = {
      val term = any.dup(_.rewriteBottomUp(fs:_*))
      fs.foldLeft(term) {
        (t, f) => if (f.isDefinedAt(t))
          f.apply(t)
        else
          t
      }
    }
  }
}

trait Rewritable {
  def dup(children: IndexedSeq[Any]): this.type
}
