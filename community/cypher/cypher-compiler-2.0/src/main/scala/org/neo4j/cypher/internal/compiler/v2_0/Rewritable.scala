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

object Rewriter {
  implicit class LiftedRewriter(f: (Any => Option[Any])) extends Rewriter {
    def apply(term: Any): Option[Any] = f.apply(term)
  }
  def lift(f: PartialFunction[Any, Any]): Rewriter = f.lift
}

trait Rewriter extends (Any => Option[Any])


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
    def rewrite(rewriter: Rewriter): Any = rewriter.apply(any).getOrElse(any)
  }
}

trait Rewritable {
  def dup(children: IndexedSeq[Any]): this.type
}

case class topDown(rewriters: Rewriter*) extends Rewriter {
  import Rewritable._
  def apply(term: Any): Some[Any] = {
    val rewrittenTerm = rewriters.foldLeft(term) {
      (t, r) => t.rewrite(r)
    }
    Some(rewrittenTerm.dup(t => this.apply(t).get))
  }
}

case class untilMatched(rewriter: Rewriter) extends Rewriter {
  import Rewritable._
  def apply(term: Any): Some[Any] =
    Some(rewriter.apply(term).getOrElse(term.dup(t => this.apply(t).get)))
}

case class bottomUp(rewriters: Rewriter*) extends Rewriter {
  import Rewritable._
  def apply(term: Any): Some[Any] =
    Some(rewriters.foldLeft(term.dup(t => this.apply(t).get)) {
      (t, r) => t.rewrite(r)
    })
}

case class bottomUpRepeated(rewriter: Rewriter) extends Rewriter {
  import Rewritable._
  def apply(term: Any): Some[Any] = {
    val rewrittenTerm = term.dup(t => this.apply(t).get)
    rewriter.apply(rewrittenTerm).fold(Some(rewrittenTerm)) {
      t => if (t == term)
        Some(t)
      else
        Some(t.rewrite(this))
    }
  }
}

case class repeat(rewriter: Rewriter) extends Rewriter {
  import Rewritable._
  def apply(term: Any): Option[Any] =
    rewriter.apply(term).map {
      t => if (t == term)
        term
      else
        t.rewrite(this)
    }
}
