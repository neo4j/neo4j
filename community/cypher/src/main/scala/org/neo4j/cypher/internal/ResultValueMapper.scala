package org.neo4j.cypher.internal

import commands.values.{LabelValue, ResolvedLabel, LabelName}
import spi.QueryContext

/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */

class ResultValueMapper(query: QueryContext) extends (Any => Any) {

  def apply(in: Any) = in match {
    case l: LabelName => l.name
    case l: LabelValue => l.resolve(query).name
    case m: scala.collection.Map[_, _] => m.mapValues(this) // Do we need this?
    case i: Traversable[_] => i.map(this)
    case x => x
  }
}