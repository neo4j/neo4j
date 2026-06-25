/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.options.CypherPlannerVersionOption

import scala.annotation.tailrec

sealed trait Optimisation

object Optimisation {
  case object MergeLabelInfo extends Optimisation
}

// This contains all the optimisations that are available in a given planner version.
sealed trait CypherPlannerVersionWithOptimisations {
  def introducedOptimisations: Set[Optimisation]

  def previous: Option[CypherPlannerVersionWithOptimisations] = None

  def allSupportedOptimisations: Set[Optimisation] = {
    @tailrec
    def loop(version: CypherPlannerVersionWithOptimisations, acc: Set[Optimisation]): Set[Optimisation] = {
      version.previous match {
        case Some(prev) => loop(prev, acc ++ version.introducedOptimisations)
        case None       => acc ++ version.introducedOptimisations
      }
    }

    loop(this, Set.empty)
  }
}

object CypherPlannerVersionWithOptimisations {

  case object Experimental extends CypherPlannerVersionWithOptimisations {
    override def introducedOptimisations: Set[Optimisation] = Set(Optimisation.MergeLabelInfo)

    override def previous: Option[CypherPlannerVersionWithOptimisations] =
      Some(Next)
  }

  // Planner version update: change this to the new version and create a new case object for next pointing to the new version.
  case object Next extends CypherPlannerVersionWithOptimisations {
    // Any optimisations accumulated here during development must be transferred to the newly cut release
    // version (and this reset to Set.empty) when bumping the planner version.
    override def introducedOptimisations: Set[Optimisation] = Set.empty

    override def previous: Option[CypherPlannerVersionWithOptimisations] = Some(V2026_05)
  }

  case object V2026_05 extends CypherPlannerVersionWithOptimisations {
    override def introducedOptimisations: Set[Optimisation] = Set.empty

    override def previous: Option[CypherPlannerVersionWithOptimisations] = Some(V2026_04)
  }

  case object V2026_04 extends CypherPlannerVersionWithOptimisations {
    override def introducedOptimisations: Set[Optimisation] = Set.empty

    override def previous: Option[CypherPlannerVersionWithOptimisations] = Some(V2026_03)
  }

  case object V2026_03 extends CypherPlannerVersionWithOptimisations {
    override def introducedOptimisations: Set[Optimisation] = Set.empty

    override def previous: Option[CypherPlannerVersionWithOptimisations] = None
  }

  def fromQueryOption(queryOption: CypherPlannerVersionOption): CypherPlannerVersionWithOptimisations = {
    queryOption match {
      case CypherPlannerVersionOption.experimental => Experimental
      case CypherPlannerVersionOption.next         => Next
      case CypherPlannerVersionOption.v2026_05     => V2026_05
      case CypherPlannerVersionOption.v2026_04     => V2026_04
      case CypherPlannerVersionOption.v2026_03     => V2026_03
    }
  }

  def allSupportedOptimisations(queryOption: CypherPlannerVersionOption): Set[Optimisation] =
    fromQueryOption(queryOption).allSupportedOptimisations
}
