/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal

sealed trait PreParserOption
sealed abstract class ExecutionModePreParserOption(val name: String) extends PreParserOption
sealed abstract class PlannerPreParserOption(val name: String) extends PreParserOption
sealed abstract class RuntimePreParserOption(val name: String) extends PreParserOption

case class VersionOption(version: String) extends PreParserOption
case object ProfileOption extends ExecutionModePreParserOption("profile")
case object ExplainOption extends ExecutionModePreParserOption("explain")
case object CostPlannerOption extends PlannerPreParserOption("cost")
case object GreedyPlannerOption extends PlannerPreParserOption("greedy")
case object RulePlannerOption extends PlannerPreParserOption("rule")
case object IDPPlannerOption extends PlannerPreParserOption("idp")
case object DPPlannerOption extends PlannerPreParserOption("dp")
case object InterpretedRuntimeOption extends RuntimePreParserOption("interpreted")
case class ConfigurationOptions(version: Option[VersionOption], options: Seq[PreParserOption]) extends PreParserOption
