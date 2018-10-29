/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance.comparisonsupport

import org.neo4j.internal.cypher.acceptance.comparisonsupport.Planners.Cost
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Planners.Rule
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.CompiledBytecode
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.CompiledSource
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.Interpreted
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.Slotted
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes.SlottedWithCompiledExpressions
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.V2_3
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.V3_1
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.V3_4
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions.V3_5

object Configs {

  // Configurations with runtimes
  def Compiled: TestConfiguration = TestConfiguration(V3_4 -> V3_5, Planners.all, Runtimes(CompiledSource, CompiledBytecode))

  def Morsel: TestConfiguration = TestConfiguration(V3_4 -> V3_5, Planners.all, Runtimes(Runtimes.Morsel))

  def InterpretedRuntime: TestConfiguration =
    TestConfiguration(Versions.all, Planners.all, Runtimes(Interpreted))

  def SlottedRuntime: TestConfiguration = TestConfiguration(V3_4 -> V3_5, Planners.all, Runtimes(Slotted, SlottedWithCompiledExpressions))

  def InterpretedAndSlotted: TestConfiguration = InterpretedRuntime + SlottedRuntime

  // Configurations for planners
  def RulePlanner: TestConfiguration = TestConfiguration(Versions.all, Rule, Runtimes.all)

  def CostPlanner: TestConfiguration = TestConfiguration(Versions.all, Cost, Runtimes.all)

  // Configurations for versions + planners
  def Cost2_3: TestConfiguration = TestConfiguration(V2_3, Cost, Runtimes.all)

  def Cost3_1: TestConfiguration = TestConfiguration(V3_1, Cost, Runtimes.all)

  def Cost3_4: TestConfiguration = TestConfiguration(V3_4, Cost, Runtimes.all)

  def Rule2_3: TestConfiguration = TestConfiguration(V2_3, Rule, Runtimes.all)

  def Rule3_1: TestConfiguration = TestConfiguration(V3_1, Rule, Runtimes.all)

  // Configurations for versions
  def Version2_3: TestConfiguration = TestConfiguration(V2_3, Planners.all, Runtimes.all)

  def Version3_1: TestConfiguration = TestConfiguration(V3_1, Planners.all, Runtimes.all)

  def Version3_4: TestConfiguration = TestConfiguration(V3_4, Planners.all, Runtimes.all)

  def Version3_5: TestConfiguration = TestConfiguration(V3_5, Planners.all, Runtimes.all)

  /**
    * Configs which support CREATE, DELETE, SET, REMOVE, MERGE etc.
    */
  def UpdateConf: TestConfiguration = InterpretedAndSlotted - Cost2_3

  /**
    * These are all configurations that will be executed even if not explicitly expected to succeed or fail.
    * Even if not explicitly requested, they are executed to check if they unexpectedly succeed to make sure that
    * test coverage is kept up-to-date with new features.
    */
  def All: TestConfiguration =
    TestConfiguration(Versions.all, Planners.all, Runtimes.all) -
      // No rule planner after 3.1
      TestConfiguration(V3_4 -> V3_5, Rule, Runtimes.all) -
      // No slotted runtime before 3.4
      TestConfiguration(V2_3 -> V3_1, Planners.all, Runtimes(Slotted, SlottedWithCompiledExpressions)) -
      // No slotted runtime with compiled expressions before 3.5
      TestConfiguration(V3_4, Planners.all, Runtimes(SlottedWithCompiledExpressions)) -
      // No compiled runtime before 3.4
      TestConfiguration(V2_3 -> V3_1, Planners.all, Runtimes(CompiledSource, CompiledBytecode))

  /**
    * These experimental configurations will only be executed if you explicitly specify them in the test expectation.
    * I.e. there will be no check to see if they unexpectedly succeed on tests where they were not explicitly requested.
    */
  def Experimental: TestConfiguration =
  //TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.Morsel))
    TestConfiguration.empty

  def Empty: TestConfiguration = TestConfiguration.empty

}
