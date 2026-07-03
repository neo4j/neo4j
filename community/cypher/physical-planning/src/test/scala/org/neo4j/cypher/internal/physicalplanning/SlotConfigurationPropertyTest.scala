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
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer

class SlotConfigurationPropertyTest extends CypherFunSuite with CypherScalaCheckDrivenPropertyChecks {

  private val types = ArraySeq(CTNode, CTRelationship)

  test("slot configuration with random slots") {
    forAll(genSlotConfig(), minSuccessful(100)) { state =>
      val mutableSlots = state.builder
      mutableSlots.finalized shouldBe false
      val slots = mutableSlots.build()
      mutableSlots.finalized shouldBe true

      withClue(s"\n$mutableSlots\n---\n$slots\n---\n") {
        assertEquivalent(state, slots, mutableSlots)
      }
    }
  }

  private def assertEquivalent(state: State, slots: SlotConfiguration, mutableSlots: SlotConfigurationBuilder): Unit = {
    state.names.foreach { name =>
      slots.contains(name) shouldBe true
      mutableSlots.contains(name) shouldBe true
      slots(name).slot shouldBe mutableSlots(name)
      slots(VariableSlotKey(name)).slot shouldBe mutableSlots(VariableSlotKey(name))
      slots.getSlot(name) shouldBe mutableSlots.getSlot(name)
    }

    slots.numberOfLongs shouldBe mutableSlots.numberOfLongs
    slots.numberOfReferences shouldBe mutableSlots.numberOfReferences
    slots.legacyView.iterable.toSeq should contain theSameElementsAs mutableSlots.iterable.toSeq
    slots.legacyView.iterableWithAliases.toSeq should contain theSameElementsAs mutableSlots.iterableWithAliases.toSeq
    slots.legacyView.keyedSlots.toSeq should contain theSameElementsAs mutableSlots.keyedSlots.toSeq
    slots.legacyView.keyedSlotsOrdered() should contain theSameElementsInOrderAs mutableSlots.keyedSlotsOrdered()
  }

  private def genSlotConfig(): Gen[State] = {
    for {
      mutations <- Gen.choose(0, 128)
      state <- Range(0, mutations).foldLeft(Gen.const(State())) {
        case (gen, _) => gen.flatMap(state => genSlotConfigMutation(state))
      }
    } yield state
  }

  private def genSlotConfigMutation(state: State): Gen[State] = {
    Gen.frequency(
      100 -> genNewLong(state),
      100 -> genNewRef(state),
      50 -> genNewAlias(state),
      20 -> genDiscard(state)
    )
  }

  private def genNewLong(state: State): Gen[State] = {
    for {
      cypherType <- Gen.oneOf(types)
      nullable <- Arbitrary.arbitrary[Boolean]
    } yield State(state.builder.newLong(state.varName, nullable, cypherType), state.vars + 1)
  }

  private def genNewRef(state: State): Gen[State] = {
    for {
      cypherType <- Gen.oneOf(types)
      nullable <- Arbitrary.arbitrary[Boolean]
    } yield State(state.builder.newReference(state.varName, nullable, cypherType), state.vars + 1)
  }

  private def genNewAlias(state: State): Gen[State] = {
    for {
      name <- if (state.names.nonEmpty) Gen.oneOf(state.names) else Gen.const("")
    } yield {
      if (name.nonEmpty) State(state.builder.addAlias(state.varName, name), state.vars + 1)
      else state
    }
  }

  private def genDiscard(state: State): Gen[State] = {
    for {
      names <- Gen.const(state.names)
      name <- if (names.nonEmpty) Gen.oneOf(names) else Gen.const("x")
    } yield {
      state.builder.markDiscarded(name)
      state
    }
  }
}

case class State(builder: SlotConfigurationBuilder = SlotConfigurationBuilder.empty, vars: Int = 0) {
  def newVar: State = copy(vars = vars + 1)
  def varName: String = "var" + vars
  def names: IndexedSeq[String] = Range(0, vars).map(i => "var" + i)
}

case class Namer(counter: AtomicLong = new AtomicLong()) {
  private val vars = new ArrayBuffer[String]

  def nextVar(): String = {
    val name = "var" + counter.incrementAndGet().toString
    vars.addOne(name)
    name
  }
  def varNames: collection.IndexedSeq[String] = vars
}
