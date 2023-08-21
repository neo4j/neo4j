/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.label_expressions

/**
 * Represents the set of labels on a given node in the context of solving a node pattern.
 * For example KnownLabels({A,B}) is a solution to (:A&B).
 * SomeUnknownLabels is a more subtle beast, it represent a node containing at least one label that isn't known yet.
 * For example, given the pattern (:!%|%), KnownLabels({}) is the unique solution to !%, and SomeUnknownLabels the synthetic representation of all solutions to %, to be materialised later on.
 * Similarly, evaluating (:%&!(A&B)) gives us the following solutions: {A}, {B}, and SomeUnknownLabels.
 */
sealed trait NodeLabels

object NodeLabels {
  type LabelName = String
  final case class KnownLabels(labelNames: Set[LabelName]) extends NodeLabels
  final case object SomeUnknownLabels extends NodeLabels
}
