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
package org.neo4j.cypher.docgen

import org.junit.Test
import org.neo4j.cypher.{MergeConstraintConflictException, QueryStatisticsTestSupport}
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}

class MergeTest extends DocumentingTestBase with QueryStatisticsTestSupport with SoftReset {

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section = "Merge"

  override def graphDescription = List(
    "Charlie:Person ACTED_IN WallStreet:Movie",
    "Martin:Person ACTED_IN WallStreet:Movie",
    "Michael:Person ACTED_IN WallStreet:Movie",
    "Martin:Person ACTED_IN TheAmericanPresident:Movie",
    "Michael:Person ACTED_IN TheAmericanPresident:Movie",
    "Oliver:Person DIRECTED WallStreet:Movie",
    "Rob:Person DIRECTED TheAmericanPresident:Movie",
    "Charlie:Person FATHER Martin:Person")

  override val properties = Map(
    "Charlie" -> Map("name" -> "Charlie Sheen", "bornIn" -> "New York", "chauffeurName" -> "John Brown"),
    "Oliver" -> Map("name" -> "Oliver Stone", "bornIn" -> "New York", "chauffeurName" -> "Bill White"),
    "Michael" -> Map("name" -> "Michael Douglas", "bornIn" -> "New Jersey", "chauffeurName" -> "John Brown"),
    "Rob" -> Map("name" -> "Rob Reiner", "bornIn" -> "New York", "chauffeurName" -> "Ted Green"),
    "Martin" -> Map("name" -> "Martin Sheen", "bornIn" -> "Ohio", "chauffeurName" -> "Bob Brown"),
    "WallStreet" -> Map("title" -> "Wall Street"),
    "TheAmericanPresident" -> Map("title" -> "The American President")
  )

  override val setupConstraintQueries = List(
    "CREATE CONSTRAINT ON (n:Person) ASSERT n.name IS UNIQUE",
    "CREATE CONSTRAINT ON (n:Person) ASSERT n.role IS UNIQUE")

  @Test def merge_single_node_with_label() {
    testQuery(
      title = "Merge single node with a label",
      text = "Merging a single node with a given label.",
      queryText = "merge (robert:Critic)\nreturn robert, labels(robert)",
      optionalResultExplanation = "A new node is created because there are no nodes labeled +Critic+ in the database.",
      assertions = (p) => assertStats(p, nodesCreated = 1, labelsAdded = 1)
    )
  }

  @Test def merge_single_node_with_properties() {
    testQuery(
      title = "Merge single node with properties",
      text = "Merging a single node with properties where not all properties match any existing node.",
      queryText = "merge (charlie {name:'Charlie Sheen', age:10})\nreturn charlie",
      optionalResultExplanation = "A new node with the name 'Charlie Sheen' will be created since not all properties " +
        "matched the existing 'Charlie Sheen' node.",
      assertions = (p) => assertStats(p, nodesCreated = 1, propertiesSet = 2)
    )
  }

  @Test def merge_single_node_with_label_and_property() {
    testQuery(
      title = "Merge single node specifying both label and property",
      text = "Merging a single node with both label and property matching an existing node.",
      queryText = "merge (michael:Person {name:'Michael Douglas'})\nreturn michael.name, michael.bornIn",
      optionalResultExplanation = "'Michael Douglas' will be matched and the _name_ and  _bornIn_ properties returned.",
      assertions = (p) => assertStats(p, nodesCreated = 0, propertiesSet = 0)
    )
  }

  @Test def merge_single_node_derived_from_an_existing_node_property() {
    testQuery(
      title = "Merge single node derived from an existing node property",
      text =
        """For some property 'p' in each bound node in a set of nodes, a single new node is created for each
unique value for 'p'.""",
      queryText = "match (person:Person)\nmerge (city:City {name: person.bornIn})\nreturn person.name, person.bornIn, city",
      optionalResultExplanation = """Three nodes labeled +City+ are created, each of which contains a 'name' property
with the value of 'New York', 'Ohio', and 'New Jersey', respectively. Note that even
though the +MATCH+ clause results in three bound nodes having the value 'New York' for
the 'bornIn' property, only a single 'New York' node (i.e. a +City+ node with a name
of 'New York') is created. As the 'New York' node is not matched for the first bound
node, it is created. However, the newly-created 'New York' node is matched and bound for the second and third bound nodes.""",
      assertions = (p) => assertStats(p, nodesCreated = 3, propertiesSet = 3, labelsAdded = 3)
    )
  }

  @Test def merge_node_and_set_property_on_creation() {
    testQuery(
      title = "Merge with ON CREATE",
      text = "Merge a node and set properties if the node needs to be created.",
      queryText = """merge (keanu:Person {name:'Keanu Reeves'})
on create set keanu.created = timestamp()
return keanu.name, keanu.created""",
      optionalResultExplanation = "The query creates the 'keanu' node and sets a timestamp on creation time.",
      assertions = (p) => assertStats(p, nodesCreated = 1, propertiesSet = 2, labelsAdded = 1)
    )
  }

  @Test def merge_node_and_set_property_on_match() {
    testQuery(
      title = "Merge with ON MATCH",
      text = "Merging nodes and setting properties on found nodes.",
      queryText = "merge (person:Person) on match set person.found = true return person.name, person.found",
      optionalResultExplanation = "The query finds all the +Person+ nodes, sets a property on them, and returns them.",
      assertions = (p) => assertStats(p, propertiesSet = 5)
    )
  }

  @Test def merge_node_and_set_property_on_creation_or_update_prop() {
    testQuery(
      title = "Merge with ON CREATE and ON MATCH",
      text = "Merge a node and set properties if the node needs to be created.",
      queryText =
        """merge (keanu:Person {name:'Keanu Reeves'})
on create set keanu.created = timestamp()
on match set keanu.lastSeen = timestamp()
return keanu.name, keanu.created, keanu.lastSeen""",
      optionalResultExplanation = "The query creates the 'keanu' node, and sets a timestamp on creation time. If 'keanu' had already existed, a " +
        "different property would have been set.",
      assertions = (p) => assertStats(p, nodesCreated = 1, propertiesSet = 2, labelsAdded = 1)
    )
  }

  @Test def merge_and_set_multiple_properties_on_match() {
    testQuery(
      title = "Merge with ON MATCH setting multiple properties",
      text = "If multiple properties should be set, simply separate them with commas.",
      queryText = """merge (person:Person)
                    |on match set person.found = true, person.lastAccessed = timestamp()
                    |return person.name, person.found, person.lastAccessed""".stripMargin,
      optionalResultExplanation = "",
      assertions = (p) => assertStats(p, propertiesSet = 10)
    )
  }

  @Test def merge_node_and_create_with_unique_constraint() {
    testQuery(
      title = "Merge using unique constraints creates a new node if no node is found",
      text = "Merge using unique constraints creates a new node if no node is found.",
      queryText = """merge (laurence:Person {name: 'Laurence Fishburne'}) return laurence.name""",
      optionalResultExplanation = "The query creates the 'laurence' node. If 'laurence' had already existed, +MERGE+ would " +
        "just match the existing node.",
      assertions = (p) => assertStats(p, nodesCreated = 1, propertiesSet = 1, labelsAdded = 1)
    )
  }

  @Test def merge_node_and_match_with_unique_constraint() {
    testQuery(
      title = "Merge using unique constraints matches an existing node",
      text = "Merge using unique constraints matches an existing node.",
      queryText = """merge (oliver:Person {name:'Oliver Stone'}) return oliver.name, oliver.bornIn""",
      optionalResultExplanation = "The 'oliver' node already exists, so +MERGE+ just matches it.",
      assertions = (p) => assertStats(p, nodesCreated = 0, propertiesSet = 0, labelsAdded = 0)
    )
  }

  @Test def merge_node_and_match_many_with_unique_constraint_fails_for_partial_matches() {
    generateConsole = false
    testFailingQuery[MergeConstraintConflictException](
      title = "Merge with unique constraints and partial matches",
      text = "Merge using unique constraints fails when finding partial matches.",
      queryText = """merge (michael:Person {name:'Michael Douglas', role:'Gordon Gekko'}) return michael""",
      optionalResultExplanation = "While there is a matching unique 'michael' node with the name 'Michael Douglas', there is no " +
        "unique node with the role of 'Gordon Gekko' and +MERGE+ fails to match."
    )
  }

  @Test def merge_node_and_match_many_with_unique_constraint_fails_for_conflicting_matches() {
    generateConsole = false
    testFailingQuery[MergeConstraintConflictException](
      title = "Merge with unique constraints and conflicting matches",
      text = "Merge using unique constraints fails when finding conflicting matches.",
      queryText = """merge (oliver:Person {name:'Oliver Stone', role:'Gordon Gekko'}) return oliver""",
      optionalResultExplanation = "While there is a matching unique 'oliver' node with the name 'Oliver Stone', there is also another " +
        "unique node with the role of 'Gordon Gekko' and +MERGE+ fails to match."
    )
  }

  @Test def using_map_parameters_with_merge() {
    testQuery(
      title = "Using map parameters with MERGE",
      text = """+MERGE+ does not support map parameters like for example +CREATE+ does.
To use map parameters with +MERGE+, it is necessary to explicitly use the expected properties, like in the following example.
For more information on parameters, see <<cypher-parameters>>.""",
      parameters = Map("param" -> Map("name" -> "Keanu Reeves", "role" -> "Neo")),
      queryText = "merge (person:Person {name:{param}.name, role:{param}.role}) return person.name, person.role",
      optionalResultExplanation = "",
      assertions = p => assertStats(p, nodesCreated = 1, propertiesSet = 2, labelsAdded = 1)
    )
  }

  @Test def merging_on_a_single_relationship() {
    testQuery(
      title = "Merge on a relationship",
      text = "+MERGE+ can be used to match or create a relationship.",
      queryText =
        """match (charlie:Person {name:'Charlie Sheen'}), (wallStreet:Movie {title:'Wall Street'})
merge (charlie)-[r:ACTED_IN]->(wallStreet)
return charlie.name, type(r), wallStreet.title""",
      optionalResultExplanation = "'Charlie Sheen' had already been marked as acting in 'Wall Street', so the existing relationship is found " +
        "and returned. Note that in order to match or create a relationship when using +MERGE+, at least one bound node " +
        "must be specified, which is done via the +MATCH+ clause in the above example.",
      assertions = (p) => assertStats(p, relationshipsCreated = 0)
    )
  }

  @Test def merging_on_a_longer_pattern() {
    testQuery(
      title = "Merge on multiple relationships",
      text = "When +MERGE+ is used on a whole pattern, either everything matches, or everything is created.",
      queryText =
        """match (oliver:Person {name:'Oliver Stone'}), (reiner:Person {name:'Rob Reiner'})
merge (oliver)-[:DIRECTED]->(movie:Movie)<-[:ACTED_IN]-(reiner)
return movie""",
      optionalResultExplanation = "In our example graph, 'Oliver Stone' and 'Rob Reiner' have never worked together. When we try to +MERGE+ a " +
        "movie between them, Neo4j will not use any of the existing movies already connected to either person. Instead, " +
        "a new 'movie' node is created.",
      assertions = (p) => assertStats(p, relationshipsCreated = 2, nodesCreated = 1, propertiesSet = 0, labelsAdded = 1)
    )
  }

  @Test def merging_on_undirected_relationship() {
    testQuery(
      title = "Merge on an undirected relationship",
      text = "+MERGE+ can also be used with an undirected relationship. When it needs to create a new one, it will pick a direction.",
      queryText =
        """match (charlie:Person {name:'Charlie Sheen'}), (oliver:Person {name:'Oliver Stone'})
merge (charlie)-[r:KNOWS]-(oliver)
return r""",
      optionalResultExplanation = "As 'Charlie Sheen' and 'Oliver Stone' do not know each other, " +
        "this +MERGE+ query will create a +:KNOWS+ relationship between them. " +
        "The direction of the created relationship is arbitrary.",
      assertions = (p) => assertStats(p, relationshipsCreated = 1)
    )
  }

  @Test def merge_on_a_relationship_between_two_existing_nodes() {
    testQuery(
      title = "Merge on a relationship between two existing nodes",
      text =
        """+MERGE+ can be used in conjunction with preceding +MATCH+ and +MERGE+ clauses to create a relationship
between two bound nodes 'm' and 'n', where 'm' is returned by +MATCH+ and 'n' is created or matched by the earlier +MERGE+.""",
      queryText =
        """match (person:Person)
merge (city:City {name: person.bornIn})
merge (person)-[r:BORN_IN]->(city)
return person.name, person.bornIn, city""".stripMargin,
      optionalResultExplanation =
        """This builds on the example from <<merge-merge-single-node-derived-from-an-existing-node-property>>. The second +MERGE+ creates a +BORN_IN+ relationship between each person and a city
corresponding to the value of the person’s 'bornIn' property. 'Charlie Sheen', 'Rob Reiner' and 'Oliver Stone' all have
a +BORN_IN+ relationship to the 'same' +City+ node ('New York').""",
      assertions = (p) => assertStats(p, nodesCreated = 3, propertiesSet = 3, labelsAdded = 3, relationshipsCreated = 5)
    )
  }

  @Test def merge_on_a_relationship_between_an_existing_node_and_a_merged_node_derived_from_a_node_property() {
    testQuery(
      title = "Merge on a relationship between an existing node and a merged node derived from a node property",
      text =
        """+MERGE+ can be used to simultaneously create both a new node 'n' and a relationship between a bound node 'm'
and 'n'.""".stripMargin,
      queryText =
        """match (person:Person)
merge (person)-[r:HAS_CHAUFFEUR]->(chauffeur:Chauffeur {name: person.chauffeurName})
return person.name, person.chauffeurName, chauffeur""".stripMargin,
      optionalResultExplanation =
        """As +MERGE+ found no matches -- in our example graph, there are no nodes labeled with +Chauffeur+ and no +HAS_CHAUFFEUR+
relationships -- +MERGE+ creates five nodes labeled with +Chauffeur+, each of which contains a 'name' property whose value corresponds
to each matched +Person+ node's 'chauffeurName' property value. +MERGE+ also creates a +HAS_CHAUFFEUR+ relationship between each
+Person+ node and the newly-created corresponding +Chauffeur+ node.
As 'Charlie Sheen' and 'Michael Douglas' both have a chauffeur with the same name -- 'John Brown' -- a new node is created in
each case, resulting in 'two' +Chauffeur+ nodes having a 'name' of 'John Brown', correctly denoting the fact that
even though the 'name' property may be identical, these are two separate people.
This is in contrast to the example shown above in <<merge-merge-on-a-relationship-between-two-existing-nodes>>,
where we used the first +MERGE+ to bind the +City+ nodes to prevent them from being recreated (and thus duplicated) in the
second +MERGE+.""",
      assertions = (p) => assertStats(p, nodesCreated = 5, propertiesSet = 5, labelsAdded = 5, relationshipsCreated = 5)
    )
  }
}
