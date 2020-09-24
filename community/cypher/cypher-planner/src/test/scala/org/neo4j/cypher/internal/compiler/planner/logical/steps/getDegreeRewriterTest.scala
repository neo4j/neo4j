/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegree
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasDegreeLessThan
import org.neo4j.cypher.internal.expressions.HasDegreeLessThanOrEqual
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Length
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class getDegreeRewriterTest extends CypherFunSuite with AstConstructionTestSupport {

  // All pattern elements have been named at this point, so the outer scope of PatternExpression is used to signal which expressions come from the outside.
  // In the test names, empty names denote anonymous notes, i.e. ones that do not come from the outside.

  test("Rewrite exists( (a)-[:FOO]->() ) to GetDegree( (a)-[:FOO]->() ) > 0") {
    val incoming = Exists(pattern(from = Some("a"), relationships = Seq("FOO")))(pos)
    val expected =  HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(0))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite exists( ()-[:FOO]->(a) ) to GetDegree( (a)<-[:FOO]-() ) > 0") {
    val incoming = Exists(pattern(to = Some("a"), relationships = Seq("FOO")))(pos)
    val expected =  HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(0))(pos)
    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite exists( (a)-[:FOO|:BAR]->() ) to HasDegreeGreaterThan( (a)-[:FOO]->(), 0) OR HasDegreeGreaterThan( (a)-[:BAR]->(), 0)") {
    val incoming = Exists.asInvocation(pattern(from = Some("a"), relationships = Seq("FOO", "BAR")))(pos)
    val expected =
      or(
        HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(0))(pos),
        HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("BAR")(pos)), OUTGOING, literalInt(0))(pos)
      )

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Does not rewrite exists( (a)-[:FOO]->(b) )") {
    val incoming = Exists(pattern(from = Some("a"), to = Some("b"), relationships = Seq("FOO")))(pos)

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test("Rewrite length( (a)-[:FOO]->() ) to GetDegree( (a)-[:FOO]->() )") {
    val incoming = Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos)
    val expected = GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.OUTGOING)(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( ()-[:FOO]->(a) ) to GetDegree( (a)<-[:FOO]-() )") {
    val incoming = Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos)
    val expected = GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.INCOMING)(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( (a)-[:FOO|:BAR]->() ) to GetDegree( (a)-[:FOO]->() ) + GetDegree( (a)-[:BAR]->() )") {
    val incoming = Length(pattern(from = Some("a"), relationships = Seq("FOO", "BAR")))(pos)
    val expected =
      add(
        GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING)(pos),
        GetDegree(varFor("a"), Some(RelTypeName("BAR")(pos)), OUTGOING)(pos)
      )

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Does not rewrite length( (a)-[:FOO]->(b) ) ") {
    val incoming = Length(pattern(from = Some("a"), to = Some("b"), relationships = Seq("FOO")))(pos)

    getDegreeRewriter(incoming) should equal(incoming)
  }

  test("Rewrite length( (a)-[:FOO]->() ) > 5 to HasDegreeGreaterThan( (a)-[:FOO]->() , 5)") {
    val incoming = GreaterThan(Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos), literalInt(5))(pos)
    val expected = HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( ()-[:FOO]->(a) ) > 5 to HasDegreeGreaterThan( (a)<-[:FOO]-() , 5)") {
    val incoming = GreaterThan(Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos), literalInt(5))(pos)
    val expected = HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite 5 > length( (a)-[:FOO]->() ) to HasDegreeLessThan( (a)-[:FOO]->() , 5)") {
    val incoming = GreaterThan(literalInt(5), Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos))(pos)
    val expected = HasDegreeLessThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite 5 > length( ()-[:FOO]->(a) ) to HasDegreeLessThan( (a)<-[:FOO]-() , 5)") {
    val incoming = GreaterThan(literalInt(5), Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos))(pos)
    val expected = HasDegreeLessThan(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( (a)-[:FOO]->() ) >= 5 to HasDegreeGreaterThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = GreaterThanOrEqual(Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos), literalInt(5))(pos)
    val expected = HasDegreeGreaterThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( ()-[:FOO]->(a) ) >= 5 to HasDegreeGreaterThanOrEqual( (a)<-[:FOO]-() , 5)") {
    val incoming = GreaterThanOrEqual(Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos), literalInt(5))(pos)
    val expected = HasDegreeGreaterThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite 5 >= length( (a)-[:FOO]->() ) to HasDegreeLessThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = GreaterThanOrEqual(literalInt(5), Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos))(pos)
    val expected = HasDegreeLessThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite 5 >= length( ()-[:FOO]->(a) ) to HasDegreeLessThanOrEqual( (a)<-[:FOO]-() , 5)") {
    val incoming = GreaterThanOrEqual(literalInt(5), Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos))(pos)
    val expected = HasDegreeLessThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( (a)-[:FOO]->() ) = 5 to HasDegree( (a)-[:FOO]->() , 5)") {
    val incoming = Equals(Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos), literalInt(5))(pos)
    val expected = HasDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( ()-[:FOO]->(a) ) = 5 to HasDegree( (a)<-[:FOO]-() , 5)") {
    val incoming = Equals(Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos), literalInt(5))(pos)
    val expected = HasDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite 5 = length( (a)-[:FOO]->() ) to HasDegree( (a)-[:FOO]->() , 5)") {
    val incoming = Equals(literalInt(5), Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos))(pos)
    val expected = HasDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite 5 = length( ()-[:FOO]->(a) ) to HasDegree( (a)<-[:FOO]-() , 5)") {
    val incoming = Equals(literalInt(5), Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos))(pos)
    val expected = HasDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( (a)-[:FOO]->() ) <= 5 to HasDegreeLessThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = LessThanOrEqual(Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos), literalInt(5))(pos)
    val expected = HasDegreeLessThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( ()-[:FOO]->(a) ) <= 5 to HasDegreeLessThanOrEqual( (a)<-[:FOO]-() , 5)") {
    val incoming = LessThanOrEqual(Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos), literalInt(5))(pos)
    val expected = HasDegreeLessThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite 5 <= length( (a)-[:FOO]->() ) to HasDegreeGreaterThanOrEqual( (a)-[:FOO]->() , 5)") {
    val incoming = LessThanOrEqual(literalInt(5), Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos))(pos)
    val expected = HasDegreeGreaterThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite 5 <= length( ()-[:FOO]->(a) ) to HasDegreeGreaterThanOrEqual( (a)<-[:FOO]-() , 0)") {
    val incoming = LessThanOrEqual(literalInt(5), Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos))(pos)
    val expected = HasDegreeGreaterThanOrEqual(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( (a)-[:FOO]->() ) < 5 to HasDegreeLessThan( (a)-[:FOO]->() , 0)") {
    val incoming = LessThan(Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos), literalInt(5))(pos)
    val expected = HasDegreeLessThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite length( ()-[:FOO]->(a) ) < 5 to HasDegreeLessThan( (a)<-[:FOO]-() , 0)") {
    val incoming = LessThan(Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos), literalInt(5))(pos)
    val expected = HasDegreeLessThan(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite 5 < length( (a)-[:FOO]->() ) to HasDegreeGreaterThan( (a)-[:FOO]->() , 5)") {
    val incoming = LessThan(literalInt(5), Length(pattern(from = Some("a"), relationships = Seq("FOO")))(pos))(pos)
    val expected = HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), OUTGOING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite 5 < length( ()-[:FOO]->(a) ) to HasDegreeGreaterThan( (a)<-[:FOO]-() , 5)") {
    val incoming = LessThan(literalInt(5), Length(pattern(to = Some("a"), relationships = Seq("FOO")))(pos))(pos)
    val expected = HasDegreeGreaterThan(varFor("a"), Some(RelTypeName("FOO")(pos)), INCOMING, literalInt(5))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  private def pattern(from: Option[String] = None, rel: Option[String] = None, to: Option[String] = None, relationships: Seq[String] = Seq.empty) = {
    PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(from.map(varFor).getOrElse(varFor("DEFAULT"))), Seq.empty, None)(pos),
      RelationshipPattern(Some(varFor("r")), relationships.map(r => RelTypeName(r)(pos)), None, None, SemanticDirection.OUTGOING)(pos),
      NodePattern(Some(to.map(varFor).getOrElse(varFor("DEFAULT"))), Seq.empty, None)(pos))(pos))(pos))((from.toSet ++ to.toSet).map(varFor))
  }
}
