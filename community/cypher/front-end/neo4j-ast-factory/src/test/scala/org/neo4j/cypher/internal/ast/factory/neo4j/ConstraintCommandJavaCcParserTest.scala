/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.ConstraintVersion0
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropNodeKeyConstraint
import org.neo4j.cypher.internal.ast.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.ConstraintType
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.scalatest.FunSuiteLike

class ConstraintCommandJavaCcParserTest extends ParserComparisonTestBase with FunSuiteLike with TestName with AstConstructionTestSupport {

  Seq("ON", "FOR")
    .foreach { onOrFor =>
      Seq("ASSERT", "REQUIRE")
        .foreach { assertOrRequire =>
          // Create constraint: Without name

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node3.prop2) IS NODE KEY") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node3.prop2) IS NODE KEY") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY OPTIONS {indexProvider : 'native-btree-1.0'}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY OPTIONS {indexProvider : 'lucene+native-3.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY OPTIONS {nonValidOption : 42}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY OPTIONS {}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor ()-[r1:REL]-() $assertOrRequire (r2.prop) IS NODE KEY") {
            assertJavaCCExceptionStart(testName, ASTExceptionFactory.relationshipPattternNotAllowed(ConstraintType.NODE_KEY))
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire node2.prop IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire node2.prop IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire node2.prop IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node.prop2) IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node3.prop2) IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS UNIQUE OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}, indexProvider : 'native-btree-1.0'}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor ()-[r1:R]-() $assertOrRequire (r2.prop) IS UNIQUE") {
            assertJavaCCExceptionStart(testName, ASTExceptionFactory.relationshipPattternNotAllowed(ConstraintType.UNIQUE))
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire node2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire node2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire node2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor ()-[r1:R]-() $assertOrRequire r2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor ()-[r1:R]->() $assertOrRequire r2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor ()<-[r1:R]-() $assertOrRequire (r2.prop) IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT $onOrFor ()<-[r1:R]-() $assertOrRequire r2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $onOrFor ()-[r1:R]-() $assertOrRequire r2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT IF NOT EXISTS $onOrFor ()-[r1:R]->() $assertOrRequire r2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor ()-[r1:R]-() $assertOrRequire (r2.prop) IS NOT NULL OPTIONS {}") {
            assertSameAST(testName)
          }

          // Create constraint: With name

          test(s"USE neo4j CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY") {
            assertSameAST(testName)
          }

          test(s"USE neo4j CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node2.prop2) IS NODE KEY") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node3.prop2) IS NODE KEY") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node3.prop2) IS NODE KEY") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire node2.prop IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node3.prop2) IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node3.prop2) IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node3.prop2) IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS UNIQUE OPTIONS {indexProvider : 'native-btree-1.0'}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS UNIQUE OPTIONS {indexProvider : 'lucene+native-3.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS UNIQUE OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS UNIQUE OPTIONS {nonValidOption : 42}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop1,node3.prop2) IS UNIQUE OPTIONS {}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY {indexProvider : 'native-btree-1.0'}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NODE KEY OPTIONS") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire node2.prop.part IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop.part) IS UNIQUE") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS UNIQUE {indexProvider : 'native-btree-1.0'}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT $onOrFor (node1:Label) $assertOrRequire (node2.prop1, node.prop2) IS UNIQUE OPTIONS") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop) IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire node2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire (node2.prop2, node3.prop3) IS NOT NULL") {
            assertJavaCCException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_IS_NOT_NULL)))
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint $onOrFor ()-[r1:REL]-() $assertOrRequire (r2.prop2, r3.prop3) IS NOT NULL") {
            assertJavaCCException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_IS_NOT_NULL)))
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire node2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $onOrFor (node1:Label) $assertOrRequire node2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor (node1:Label) $assertOrRequire node2.prop IS NOT NULL OPTIONS {}") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT `$$my_constraint` $onOrFor ()-[r1:R]-() $assertOrRequire r2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT my_constraint $onOrFor ()-[r1:R]-() $assertOrRequire (r2.prop) IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` $onOrFor ()-[r1:R]-() $assertOrRequire r2.prop IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` IF NOT EXISTS $onOrFor ()-[r1:R]->() $assertOrRequire (r2.prop) IS NOT NULL") {
            assertSameAST(testName)
          }

          test(s"CREATE CONSTRAINT `$$my_constraint` IF NOT EXISTS $onOrFor ()<-[r1:R]-() $assertOrRequire r2.prop IS NOT NULL") {
            assertSameAST(testName)
          }
        }
    }

  test(s"CREATE CONSTRAINT my_constraint ON ()-[r1:R]-() ASSERT r2.prop IS NULL") {
    assertJavaCCException(testName, "Invalid input 'NULL': expected \"NODE\", \"NOT\" or \"UNIQUE\" (line 1, column 67 (offset: 66))")
  }

  test(s"CREATE CONSTRAINT my_constraint FOR ()-[r1:R]-() REQUIRE r2.prop IS NULL") {
    assertJavaCCException(testName, "Invalid input 'NULL': expected \"NODE\", \"NOT\" or \"UNIQUE\" (line 1, column 69 (offset: 68))")
  }

  test(s"CREATE CONSTRAINT my_constraint ON (node1:Label) ASSERT node2.prop IS NULL") {
    assertJavaCCException(testName, "Invalid input 'NULL': expected \"NODE\", \"NOT\" or \"UNIQUE\" (line 1, column 71 (offset: 70))")
  }

  test(s"CREATE CONSTRAINT my_constraint FOR (node1:Label) REQUIRE node2.prop IS NULL") {
    assertJavaCCException(testName, "Invalid input 'NULL': expected \"NODE\", \"NOT\" or \"UNIQUE\" (line 1, column 73 (offset: 72))")
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT node2.prop IS NODE KEY") {
    assertJavaCCAST(testName,
      CreateNodeKeyConstraint(Variable("node1")(pos),
        LabelName("Label")(pos),
        Seq(Property(Variable("node2")(pos), PropertyKeyName("prop")(pos))(pos)),
        None,
        IfExistsThrowError,
        NoOptions,
        containsOn = true,
        ConstraintVersion0,
        None
      )(pos)
    )
  }

  // ASSERT EXISTS

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS (node2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE CONSTRAINT ON (node1:Label) ASSERT EXISTS (node2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node1:Label) ASSERT EXISTS (node2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON (node1:Label) ASSERT EXISTS (node2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS (node2.prop) OPTIONS {}") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT ON ()-[r1:R]->() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT ON ()<-[r1:R]-() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE CONSTRAINT ON ()<-[r1:R]-() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON ()-[r1:R]-() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r1:R]->() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS node2.prop") {
    assertJavaCCAST(testName,
      CreateNodePropertyExistenceConstraint(Variable("node1")(pos), LabelName("Label")(pos), Property(Variable("node2")(pos), PropertyKeyName("prop")(pos))(pos), None, IfExistsThrowError, NoOptions, containsOn = true, ConstraintVersion0, None)(pos))
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS (node2.prop1, node3.prop2)") {
    assertJavaCCException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_EXISTS)))
  }

  test("CREATE CONSTRAINT ON ()-[r1:REL]-() ASSERT EXISTS (r2.prop1, r3.prop2)") {
    assertJavaCCException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_EXISTS)))
  }

  test("CREATE CONSTRAINT my_constraint ON (node1:Label) ASSERT EXISTS (node2.prop) IS NOT NULL") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS r2.prop") {
    assertJavaCCAST(testName,
      CreateRelationshipPropertyExistenceConstraint(Variable("r1")(pos), RelTypeName("R")(pos), Property(Variable("r2")(pos), PropertyKeyName("prop")(pos))(pos), None, IfExistsThrowError, NoOptions, containsOn = true, ConstraintVersion0, None)(pos)
    )
  }

  test("CREATE CONSTRAINT my_constraint ON (node1:Label) ASSERT EXISTS (node2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint ON (node1:Label) ASSERT EXISTS (node2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node1:Label) ASSERT EXISTS (node2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node1:Label) ASSERT EXISTS (node2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT `$my_constraint` ON ()-[r1:R]-() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r1:R]-() ASSERT EXISTS (r2.prop) OPTIONS {}") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` ON ()-[r1:R]-() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()-[r1:R]->() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()<-[r1:R]-() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r1:R]-() ASSERT EXISTS (r2.prop) IS NOT NULL") {
    assertJavaCCExceptionStart(testName, ASTExceptionFactory.constraintTypeNotAllowed(ConstraintType.REL_IS_NOT_NULL, ConstraintType.REL_EXISTS))
  }

  test("CREATE CONSTRAINT $my_constraint ON ()-[r1:R]-() ASSERT EXISTS (r2.prop)") {
    assertJavaCCException(testName, "Invalid input '$': expected \"FOR\", \"IF\", \"ON\" or an identifier (line 1, column 19 (offset: 18))")
  }

  test("CREATE CONSTRAINT FOR (node1:Label) REQUIRE EXISTS (node1.prop)") {
    assertJavaCCException(testName, "Invalid input '(': expected \".\" (line 1, column 52 (offset: 51))")
  }

  test("CREATE CONSTRAINT FOR ()-[r1:R]-() REQUIRE EXISTS (r1.prop)") {
    assertJavaCCException(testName, "Invalid input '(': expected \".\" (line 1, column 51 (offset: 50))")
  }

  // Drop constraint

  test("DROP CONSTRAINT ON (node1:Label) ASSERT (node2.prop) IS NODE KEY") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT (node2.prop1,node3.prop2) IS NODE KEY") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT node2.prop IS UNIQUE") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS UNIQUE") {
    assertJavaCCExceptionStart(testName, ASTExceptionFactory.relationshipPattternNotAllowed(ConstraintType.UNIQUE))
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT (node2.prop) IS UNIQUE") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT (node2.prop1,node3.prop2) IS UNIQUE") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT EXISTS (node2.prop)") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT ON ()-[r1:R]->() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT ON ()<-[r1:R]-() ASSERT EXISTS (r2.prop)") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT node2.prop IS NODE KEY") {
    assertJavaCCAST(testName,
      DropNodeKeyConstraint(Variable("node1")(pos),
        LabelName("Label")(pos),
        Seq(Property(Variable("node2")(pos), PropertyKeyName("prop")(pos))(pos)),
        None)(pos)
    )
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT EXISTS node2.prop") {
    assertJavaCCAST(testName,
      DropNodePropertyExistenceConstraint(Variable("node1")(pos),
        LabelName("Label")(pos),
        Property(Variable("node2")(pos), PropertyKeyName("prop")(pos))(pos),
        None)(pos)
    )
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS NODE KEY") {
    assertJavaCCExceptionStart(testName, ASTExceptionFactory.relationshipPattternNotAllowed(ConstraintType.NODE_KEY))
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT (node2.prop) IS NOT NULL") {
    assertJavaCCException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand))
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT node2.prop IS NOT NULL") {
    assertJavaCCException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand))
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS r2.prop") {
    assertJavaCCAST(testName,
      DropRelationshipPropertyExistenceConstraint(
        Variable("r1")(pos),
        RelTypeName("R")(pos),
        Property(Variable("r2")(pos), PropertyKeyName("prop")(pos))(pos),
        None)(pos)
    )
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT (r2.prop) IS NOT NULL") {
    assertJavaCCException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand))
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS NOT NULL") {
    assertJavaCCException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand))
  }

  test("DROP CONSTRAINT my_constraint") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT `$my_constraint`") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT my_constraint IF EXISTS") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT $my_constraint") {
    assertSameAST(testName)
  }

  test("DROP CONSTRAINT my_constraint ON (node1:Label) ASSERT (node2.prop1,node3.prop2) IS NODE KEY") {
    assertJavaCCException(testName, "Invalid input 'ON': expected \"IF\" or <EOF> (line 1, column 31 (offset: 30))")
  }
}
