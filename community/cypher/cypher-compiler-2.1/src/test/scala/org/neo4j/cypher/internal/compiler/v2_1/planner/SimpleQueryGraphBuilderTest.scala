import org.neo4j.cypher.internal.compiler.v2_1.planner.{Selections, SimpleQueryGraphBuilder}
import org.neo4j.cypher.internal.compiler.v2_1.{InputPosition, DummyPosition}
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.parser.CypherParser
import org.neo4j.cypher.internal.compiler.v2_1.DummyPosition
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Id

class SimpleQueryGraphBuilderTest extends CypherFunSuite {

  val parser = new CypherParser()
  val pos = DummyPosition(0)

  test("projection only query") {
    val ast = parse("RETURN 42")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)
    qg.projections should equal(Map("42" -> SignedIntegerLiteral("42")(pos)))
  }

  test("multiple projection query") {
    val ast = parse("RETURN 42, 'foo'")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)
    qg.projections should equal(Map(
      "42" -> SignedIntegerLiteral("42")(pos),
      "'foo'" -> StringLiteral("foo")(pos)
    ))
  }

  test("match n return n") {
    val ast = parse("MATCH n RETURN n")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)

    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.identifiers should equal(Set(Id("n")))
  }

  test("match n where n:Awesome return n") {
    val ast = parse("MATCH n WHERE n:Awesome RETURN n")
    val builder = new SimpleQueryGraphBuilder
    val qg = builder.produce(ast)

    qg.projections should equal(Map(
      "n" -> Identifier("n")(pos)
    ))

    qg.selections should equal(Selections(Seq(
      Set(Id("n")) -> HasLabels(Identifier("n")(pos), Seq(Identifier("Awesome")(pos)))(pos)
    )))

    qg.identifiers should equal(Set(Id("n")))
  }

  def parse(s: String): Query =
    parser.parse(s).asInstanceOf[Query]
}
