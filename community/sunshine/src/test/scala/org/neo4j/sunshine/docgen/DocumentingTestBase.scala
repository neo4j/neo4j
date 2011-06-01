package org.neo4j.sunshine.docgen

import org.neo4j.graphdb.{Node, GraphDatabaseService}
import org.neo4j.graphdb.index.Index
import org.neo4j.sunshine.{Projection, ExecutionEngine, SunshineParser}
import org.junit.Before
import org.neo4j.kernel.ImpermanentGraphDatabase
import org.neo4j.test.GraphDescription
import scala.collection.JavaConverters._
import java.io.{PrintWriter, File, FileWriter}
/**
 * @author ata
 * @since 6/1/11
 */

abstract class DocumentingTestBase {
  var db: GraphDatabaseService = null
  val parser: SunshineParser = new SunshineParser
  var engine: ExecutionEngine = null
  var nodes: Map[String, Node] = null
  var nodeIndex: Index[Node] = null

  def section: String

  def graphDescription: List[String]

  def indexProps: List[String]

  def nicefy(in: String): String = in.toLowerCase.replace(" ", "_")

  def dumpToFile(writer: PrintWriter, title: String, query: String, returns: String, result: Projection) {
    writer.println("[[" + nicefy(section + " " + title) + "]]")
    writer.println("== " + title + " ==")
    writer.println()
    writer.println("_Query_")
    writer.println("[source]")
    writer.println("----")
    writer.println(query)
    writer.println("----")
    writer.println()
    writer.println(returns)
    writer.println("_Result_")
    writer.println()
    writer.println("[source]")
    writer.println("----")
    writer.println(result.toString())
    writer.println("----")
    writer.flush()
    writer.close()
  }

  def testQuery(title: String, query: String, returns: String, assertions: ( ( Projection ) => Unit )*) {
    val q = parser.parse(query)
    val result = engine.execute(q)
    assertions.foreach(_.apply(result))


    val dir = new File("target/docs/" + nicefy(section))
    if ( !dir.exists() ) {
      dir.mkdirs()
    }

    val writer = new PrintWriter(new FileWriter(new File(dir, nicefy(title)+".txt")))

    dumpToFile(writer, title, query, returns, result)

  }

  @Before def init() {
    db = new ImpermanentGraphDatabase()
    engine = new ExecutionEngine(db)
    nodeIndex = db.index().forNodes("Person")
    val description = GraphDescription.create(graphDescription: _*)
    nodes = description.create(db).asScala.toMap
  }
}