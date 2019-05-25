/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import java.io.{File, FileInputStream}
import java.net.URL
import java.util.Optional

import org.apache.commons.io.IOUtils
import org.neo4j.blob.{BlobFactory, Blob}
import org.neo4j.driver._
import org.neo4j.server.CommunityBootstrapper
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

class BoltCypherTest extends FunSuite with BeforeAndAfter with TestBase {

  val server = new CommunityBootstrapper();

  before {
    setupNewDatabase(new File("./testdata/testdb/data/databases/graph.db"));

    server.start(testDbDir, Optional.of(new File(testConfPath)),
      JavaConversions.mapAsJavaMap(Map("config.file.path" -> new File(testConfPath).getAbsolutePath)));
  }

  after {
    server.stop()
  }

  test("test blob R/W via cypher") {
    val conn = new CypherConnection("bolt://localhost:7687");
    //a non-blob
    val (node, name, age) = conn.querySingleObject("match (n) where n.name='bob' return n, n.name, n.age", (result: Record) => {
      (result.get("n").asNode(), result.get("n.name").asString(), result.get("n.age").asInt())
    });

    assert("bob" === name);
    assert(40 == age);

    val nodes = conn.queryObjects("match (n) return n", (result: Record) => {
      result.get("n").asNode()
    });

    assert(2 == nodes.length);

    //blob
    val blob0 = conn.querySingleObject("return Blob.empty()", (result: Record) => {
      result.get(0).asBlob
    });

    assert(0 == blob0.length);

    conn.querySingleObject("return Blob.fromFile('./testdata/test.png')", (result: Record) => {
      val blob1 = result.get(0).asBlob
      assert(new File("./testdata/test.png").length() == blob1.toBytes().length);
      blob1.offerStream(is => {
        //remote input stream should be closed
        is.read();
      })
      assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png"))) ===
        blob1.toBytes());
      1;
    });

    var blob20: Blob = null;

    conn.querySingleObject("match (n) where n.name='bob' return n.photo,n.album,Blob.len(n.photo) as len", (result: Record) => {
      val blob2 = result.get("n.photo").asBlob;
      blob20 = blob2;
      val album = result.get("n.album").asList();
      val len = result.get("len").asInt()

      assert(len == new File("./testdata/test.png").length());

      assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png"))) ===
        blob2.offerStream {
          IOUtils.toByteArray(_)
        });

      assert(6 == album.size());

      assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png"))) ===
        album.get(0).asInstanceOf[Blob].offerStream {
          IOUtils.toByteArray(_)
        });
      assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png"))) ===
        album.get(5).asInstanceOf[Blob].offerStream {
          IOUtils.toByteArray(_)
        });
    });

    //now, blob is unaccessible
    val ex =
      try {
        blob20.offerStream {
          IOUtils.toByteArray(_)
        };

        false;
      }
      catch {
        case _ => true;
      }

    assert(ex);

    conn.querySingleObject("match (n) where n.name='alex' return n.photo", (result: Record) => {
      val blob3 = result.get("n.photo").asBlob
      assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test1.png"))) ===
        blob3.offerStream {
          IOUtils.toByteArray(_)
        });
    });

    //query with parameters
    val blob4 = conn.querySingleObject("match (n) where n.name={NAME} return n.photo",
      Map("NAME" -> "bob"), (result: Record) => {
        result.get("n.photo").asBlob
      });

    //commit new records
    conn.executeUpdate("CREATE (n {name:{NAME}})",
      Map("NAME" -> "张三"));

    conn.executeUpdate("CREATE (n {name:{NAME}, photo:{BLOB_OBJECT}})",
      Map("NAME" -> "张三", "BLOB_OBJECT" -> BlobFactory.EMPTY));

    conn.executeUpdate("CREATE (n {name:{NAME}, photo:{BLOB_OBJECT}})",
      Map("NAME" -> "张三", "BLOB_OBJECT" -> BlobFactory.fromFile(new File("./testdata/test1.png"))));

    conn.executeQuery("return {BLOB_OBJECT}",
      Map("BLOB_OBJECT" -> BlobFactory.fromFile(new File("./testdata/test.png"))));

    conn.querySingleObject("return {BLOB_OBJECT}",
      Map("BLOB_OBJECT" -> BlobFactory.fromFile(new File("./testdata/test.png"))), (result: Record) => {
        val blob = result.get(0).asBlob

        assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png"))) ===
          blob.offerStream {
            IOUtils.toByteArray(_)
          });

      });
  }

  test("test blob R/W via blob literal") {
    val conn = new CypherConnection("bolt://localhost:7687");

    //blob
    val blob0 = conn.querySingleObject("return <base64://>", (result: Record) => {
      result.get(0).asBlob
    });

    assert(0 == blob0.length);

    //blob
    val blob01 = conn.querySingleObject("return <base64://dGhpcyBpcyBhIGV4YW1wbGU=>", (result: Record) => {
      result.get(0).asBlob
    });

    assert("this is a example".getBytes() === blob01.toBytes());

    //test localfile
    conn.querySingleObject("return <file://./testdata/test.png>", (result: Record) => {
      val blob1 = result.get(0).asBlob

      assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png"))) ===
        blob1.toBytes());
    });

    //test http
    conn.querySingleObject("return <http://img.zcool.cn/community/049f6b5674911500000130b7f00a87.jpg>", (result: Record) => {
      val blob2 = result.get(0).asBlob

      assert(IOUtils.toByteArray(new URL("http://img.zcool.cn/community/049f6b5674911500000130b7f00a87.jpg")) ===
        blob2.toBytes());
    });

    //test https
    val blob3 = conn.querySingleObject("return <https://avatars0.githubusercontent.com/u/2328905?s=460&v=4>", (result: Record) => {
      result.get(0).asBlob.toBytes()
    });

    assert(IOUtils.toByteArray(new URL("https://avatars0.githubusercontent.com/u/2328905?s=460&v=4")) ===
      blob3);

    assert(conn.querySingleObject("return Blob.len(<file://./testdata/test.png>)", (result: Record) => {
      result.get(0).asInt
    }) == new File("./testdata/test.png").length());
  }
}

class CypherConnection(url: String, user: String = "", pass: String = "")
  extends org.apache.logging.log4j.scala.Logging {

  lazy val _driver = GraphDatabase.driver(url, AuthTokens.basic(user, pass));

  def execute[T](f: (Session) => T): T = {
    val session = _driver.session();
    val result = f(session);
    session.close();
    result;
  }

  def queryObjects[T: ClassTag](queryString: String, fnMap: (Record => T)): Iterator[T] = {
    executeQuery(queryString, (result: StatementResult) => {
      result.map(fnMap)
    });
  }

  final def executeQuery[T](queryString: String, params: Map[String, AnyRef]): Unit =
    executeQuery(queryString, params, (StatementResult) => {
      null.asInstanceOf[T]
    })

  final def querySingleObject[T](queryString: String, fnMap: (Record => T)): T = {
    executeQuery(queryString, (rs: StatementResult) => {
      fnMap(rs.next());
    });
  }

  final def querySingleObject[T](queryString: String, params: Map[String, AnyRef], fnMap: (Record => T)): T = {
    executeQuery(queryString, params, (rs: StatementResult) => {
      fnMap(rs.next());
    });
  }

  def executeUpdate(queryString: String) = {
    _executeUpdate(queryString, None);
  }

  def executeUpdate(queryString: String, params: Map[String, AnyRef]) = {
    _executeUpdate(queryString, Some(params));
  }

  def executeQuery[T](queryString: String, fn: (StatementResult => T)): T = {
    _executeQuery(queryString, None, fn);
  }

  def executeQuery[T](queryString: String, params: Map[String, AnyRef], fn: (StatementResult => T)): T = {
    _executeQuery(queryString, Some(params), fn);
  }

  private def _executeUpdate[T](queryString: String, optParams: Option[Map[String, AnyRef]]): Unit = {
    execute((session: Session) => {
      logger.debug(s"execute update: $queryString");
      session.writeTransaction(new TransactionWork[T] {
        override def execute(tx: Transaction): T = {
          if (optParams.isDefined)
            tx.run(queryString, JavaConversions.mapAsJavaMap(optParams.get));
          else
            tx.run(queryString);

          null.asInstanceOf[T];
        }
      });
    });
  }

  private def _executeQuery[T](queryString: String, optParams: Option[Map[String, AnyRef]], fn: (StatementResult => T)): T = {
    execute((session: Session) => {
      logger.debug(s"execute query: $queryString");
      session.readTransaction(new TransactionWork[T] {
        override def execute(tx: Transaction): T = {
          val result = if (optParams.isDefined)
            tx.run(queryString, JavaConversions.mapAsJavaMap(optParams.get));
          else
            tx.run(queryString);

          fn(result);
        }
      });
    });
  }
}
