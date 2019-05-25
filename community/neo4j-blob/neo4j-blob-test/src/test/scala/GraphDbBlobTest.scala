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

import org.apache.commons.io.IOUtils
import org.neo4j.graphdb.Node
import org.neo4j.kernel.impl.InstanceContext
import org.neo4j.kernel.impl.blob.BlobStorage
import org.neo4j.blob.{BlobFactory, Blob}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.JavaConversions

class GraphDbBlobTest extends FunSuite with BeforeAndAfter with TestBase {

  before {
    setupNewDatabase();
  }

  test("test blob R/W using API") {
    //reload database
    val db2 = openDatabase();
    val tx2 = db2.beginTx();
    //get first node
    val it = db2.getAllNodes().iterator();
    val v1: Node = it.next();
    val v2: Node = it.next();

    println(v1.getAllProperties);
    assert(4 == v1.getAllProperties.size());

    val blob = v1.getProperty("photo").asInstanceOf[Blob];

    assert(new File("./testdata/test.png").length() == blob.length);

    assert(new File("./testdata/test.png").length() == blob.offerStream {
      IOUtils.toByteArray(_).length
    });

    assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png"))) ===
      blob.toBytes());

    //test array[blob]
    val blob2 = v1.getProperty("album").asInstanceOf[Array[Blob]];
    assert(6 == blob2.length);

    assert((0 to 5).toArray.map { x =>
      IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png")))
    }
      ===
      blob2.map {
        _.offerStream {
          IOUtils.toByteArray(_)
        }
      });

    val blob3 = v2.getProperty("photo").asInstanceOf[Blob];
    assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test1.png"))) ===
      blob3.toBytes());

    tx2.close();
    db2.shutdown();
  }

  test("remove a blob property") {
    val db2 = openDatabase();
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 8);

    val tx2 = db2.beginTx();

    //get first node
    val it = db2.getAllNodes().iterator();
    val v1: Node = it.next();
    //delete one
    v1.removeProperty("photo");

    //should not be deleted now
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 8);

    tx2.success();
    tx2.close();
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 7);
    db2.shutdown();
  }

  test("remove a blob array property") {
    val db2 = openDatabase();
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 8);

    val tx2 = db2.beginTx();

    //get first node
    val it = db2.getAllNodes().iterator();
    val v1: Node = it.next();
    v1.removeProperty("album");
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 8);
    tx2.success();
    tx2.close();
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 2);
    db2.shutdown();
  }

  test("set a blob property to other") {
    val db2 = openDatabase();
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 8);

    val tx2 = db2.beginTx();

    //get first node
    val it = db2.getAllNodes().iterator();
    val v1: Node = it.next();

    v1.setProperty("album", 1);
    v1.setProperty("photo", 1);

    tx2.success();
    tx2.close();
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 1);

    db2.shutdown();
  }

  test("remove a record with blob properties") {
    val db2 = openDatabase();
    val tx2 = db2.beginTx();

    //get first node
    val it = db2.getAllNodes().iterator();
    val v1: Node = it.next();

    v1.delete();

    tx2.success();
    tx2.close();
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 1);

    db2.shutdown();
  }

  test("test blob using Cypher query") {
    //reload database
    val db2 = openDatabase();
    val tx2 = db2.beginTx();

    //cypher query
    val r1 = db2.execute("match (n) where n.name='bob' return n.photo,n.name,n.age,n.album").next();
    assert("bob" === r1.get("n.name"));
    assert(40 == r1.get("n.age"));

    val blob22 = r1.get("n.album").asInstanceOf[Array[Blob]];
    assert(6 == blob22.length);
    assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png"))) ===
      blob22(0).toBytes());

    assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png"))) ===
      blob22(5).toBytes());

    val blob1 = r1.get("n.photo").asInstanceOf[Blob];

    assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test.png"))) ===
      blob1.toBytes());

    val blob3 = db2.execute("match (n) where n.name='alex' return n.photo").next()
      .get("n.photo").asInstanceOf[Blob];

    assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test1.png"))) ===
      blob3.toBytes());

    tx2.success();
    tx2.close();
    db2.shutdown();
  }

  test("test blob using Cypher create") {
    //reload database
    val db2 = openDatabase();
    val tx2 = db2.beginTx();

    db2.execute("CREATE (n {name:{NAME}})",
      JavaConversions.mapAsJavaMap(Map("NAME" -> "张三")));

    db2.execute("CREATE (n {name:{NAME}, photo:{BLOB_OBJECT}})",
      JavaConversions.mapAsJavaMap(Map("NAME" -> "张三", "BLOB_OBJECT" -> BlobFactory.EMPTY)));

    db2.execute("CREATE (n {name:{NAME}, photo:{BLOB_OBJECT}})",
      JavaConversions.mapAsJavaMap(Map("NAME" -> "张三", "BLOB_OBJECT" -> BlobFactory.fromFile(new File("./testdata/test1.png")))));

    assert(3.toLong === db2.execute("match (n) where n.name=$NAME return count(n)",
      JavaConversions.mapAsJavaMap(Map("NAME" -> "张三"))).next().get("count(n)"));

    val it2 = db2.execute("match (n) where n.name=$NAME return n.photo",
      JavaConversions.mapAsJavaMap(Map("NAME" -> "张三")));

    assert(null ==
      it2.next().get("n.photo"));

    assert(it2.next().get("n.photo") === BlobFactory.EMPTY);

    assert(IOUtils.toByteArray(new FileInputStream(new File("./testdata/test1.png"))) ===
      it2.next().get("n.photo").asInstanceOf[Blob].toBytes());

    tx2.success();
    tx2.close();
    db2.shutdown();
  }
}
