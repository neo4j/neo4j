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
import org.neo4j.graphdb.{GraphDatabaseService, Node, Transaction}
import org.neo4j.kernel.impl.InstanceContext
import org.neo4j.kernel.impl.blob.BlobStorage
import org.neo4j.blob.{BlobFactory, Blob}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by bluejoe on 2019/4/11.
  */
class MultiDbBlobTest extends FunSuite with BeforeAndAfter with TestBase {

  before {
    setupNewDatabase();
  }

  def testCreateBlob(db: GraphDatabaseService, name: String, photo: File): Transaction = {
    val tx = db.beginTx();
    //create a node
    val node1 = db.createNode();
    node1.setProperty("name", name);
    //with a blob property
    node1.setProperty("photo", BlobFactory.fromFile(photo));

    tx;
  }

  def testQuery(db: GraphDatabaseService, file: File): Unit = {
    val tx = db.beginTx();

    //get first node
    val it = db.getAllNodes().iterator();
    val v1: Node = it.next();

    assert(false == it.hasNext);
    assert(3 == v1.getAllProperties.size());

    val blob = v1.getProperty("photo").asInstanceOf[Blob];
    assert(IOUtils.toByteArray(new FileInputStream(file)) ===
      blob.toBytes());

    tx.success();
    tx.close();
  }

  test("test multiple db transaction") {
    val db = openDatabase();
    assert(InstanceContext.of(db).get[BlobStorage].iterator().size == 8);

    val tx1 = testCreateBlob(db, "lawson", new File("./testdata/test.png"));
    val tx2 = testCreateBlob(db, "alex", new File("./testdata/test1.png"));

    tx2.success();
    tx2.close();

    assert(InstanceContext.of(db).get[BlobStorage].iterator().size == 8);

    tx1.success();
    tx1.close();

    //top level transaction commit
    assert(InstanceContext.of(db).get[BlobStorage].iterator().size == 10);

    db.shutdown()
  }

  test("test multiple db instances") {
    val testDbDir1 = new File("./testdata/testdb1/db");
    val testDbDir2 = new File("./testdata/testdb2/db");

    setupNewDatabase(testDbDir1);
    setupNewDatabase(testDbDir2);

    val db1 = openDatabase(testDbDir1);
    val db2 = openDatabase(testDbDir2);

    assert(InstanceContext.of(db1).get[BlobStorage].iterator().size == 8);
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 8);

    val tx1 = testCreateBlob(db1, "lawson", new File("./testdata/test.png"));
    val tx2 = testCreateBlob(db2, "alex", new File("./testdata/test1.png"));

    tx2.success();
    tx2.close();

    assert(InstanceContext.of(db1).get[BlobStorage].iterator().size == 8);
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 9);

    tx1.success();
    tx1.close();

    assert(InstanceContext.of(db1).get[BlobStorage].iterator().size == 9);
    assert(InstanceContext.of(db2).get[BlobStorage].iterator().size == 9);

    db1.shutdown()
    db2.shutdown()
  }
}