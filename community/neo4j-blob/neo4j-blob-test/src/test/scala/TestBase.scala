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

import java.io.File

import org.apache.commons.io.FileUtils
import org.neo4j.blob.BlobFactory
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory

/**
  * Created by bluejoe on 2019/4/13.
  */
trait TestBase {
  val testDbDir = new File("./testdata/testdb");
  val testConfPath = new File("./testdata/neo4j.conf").getPath;

  def setupNewDatabase(dbdir: File = testDbDir, conf: String = testConfPath): Unit = {
    FileUtils.deleteDirectory(dbdir);
    //create a new database
    val db = openDatabase(dbdir, conf);
    val tx = db.beginTx();
    //create a node
    val node1 = db.createNode();

    node1.setProperty("name", "bob");
    node1.setProperty("age", 40);

    //with a blob property
    node1.setProperty("photo", BlobFactory.fromFile(new File("./testdata/test.png")));
    //blob array
    node1.setProperty("album", (0 to 5).map(x => BlobFactory.fromFile(new File("./testdata/test.png"))).toArray);

    val node2 = db.createNode();
    node2.setProperty("name", "alex");
    //with a blob property
    node2.setProperty("photo", BlobFactory.fromFile(new File("./testdata/test1.png")));
    node2.setProperty("age", 10);

    //node2.createRelationshipTo(node1, RelationshipType.withName("dad"));

    tx.success();
    tx.close();
    db.shutdown();
  }

  def openDatabase(dbdir: File = testDbDir, conf: String = testConfPath): GraphDatabaseService = {
    val builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbdir);
    builder.loadPropertiesFromFile(conf);
    //bolt server is not required
    builder.setConfig("dbms.connector.bolt.enabled", "false");
    builder.newGraphDatabase();
  }
}
