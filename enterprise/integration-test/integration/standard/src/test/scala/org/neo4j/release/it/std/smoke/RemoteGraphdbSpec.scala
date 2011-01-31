package org.neo4j.release.it.std.smoke

import org.specs._

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.remote.RemoteGraphDatabase;

/**
 * Integration smoketest for the neo4j-remote-graphdb component.
 *
 */
class RemoteGraphdbSpec extends SpecificationWithJUnit {

  val RESOURCE_URI = "rmi://rmi-server/neo4j-graphdb"

  "neo4j remote graphdb" should {

    "attach to a remote graph database" in {
      skip("needs to start RMI, spawn neo4j server in a jvm.")
      // TODO: implement this integration test
    }

  }

}