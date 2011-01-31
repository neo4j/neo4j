package org.neo4j.release.it.std.smoke

import org.specs._

import org.neo4j.release.it.std.io.FileHelper._
import java.io.File
import org.neo4j.kernel.{KernelExtension, EmbeddedGraphDatabase}
import org.neo4j.ext.udc.impl.UdcExtensionImpl
import scala.reflect.ClassManifest.fromClass
/**
 * Integration smoketest for the neo4j-udc component.
 *
 */
class UdcSpec extends SpecificationWithJUnit {

  "neo4j udc" should {

    val dbname = "udcdb"
    var graphdb:EmbeddedGraphDatabase = null

    doBefore {
      graphdb = new EmbeddedGraphDatabase( dbname )
    }
    doAfter {
      graphdb.shutdown
      new File(dbname).deleteAll
    }

    "resolve as a kernel extension" in {
      val udc = new UdcExtensionImpl

      udc must haveSuperClass(fromClass(classOf[KernelExtension]))
    }

    "load as a graphdb ext" in {
      // ABK: should kernel expose extensions in an api?
    }

  }
}