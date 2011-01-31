package org.neo4j.release.it.std.io

import java.io._
import org.specs.SpecificationWithJUnit

class FileHelper(file: File) {
  def write(text: String): Unit = {
    val fw = new FileWriter(file)
    try {fw.write(text)}
    finally {fw.close}
  }

  def foreachLine(proc: String => Unit): Unit = {
    val br = new BufferedReader(new FileReader(file))
    try {while (br.ready) proc(br.readLine)}
    finally {br.close}
  }

  def deleteAll: Unit = {
    def deleteFile(dfile: File): Unit = {
      if (dfile.isDirectory)
        dfile.listFiles.foreach {f => deleteFile(f)}
      dfile.delete
    }
    deleteFile(file)
  }
}
object FileHelper {
  implicit def file2helper(file: File) = new FileHelper(file)
}

class FileHelperSpec extends SpecificationWithJUnit {
  import FileHelper._

  "FileHelper" should {
    "create some tmp dirs and files" in {
      val pdir = new File("/tmp/mydir")
      val dir = new File(pdir, "nested_dir")
      dir.mkdirs
      val file = new File(dir, "myfile.txt")
      file.write("one\ntwo\nthree")
      file.foreachLine {line => println(">> " + line)}
      pdir.deleteAll
    }
  }
}


