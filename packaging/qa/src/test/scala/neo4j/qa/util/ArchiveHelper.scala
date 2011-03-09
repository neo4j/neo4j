package neo4j.qa.util

import org.apache.commons.vfs.{AllFileSelector, FileUtil, FileObject, VFS}
import org.apache.commons.io.IOUtils
import java.io.{FileOutputStream, File}

object ArchiveHelper
{
  val fsManager = VFS.getManager();

  val CompressedTarballRE = """.*\.(tgz|tar.gz)""".r

  def unarchive(archive: String, toDirectory: String): Either[Exception, File] =
  {
    val p = Runtime.getRuntime.exec("tar xvzf " + archive + "")
    IOUtils.copy(p.getErrorStream, System.out)
    IOUtils.copy(p.getInputStream, System.out)

    Left(new Exception("not really a problem"))
//    val absoluteArchivePath = new File(archive).getAbsolutePath
//    resolve(absoluteArchivePath) match {
//      case Left(l) => Left(l)
//      case Right(archiveFO) => {
//        val destinationFO = fsManager.resolveFile("file:" + new File(toDirectory).getAbsolutePath)
//        destinationFO.createFolder
//        destinationFO.copyFrom(archiveFO, new AllFileSelector())
//        Right(new File(toDirectory))
//      }
//    }
  }

  def resolve(archive: String): Either[Exception, FileObject] = archive match
  {
    case CompressedTarballRE(_) => {
      val archiveFO = fsManager.resolveFile("gz:/Users/akollegger/Developer/neo/githubs/server/qa/neo4j-1.3-SNAPSHOT-unix.tar.gz").getChildren.head
      val tmpUncompressed = File.createTempFile("archive", "tar")
      IOUtils.copy(archiveFO.getContent.getInputStream, new FileOutputStream(tmpUncompressed))
      val tarFO = fsManager.resolveFile("tar:file:" + tmpUncompressed.getAbsolutePath)
      Right(tarFO)
    }
    case _ => Left(new IllegalArgumentException("No support for un-archiving \"" + archive + "\""))
  }

}

