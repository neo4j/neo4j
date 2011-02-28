package neo4j.qa.util

import java.io.File
import org.apache.commons.vfs.{AllFileSelector, FileUtil, FileObject, VFS}

object ArchiveHelper
{
  val fsManager = VFS.getManager();

  val CompressedTarballRE = """.*\.(tgz|tar.gz)""".r

  def unarchive(archive: String, toDirectory: String): Either[Exception, File] =
  {
    val absoluteArchivePath = new File(archive).getAbsolutePath
    resolve(absoluteArchivePath) match {
      case Left(l) => Left(l)
      case Right(archiveFO) => {
        val destinationFO = fsManager.resolveFile("file:" + new File(toDirectory).getAbsolutePath)
        destinationFO.createFolder
        destinationFO.copyFrom(archiveFO, new AllFileSelector())
        Right(new File(toDirectory))
      }
    }
  }

  def resolve(archive: String): Either[Exception, FileObject] = archive match
  {
    case CompressedTarballRE(_) => Right(fsManager.resolveFile("tgz:file:" + archive))
    case _ => Left(new IllegalArgumentException("No support for un-archiving \"" + archive + "\""))
  }

}

