package org.neo4j.qa.util

import org.apache.commons.vfs.{AllFileSelector, FileUtil, FileObject, VFS}
import org.apache.commons.io.IOUtils
import java.io.{FileNotFoundException, FileOutputStream, File}

object ArchiveHelper
{
  val fsManager = VFS.getManager();

  val CompressedTarballRE = """.*\.(tgz|tar.gz)""".r

  def unarchive(archive: String, toDirectory: String): Either[Exception, File] =
  {
    archive match
    {
      case CompressedTarballRE(_) => untar(archive, toDirectory)
      case _ => Left(new UnsupportedOperationException("no support for unarchiving \"" + archive + "\""))
    }
  }

  def untar(archivePath: String, toDirectory: String): Either[Exception, File] =
  {
    val archive = new File(archivePath)
    if ( archive.exists )
    {
      val destdir = new File(toDirectory)
      if ( !destdir.exists ) destdir.mkdirs
      if ( destdir.isDirectory )
      {
        Platform.current match
        {
          case Platform.Unix =>
          {
            val process = Runtime.getRuntime.exec("tar xvzf " + archive.getAbsolutePath + " --strip-components 1", Array[String](), destdir)
            IOUtils.copy(process.getErrorStream, System.out)
            IOUtils.copy(process.getInputStream, System.out)
            process.waitFor
            if ( process.exitValue == 0 ) Right(destdir)
            else (Left(new RuntimeException("Untar failed with exit code \"" + process + "\". Check stderr for details.")))
          }
          case Platform.Windows => Left(new UnsupportedOperationException("untarring on windows is not supported"))
        }
      }
      else
      {
        Left(new IllegalArgumentException("Destination directory " + toDirectory + " exists and is not a directotry."))
      }
    }
    else
    {
      Left(new FileNotFoundException(archive.getAbsolutePath))
    }
  }

  def resolve(archive: String): Either[Exception, FileObject] = archive match
  {
    case CompressedTarballRE(_) =>
    {
      val archiveFO = fsManager.resolveFile("gz:/Users/akollegger/Developer/neo/githubs/server/qa/neo4j-1.3-SNAPSHOT-unix.tar.gz").getChildren.head
      val tmpUncompressed = File.createTempFile("archive", "tar")
      IOUtils.copy(archiveFO.getContent.getInputStream, new FileOutputStream(tmpUncompressed))
      val tarFO = fsManager.resolveFile("tar:file:" + tmpUncompressed.getAbsolutePath)
      Right(tarFO)
    }
    case _ => Left(new IllegalArgumentException("No support for un-archiving \"" + archive + "\""))
  }

}

