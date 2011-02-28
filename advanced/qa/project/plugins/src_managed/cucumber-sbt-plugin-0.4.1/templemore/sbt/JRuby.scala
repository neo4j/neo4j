package templemore.sbt

import _root_.sbt._
import java.io.File

/**
 * @author Chris Turner
 */
class JRuby(val classpath: PathFinder,
            val scalaLibraryPath: Path,
            val defaultArgs: List[String],
            val jRubyHome: Path,
            val gemPath: Path,
            val maxMemory: String,
            val maxPermGen: String,
            val log: Logger) {

  if ( !jRubyHome.exists ) jRubyHome.asFile.mkdirs

  def apply(args: List[String]): Int = {
    log.debug("Launching JRuby")
    log.debug("classpath: " + classpathAsString)
    log.debug("javaArgs: " + defaultArgs)
    log.debug("args: " + args)
    log.debug("jRubyHome: " + jRubyHome)
    log.debug("gemPath: " + gemPath)

    Fork.java(None, javaArgs ++ args, None, jRubyEnv, LoggedOutput(log))
  }

  def installGem(gem:String) = {
    apply(List("-S", "gem", "install", "--no-ri", "--no-rdoc", "--install-dir", gemPath.absolutePath) ++ gem.split("\\s+"))
  }

  private def classpathAsString =
    (scalaLibraryPath.asFile :: classpath.getFiles.toList).map(_.getAbsolutePath).mkString(File.pathSeparator)
  private def javaArgs = defaultArgs ++ ("-Xmx%s".format(maxMemory) :: "-XX:MaxPermSize=%s".format(maxPermGen) ::
                                         "-classpath" :: classpathAsString :: "org.jruby.Main" :: Nil)
  private def jRubyEnv = Map("GEM_PATH" -> gemPath.absolutePath,
                             "HOME" -> jRubyHome.absolutePath)
}
