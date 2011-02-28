package templemore.sbt

/**
 * @author Chris Turner
 */
case class Gem(name: String, version: String, source: String) {

  def command = "%s --version %s --source %s".format(name, version, source)
}