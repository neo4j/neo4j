import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val templemoreRepo = "templemore repo" at "http://templemore.co.uk/repo"
  val cucumberPlugin = "templemore" % "cucumber-sbt-plugin" % "0.4.1"
}
