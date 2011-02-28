import sbt._
import templemore.sbt.CucumberProject

class ServerQaProject(info: ProjectInfo) extends DefaultProject(info) with CucumberProject {

  //
  // Dependencies
  //
  val scalatest = "org.scalatest" % "scalatest" % "1.3" % "test"
  val dispatch = "net.databinder" %% "dispatch" % "0.7.8"
  val commons_compress = "org.apache.commons" % "commons-compress" % "1.1"
  val commons_io = "commons-io" % "commons-io" % "2.0.1"

  // include any script language steps defined in the usual place
  def dynamicStepsDirectory = featuresDirectory / "step_definitions"
  override def extraCucumberOptions = List("--require", dynamicStepsDirectory.absolutePath)
  

}
