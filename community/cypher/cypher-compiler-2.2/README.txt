This is an internal module used by the Cypher module. Users should
not use this module directly.

Working with the source code in Eclipse IDE
===========================================

Note: these instructions are for the Eclipse Indigo (3.7) release.
For the previous release (Helios, 3.6) simply install m2eclipse
and ScalaIDE and that's it.

These are the necessary plug-ins:

Eclipse Maven integration:
It's included in the Java package, for other packages see:
http://www.eclipse.org/m2e/

ScalaIDE for Eclipse, version using Scala 2.9.0-1:
Update site: http://download.scala-ide.org/releases-29/2.0.0-beta
For other options see:
http://download.scala-ide.org/
Note: Avoid installing JDT Weaving in case you already have another version
installed (for example through SpringSource Tool Suite).

M2E Scala integration:
Update site: http://alchim31.free.fr/m2e-scala/update-site/
For more information:
https://www.assembla.com/wiki/show/scala-ide/With_M2Eclipse

There may be other M2E integrations needed, but M2E should be able to find
and install these for you.

To get the Cypher project working in Eclipse (with the above plugins installed):
* Delete the project if it was already imported into the workbench.
* Delete files/directories like .project, .classpath, .settings/
* Use File -> Import... -> Existing Maven Projects to import the project.
* After the import has finished, right-click on the project and
  -> Maven -> Update Project Configuration...
