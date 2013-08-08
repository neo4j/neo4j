Windows installer project
-------------------------

Builds windows installers for all neo4j server editions. 

The actual installer is built with AdvancedInstaller (http://www.advancedinstaller.com/). 
That means you need to run this project on windows, and you need AdvancedInstaller installed.
The project expects AdvancedInstaller to be installed in C:\Program Files\Caphyon\Advanced Installer 8.6\bin\x86\AdvancedInstaller.com, but you can change that by setting the "ai.executable" maven property.

To build:

  mvn clean package

Installers will be located under target/


Where does it find the standalone resources?
--------------------------------------------

This project expects windows standalone artifacts, as produced by the standalone project,
to be located under /target.

To help development, there is a profile, active by default, that will pull in these artifacts
from the standalone project in ../standalone/target. To deactivate that and provide these
artifacts "externally" (eg. via a build system), invoke the build like so:

  mvn clean package -DpullArtifacts=false


Working on this project
-----------------------

Installer config is done through the "installer.commands" file, located in src/main/resources
See http://www.advancedinstaller.com/user-guide/command-line-editing.html for available commands.

More complex changes than what is supported via commands must be done via the AdvancedInstaller GUI.
The AdvancedInstaller project is located at src/main/resources/installer.ai.

Please see the ANT script in pom.xml for how the installer.ai project is modified and executed by the installer.commands file.
