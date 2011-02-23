import org.gradle.api.Project
import org.gradle.api.Plugin
import org.gradle.api.plugins.JavaPlugin

class GenerateVersionClass implements Plugin<Project> {

    def void apply(Project project) {
        project.plugins.apply(JavaPlugin.class)
        def generatedSrcDir = new File(project.buildDir, 'generated-src/version/java')

        def makeVersionClassTask = project.task('makeVersionClass') << {


            Process git1 = "git fetch --tags".execute([], project.projectDir)
            git1.waitFor()
            if (git1.exitValue() != 0) throw new RuntimeException("unable update tags 'git fetch --tags' " + git1.err.text);

            Process git2 = "git describe".execute([], project.projectDir)
            git2.waitFor()
            if (git2.exitValue() != 0) throw new RuntimeException("unable fetch 'git describe' " + git2.err.text);
            def gitVersion = git2.text.readLines()[0];

            println "Version: ${project.name}, ${project.version}, $gitVersion"

            def packageName = "org.neo4j.kernel.impl"
            def outFilename = packageName.replace('.', '/') + "/ComponentVersion.java"
            def outFile = new File(generatedSrcDir, outFilename)
            outFile.parentFile.mkdirs()
            def f = new FileWriter(outFile)
            f.write("""package  $packageName;
import org.neo4j.kernel.Version;
import org.neo4j.helpers.Service;

@Service.Implementation(Version.class) public class ComponentVersion extends Version {
    public ComponentVersion() { super(KERNEL_ARTIFACT_ID, "${project.version}"); }
    public String getRevision() { return "$gitVersion"; }
}
""")
            f.close()
        }

        project.sourceSets.main.java.srcDir generatedSrcDir

        makeVersionClassTask.inputs.files(project.sourceSets.main.allSource)
        makeVersionClassTask.outputs.files(generatedSrcDir)

        if (project.buildFile != null && project.buildFile.exists()) {
            makeVersionClassTask.inputs.files(project.buildFile)
        }
        project.tasks.getByName('compileJava').dependsOn('makeVersionClass')
    }
}