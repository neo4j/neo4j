import org.gradle.api.Project
import org.gradle.api.Plugin
import org.gradle.api.plugins.JavaPlugin

class GenerateVersionClass implements Plugin<Project> {

    def void apply(Project project) {
        project.plugins.apply(JavaPlugin.class)
        def genSrc = 'generated-src/version'
        def generatedSrcDir = new File(project.buildDir, genSrc)

        def makeVersionClassTask = project.task('makeVersionClass') << {


            Process git = "git describe".execute([], project.projectDir)
            git.waitFor()
            if (git.exitValue() != 0) throw new RuntimeException("unable fetch 'git describe' " + git.err.text);
            def gitVersion = git.text.readLines()[0];

            println "Version: ${project.name}, ${project.version}, $gitVersion"

            def outFilename = "java/" + project.group.replace('.', '/') + "/" + project.name + "/impl/ComponentVersion.java"
            def outFile = new File(generatedSrcDir, outFilename)
            outFile.parentFile.mkdirs()
            def f = new FileWriter(outFile)
            f.write("""package  ${project.group}.${project.name}.impl;
import org.neo4j.kernel.Version;
import org.neo4j.helpers.Service;

@Service.Implementation(Version.class) public class ComponentVersion extends Version {
    public ComponentVersion() { super("${project.name}", "${project.version}"); }
    public String getRevision() { return "$gitVersion"; }
}
""")
            f.close()
        }

        project.sourceSets.main.java.srcDir project.buildDir.name + '/' + genSrc + '/java'


        makeVersionClassTask.inputs.files(project.sourceSets.main.allSource)
        makeVersionClassTask.outputs.files(generatedSrcDir)

        if (project.buildFile != null && project.buildFile.exists()) {
            makeVersionClassTask.inputs.files(project.buildFile)
        }
        project.tasks.getByName('compileJava').dependsOn('makeVersionClass')
    }
}