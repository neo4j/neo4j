package org.neo4j.release.it.std.exec;

import org.junit.Test;
import org.ops4j.pax.runner.platform.DefaultJavaRunner;
import org.ops4j.pax.runner.platform.PlatformException;

import java.io.File;
import java.util.Properties;

/**
 *
 */
public class PaxRunnerTest {

    @Test
    public void shouldRunJava() throws PlatformException {
        boolean waitForExit = true;
        DefaultJavaRunner runner = new DefaultJavaRunner( waitForExit );

        String[] vmOptions = { "-server" };
        String[] classpath = System.getProperty("java.class.path").split(":");
        String mainClass = "org.neo4j.release.it.std.exec.HelloWorldApp";
        String[] programOptions = null;
        String javaHome = System.getProperty("java.home");
        File workingDir = null; // new File(".");

        dumpProperties(System.getProperties());
        
        runner.exec(vmOptions, classpath, mainClass, programOptions, javaHome, workingDir);

    }

    private void dumpProperties(Properties properties) {
        System.out.println("System Properties...");
        for (Object key : properties.keySet()) {
            System.out.println("\t" + key + " = " + properties.getProperty((String) key));
        }
    }
}
