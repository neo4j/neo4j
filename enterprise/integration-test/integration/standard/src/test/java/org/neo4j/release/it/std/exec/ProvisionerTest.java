package org.neo4j.release.it.std.exec;

import org.junit.Test;
import org.ops4j.io.FileUtils;
import org.ops4j.pax.exam.options.MavenArtifactProvisionOption;
import org.ops4j.pax.exam.options.ProvisionOption;
import org.ops4j.pax.runner.platform.DefaultJavaRunner;
import org.ops4j.pax.runner.platform.PlatformException;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Is.is;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.release.it.std.exec.HasItemEndingIn.hasItemEndingIn;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;

/**
 * Unit tests for the Provisioner.
 */
public class ProvisionerTest {


    @Test
    public void shouldCreateProvisioner() {
        Provisioner prover = new Provisioner();

        assertThat(prover, notNullValue());
    }

    @Test
    public void shouldCreateProvisionDirectory() {
        final String EXPECTED_DIRECTORY = "prove_dir";

        Provisioner prover = new Provisioner();
        prover.setProvisionDirectory( EXPECTED_DIRECTORY );
        prover.provision();

        File actualDirectory = new File(EXPECTED_DIRECTORY);
        assertTrue(actualDirectory.exists());
        assertTrue(actualDirectory.isDirectory());
        FileUtils.delete(actualDirectory);
    }

    @Test
    public void shouldProvisionMavenArtifact() throws MalformedURLException {
        final String EXPECTED_DIRECTORY = "prove_dir";
        
        Provisioner prover = new Provisioner();
        prover.setProvisionDirectory( EXPECTED_DIRECTORY );
        prover.include("mvn:org.neo4j/neo4j-kernel/1.1", "neo4j-kernel.jar");
        prover.provision();

        File actualDirectory = new File(EXPECTED_DIRECTORY);
        String[] foundJars = actualDirectory.list( new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return name.endsWith("jar");
            }
        });
        assertThat( foundJars.length, is ( equalTo(1)));
        FileUtils.delete(actualDirectory);
    }

    @Test
    public void shouldProvisionMavenArtifactWithARecognizableName() throws MalformedURLException {
        final String EXPECTED_DIRECTORY = "prove_dir";

        Provisioner prover = new Provisioner();
        prover.setProvisionDirectory( EXPECTED_DIRECTORY );
        prover.include("mvn:org.neo4j/neo4j-kernel/1.1", "neo4j-kernel.jar");
        prover.provision();

        File actualDirectory = new File(EXPECTED_DIRECTORY);
        String[] foundJars = listJarsIn(actualDirectory);

        assertThat( foundJars.length, is ( equalTo(1)));
        assertThat( foundJars[0], containsString("neo4j-kernel"));
        FileUtils.delete(actualDirectory);
    }

    @Test
    public void shouldProvisionArtifactWithFluently() {
        Provisioner prover = new Provisioner();


    }

    @Test
    public void shouldIncludeJarsInClasspath() throws MalformedURLException {
        final String EXPECTED_DIRECTORY = "prove_dir";
        final String EXPECTED_JAR = "neo4j-kernel.jar";

        Provisioner prover = new Provisioner();
        prover.setProvisionDirectory( EXPECTED_DIRECTORY );
        prover.include("mvn:org.neo4j/neo4j-kernel/1.1", EXPECTED_JAR);
        prover.provision();

        assertThat( Arrays.asList(prover.getProvisionedClasspath()), hasItemEndingIn(EXPECTED_JAR));

        File actualDirectory = new File(EXPECTED_DIRECTORY);
        FileUtils.delete(actualDirectory);
    }

    @Test
    public void shouldNotRequireArtifactsInClasspath() {
        final String EXPECTED_DIRECTORY = "prove_dir";

        Provisioner prover = new Provisioner();
        prover.setProvisionDirectory( EXPECTED_DIRECTORY );

        assertThat( prover.getProvisionedClasspath(), is(notNullValue()));

        File actualDirectory = new File(EXPECTED_DIRECTORY);
        FileUtils.delete(actualDirectory);
    }

    @Test
    public void shouldIncludeTargetClassesInClasspath() {
        final String EXPECTED_DIRECTORY = "prove_dir";

        Provisioner prover = new Provisioner();
        prover.setProvisionDirectory( EXPECTED_DIRECTORY );
        prover.includeTargetClasses();

        assertThat( Arrays.asList(prover.getProvisionedClasspath()), hasItemEndingIn("classes"));
        
        File actualDirectory = new File(EXPECTED_DIRECTORY);
        FileUtils.delete(actualDirectory);
    }


    @Test
    public void shouldIncludeTargetTestClassesInClasspath() {
        final String EXPECTED_DIRECTORY = "prove_dir";

        Provisioner prover = new Provisioner();
        prover.setProvisionDirectory( EXPECTED_DIRECTORY );
        prover.includeTargetTestClasses();

        assertThat( Arrays.asList(prover.getProvisionedClasspath()), hasItemEndingIn("test-classes"));

        File actualDirectory = new File(EXPECTED_DIRECTORY);
        FileUtils.delete(actualDirectory);
    }

    @Test
    public void shouldProvisionClasspathForLocalApp() throws PlatformException {
        Provisioner prover = new Provisioner();
        prover.includeTargetClasses();
        prover.includeTargetTestClasses();

        boolean waitForExit = true;
        DefaultJavaRunner runner = new DefaultJavaRunner( waitForExit );

        String[] vmOptions = { "-server" };
        String[] classpath = prover.getProvisionedClasspath();
        String mainClass = "org.neo4j.release.it.std.exec.HelloWorldApp";
        String[] programOptions = null;
        String javaHome = System.getProperty("java.home");
        File workingDir = null; // new File(".");

        System.out.println(Arrays.toString(classpath));

        runner.exec(vmOptions, classpath, mainClass, programOptions, javaHome, workingDir);
    }


    private String[] listJarsIn(File directory) {
         return directory.list( new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return name.endsWith("jar");
            }
        });
    }
}
