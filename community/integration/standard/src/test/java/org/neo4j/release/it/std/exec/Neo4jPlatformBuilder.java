package org.neo4j.release.it.std.exec;

import org.ops4j.pax.runner.platform.Configuration;
import org.ops4j.pax.runner.platform.PlatformBuilder;
import org.ops4j.pax.runner.platform.PlatformContext;
import org.ops4j.pax.runner.platform.PlatformException;

import java.io.IOException;
import java.io.InputStream;

/**
 * A pax-runner PlatformBuilder.
 */
public class Neo4jPlatformBuilder implements PlatformBuilder {

    /**
     * Provider name to be used in registration.
     */
    private static final String PROVIDER_NAME = "neo4j";

    /**
     * Provider version to be used in registration.
     */
    private static final String PROVIDER_VERSION = "0.1";

    /**
     * The directory name where the configuration will be stored.
     */
    private static final String CONFIG_DIRECTORY = "felix";
    /**
     * Configuration file name.
     */
    private static final String CONFIG_INI = "config.ini";
    /**
     * Caching directory.
     */
    private static final String CACHE_DIRECTORY = "cache";
    /**
     * Profile name to be used when console should be started.
     */
    private static final String CONSOLE_PROFILE = "tui";
    /**
     * Separator for properties (bundles)
     */
    private static final String SEPARATOR = " ";

    /**
     * Name of the main class from Felix.
     */
    private String mainClassName = "org.apache.felix.main.Main";

    /**
     * Args to be passed to the main class.
     */
    private String[] mainArgs;

    /**
     * Options to be passed to the jvm.
     */
    private String[] jvmOpts;

    public Neo4jPlatformBuilder(String mainClassName, String[] mainArgs, String[] jvmOpts) {
        this.mainClassName = mainClassName;
        this.mainArgs = mainArgs;
        this.jvmOpts = jvmOpts;
    }

    public void prepare(PlatformContext context) throws PlatformException {
        // TODO
    }

    public String getMainClassName() {
        return mainClassName;
    }

    public String[] getArguments(PlatformContext context) {
        return mainArgs;
    }

    public String[] getVMOptions(PlatformContext context) {
        return jvmOpts;
    }

    public InputStream getDefinition(Configuration configuration) throws IOException {
        return null;
    }

    public String getRequiredProfile(PlatformContext context) {
        return null;
    }

    public String getProviderName() {
        return PROVIDER_NAME;
    }

    public String getProviderVersion() {
        return PROVIDER_VERSION;
    }
}
