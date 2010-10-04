package org.neo4j.ext.udc;

/**
 * Property keys used to configure the UDC extension.
 */
public interface UdcProperties {

    /**
     * Configuration key for the first delay, expressed
     * in milliseconds.
     */
    public static final String FIRST_DELAY_CONFIG_KEY = "neo4j.ext.udc.first_delay";

    /**
     * Configuration key for the interval for regular updates,
     * expressed in milliseconds.
     */
    public static final String INTERVAL_CONFIG_KEY = "neo4j.ext.udc.interval";

    /**
     * Configuration key for disabling the UDC extension. Set to "true"
     * to disable; any other value is considered false.
     */
    public static final String UDC_DISABLE_KEY = "neo4j.ext.udc.disable";


    /**
     * The host address to which UDC updates will be sent.
     * Should be of the form hostname[:port].
     */
    public static final String UDC_HOST_ADDRESS_KEY = "neo4j.ext.udc.host";

}
