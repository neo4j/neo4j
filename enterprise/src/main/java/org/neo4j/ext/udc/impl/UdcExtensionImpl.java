package org.neo4j.ext.udc.impl;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

import java.util.Timer;

/**
 * Kernel extension for UDC, the Usage Data Collector. The UDC runs as a background
 * daemon, waking up once a day to collect basic usage information about a long
 * running graph database.
 * <p/>
 * The first update is delayed to avoid needless activity during integration
 * testing and short-run applications. Subsequent updates are made at regular
 * intervals. Both times are specified in milliseconds.
 *
 */
@Service.Implementation(KernelExtension.class)
public class UdcExtensionImpl extends KernelExtension {

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
    public static final String UDC_HOST_ADDRESS = "neo4j.ext.udc.host";

    /**
     * Delay, in milliseconds, before the first UDC update is sent.
     *
     * Defaults to 10 minutes.
     */
    private int firstDelay = 10 * 1000 * 60;

    /**
     * Millisecond interval for subsequent updates.
     *
     * Defaults to 24 hours.
     */
    private int interval = 1000 * 60 * 60 * 24;

    /**
     * Host address to which UDC updates will be sent.
     */
    private String hostAddress = "udc.neo4j.org";

    /**
     * Disable the extension.
     *
     * Defaults to false.
     */
    private boolean disabled = false;

    /**
     * No-arg constructor, sets the extension key to "kernel udc".
     *  
     */
    public UdcExtensionImpl() {
        super("kernel udc");
    }

    /**
     * Lifecycle load event, which occurs during the startup of
     * a new GraphDbInstance.
     *
     * Configuration information is retrieved from the KernelData,
     * then a daemon thread is started to periodically collect
     * and submit usage data.
     *
     * @param kernel reference to the loading graph database kernel.
     */
    @Override
    protected void load(KernelData kernel) {
        configure(kernel.getConfig());

        kernel.setState(this, new Object());

        if (!disabled) {
            Timer timer = new Timer();
            NeoStoreXaDataSource ds = (NeoStoreXaDataSource) kernel.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource("nioneodb");
            String storeId = Long.toHexString(ds.getRandomIdentifier());
            UdcTimerTask task = new UdcTimerTask(hostAddress, Version.VERSION.getRevision(), storeId);
            timer.scheduleAtFixedRate(task, firstDelay, interval);
        }
    }

    /**
     * Attempt to retrieve configuration provided by user.
     * If not found, the defaults will be used.
     *
     * @param config user defined configuration parameters
     */
    private void configure(Config config) {
        try {
            firstDelay = Integer.parseInt((String) config.getParams().get(FIRST_DELAY_CONFIG_KEY));
        } catch (Exception e) {
            ;
        }
        try {
            interval = Integer.parseInt((String)config.getParams().get(INTERVAL_CONFIG_KEY));
        } catch (Exception e) {
            ;
        }
        try {
            String possibleHost = (String)config.getParams().get(UDC_HOST_ADDRESS);
            if (null != possibleHost) hostAddress = possibleHost;
        } catch (Exception e) {
            ;
        }

        disabled = Boolean.valueOf((String)config.getParams().get(UDC_DISABLE_KEY));
    }
}
