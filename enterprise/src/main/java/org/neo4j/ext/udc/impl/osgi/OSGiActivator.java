package org.neo4j.ext.udc.impl.osgi;

import java.util.*;

import org.neo4j.ext.udc.impl.UdcExtensionImpl;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;
import org.osgi.framework.*;


/**
 * OSGi bundle activator to start an OSGi servicewatcher
 * for kernel extensions.
 */
public final class OSGiActivator
    implements BundleActivator {

    /**
     * Called whenever the OSGi framework starts our bundle
     */
    public void start(BundleContext bc)
        throws Exception {
        // register the UdcExtenionImpl
        Dictionary props = new Properties();
        // Register our example service implementation in the OSGi service registry
        bc.registerService(KernelExtension.class.getName(), new UdcExtensionImpl(), props);
    }

    /**
     * Called whenever the OSGi framework stops our bundle
     */
    public void stop(BundleContext bc)
        throws Exception {
        // no need to unregister our service - the OSGi framework handles it for us
    }

}

