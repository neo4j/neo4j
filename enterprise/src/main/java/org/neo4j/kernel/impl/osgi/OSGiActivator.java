package org.neo4j.kernel.impl.osgi;

import org.neo4j.helpers.Service;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 * OSGi bundle activator to start an OSGi servicewatcher for kernel extensions.
 * 
 */
public final class OSGiActivator implements BundleActivator
{

    /**
     * Called whenever the OSGi framework starts our bundle
     */
    public void start( BundleContext bc ) throws Exception
    {
        // start the extension listener
        Service.osgiExtensionLoader = new OSGiExtensionLoader( bc );
    }

    /**
     * Called whenever the OSGi framework stops our bundle
     */
    public void stop( BundleContext bc ) throws Exception
    {
        // no need to unregister our service - the OSGi framework handles it for
        // us
    }

}
