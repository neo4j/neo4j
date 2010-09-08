package org.neo4j.kernel.impl.osgi;

import org.neo4j.helpers.ExtensionLoader;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

import java.util.Vector;

/**
 * An OSGi-friendly ExtensionLoader, the OSGiExtensionLoader
 * uses normal OSGi service discovery to find published services.
 */
public class OSGiExtensionLoader implements ExtensionLoader {

    private BundleContext bc;

    public OSGiExtensionLoader(BundleContext bc) {
        this.bc = bc;
    }

    public <T> Iterable<T> loadExtensionsOfType(Class<T> type) {
        try {
            ServiceReference[] services = bc.getServiceReferences(type.getName(), null);
            if (services != null) {
                Vector<T> serviceCollection = new Vector<T>();
                for (ServiceReference sr : services) {
                   serviceCollection.add((T)bc.getService(sr));
                }
                return serviceCollection;
            } else {
                return null;
            }
        } catch (InvalidSyntaxException e) {
            System.out.println("Failed to load extensions of type: " + type);
            e.printStackTrace();
        }

        return null;
    }

}
