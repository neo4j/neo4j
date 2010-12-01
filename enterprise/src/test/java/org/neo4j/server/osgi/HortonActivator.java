package org.neo4j.server.osgi;

import org.neo4j.server.osgi.services.ExampleHostService;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class HortonActivator implements BundleActivator
{
    private ServiceRegistration hortonCommRegistration;

    public void start( BundleContext bundleContext ) throws Exception
    {
        ExampleHostService hortonCommunicator = new ExampleHostService() {
            public String askHorton( String aQuestion )
            {
                return "a person is a person, no matter how small";
            }
        };
        hortonCommRegistration = bundleContext.registerService(
            ExampleHostService.class.getName(), hortonCommunicator, null);

        System.out.println("Horton is listening to the Whos");
    }

    public void stop( BundleContext bundleContext ) throws Exception
    {
        hortonCommRegistration.unregister();
        System.out.println("Horton has given up on the Whos");
    }
}
