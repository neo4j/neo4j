package org.neo4j.server;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;

import org.junit.Test;


public class AddressResolverTest
{
    @Test
    public void givenNoOverrdingConfigurationShouldDefaultToLookingUpHostname() throws Exception {
        AddressResolver resolver = new AddressResolver();
        assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), resolver.getHostname());
    }
}
