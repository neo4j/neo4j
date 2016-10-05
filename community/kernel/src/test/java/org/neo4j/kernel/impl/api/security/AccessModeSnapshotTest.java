package org.neo4j.kernel.impl.api.security;

import org.junit.Test;

import org.neo4j.kernel.api.security.AccessMode;

import static org.junit.Assert.assertEquals;

public class AccessModeSnapshotTest
{
    @Test
    public void shouldCorrectlyReflectAccessMode()
    {
        testAccessMode( AccessMode.Static.READ );
        testAccessMode( AccessMode.Static.WRITE );
        testAccessMode( AccessMode.Static.WRITE_ONLY );
        testAccessMode( AccessMode.Static.FULL );
        testAccessMode( AccessMode.Static.OVERRIDE_READ );
        testAccessMode( AccessMode.Static.OVERRIDE_WRITE );
        testAccessMode( AccessMode.Static.OVERRIDE_SCHEMA );
        testAccessMode( AccessMode.Static.CREDENTIALS_EXPIRED );
    }

    private void testAccessMode( AccessMode originalAccessMode )
    {
        AccessMode accessModeSnapshot = AccessModeSnapshot.createAccessModeSnapshot( originalAccessMode );
        assertEquals( accessModeSnapshot.allowsReads(), originalAccessMode.allowsReads() );
        assertEquals( accessModeSnapshot.allowsWrites(), originalAccessMode.allowsWrites() );
        assertEquals( accessModeSnapshot.allowsSchemaWrites(), originalAccessMode.allowsSchemaWrites() );
        assertEquals( accessModeSnapshot.overrideOriginalMode(), originalAccessMode.overrideOriginalMode() );
        assertEquals( accessModeSnapshot.name(), originalAccessMode.name() );
    }
}