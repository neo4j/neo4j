/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.backup.impl;

import org.junit.Test;

import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.backup.impl.BackupSupportingClassesFactoryProvider.getProvidersByPriority;

public class OnlineBackupCommandProviderTest
{
    @Test
    public void communityBackupSupportingFactory()
    {
        BackupModule backupModule = mock( BackupModule.class );
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        when( backupModule.getOutsideWorld() ).thenReturn( outsideWorld );

        BackupSupportingClassesFactoryProvider provider = getProvidersByPriority().findFirst().get();
        BackupSupportingClassesFactory factory = provider.getFactory( backupModule );
        assertEquals( VoidPipelineWrapperFactory.VOID_WRAPPER,
                factory.createPipelineWrapper( Config.defaults() ) );
    }

    /**
     * This class must be public and static because it must be service loadable.
     */
    public static class DummyProvider extends BackupSupportingClassesFactoryProvider
    {
        @Override
        public BackupSupportingClassesFactory getFactory( BackupModule backupModule )
        {
            return new BackupSupportingClassesFactory( backupModule )
            {
                @Override
                protected PipelineWrapper createPipelineWrapper( Config config )
                {
                    throw new AssertionError( "This provider should never be loaded" );
                }
            };
        }

        @Override
        protected int getPriority()
        {
            return super.getPriority() - 1;
        }
    }
}
