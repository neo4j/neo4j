/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
