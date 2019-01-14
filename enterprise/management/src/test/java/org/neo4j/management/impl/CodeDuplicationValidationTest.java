/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.management.impl;

import org.junit.Test;

import javax.management.ObjectName;

import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.Primitives;
import org.neo4j.jmx.StoreFile;
import org.neo4j.jmx.impl.ManagementSupport;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.HighAvailability;
import org.neo4j.management.IndexSamplingManager;
import org.neo4j.management.LockManager;
import org.neo4j.management.MemoryMapping;
import org.neo4j.management.TransactionManager;

import static org.junit.Assert.assertEquals;

public class CodeDuplicationValidationTest
{
    private class DefaultManagementSupport extends ManagementSupport
    {
        @Override
        protected ObjectName createObjectName( String instanceId, String beanName, boolean query, String... extraNaming )
        {
            return super.createObjectName( instanceId, beanName, query, extraNaming );
        }

        @Override
        protected String getBeanName( Class<?> beanInterface )
        {
            return super.getBeanName( beanInterface );
        }
    }

    private class CustomManagementSupport extends AdvancedManagementSupport
    {
        // belongs to this package - no override needed
    }

    @Test
    public void kernelBeanTypeNameMatchesExpected()
    {
        assertEquals( Kernel.class.getName(), KernelProxy.KERNEL_BEAN_TYPE );
        assertEquals( Kernel.NAME, KernelProxy.KERNEL_BEAN_NAME );
    }

    @Test
    public void mbeanQueryAttributeNameMatchesMethodName() throws Exception
    {
        assertEquals( ObjectName.class, Kernel.class.getMethod( "get" + KernelProxy.MBEAN_QUERY ).getReturnType() );
    }

    @Test
    public void interfacesGetsTheSameBeanNames()
    {
        assertEqualBeanName( Kernel.class );
        assertEqualBeanName( Primitives.class );
        assertEqualBeanName( HighAvailability.class );
        assertEqualBeanName( BranchedStore.class );
        assertEqualBeanName( LockManager.class );
        assertEqualBeanName( MemoryMapping.class );
        assertEqualBeanName( StoreFile.class );
        assertEqualBeanName( TransactionManager.class );
        assertEqualBeanName( IndexSamplingManager.class );
    }

    private void assertEqualBeanName( Class<?> beanClass )
    {
        assertEquals( new DefaultManagementSupport().getBeanName( beanClass ),
                new CustomManagementSupport().getBeanName( beanClass ) );
    }

    @Test
    public void generatesEqualObjectNames()
    {
        assertEquals( new DefaultManagementSupport().createMBeanQuery( "test-instance" ),
                new CustomManagementSupport().createMBeanQuery( "test-instance" ) );
        assertEquals( new DefaultManagementSupport().createObjectName( "test-instace", Kernel.class ),
                new CustomManagementSupport().createObjectName( "test-instace", Kernel.class ) );
    }
}
