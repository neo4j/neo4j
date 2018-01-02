/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.jmx.impl;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.jmx.Primitives;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;

@Service.Implementation( ManagementBeanProvider.class )
public final class PrimitivesBean extends ManagementBeanProvider
{
    public PrimitivesBean()
    {
        super( Primitives.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new PrimitivesImpl( management );
    }

    private static class PrimitivesImpl extends Neo4jMBean implements Primitives
    {
        private final IdGeneratorFactory idGeneratorFactory;

        PrimitivesImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            this.idGeneratorFactory = management.resolveDependency( IdGeneratorFactory.class );
        }

        @Override
        public long getNumberOfNodeIdsInUse()
        {
            return idGeneratorFactory.get( IdType.NODE ).getNumberOfIdsInUse();
        }

        @Override
        public long getNumberOfRelationshipIdsInUse()
        {
            return idGeneratorFactory.get( IdType.RELATIONSHIP ).getNumberOfIdsInUse();
        }

        @Override
        public long getNumberOfPropertyIdsInUse()
        {
            return idGeneratorFactory.get( IdType.PROPERTY ).getNumberOfIdsInUse();
        }

        @Override
        public long getNumberOfRelationshipTypeIdsInUse()
        {
            return idGeneratorFactory.get( IdType.RELATIONSHIP_TYPE_TOKEN ).getNumberOfIdsInUse();
        }
    }
}
