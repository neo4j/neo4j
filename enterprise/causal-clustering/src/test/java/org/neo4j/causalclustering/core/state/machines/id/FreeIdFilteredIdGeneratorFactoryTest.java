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
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.function.LongSupplier;

import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FreeIdFilteredIdGeneratorFactoryTest
{
    private IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
    private File file = mock( File.class );

    @Test
    public void openFilteredGenerator()
    {
        FreeIdFilteredIdGeneratorFactory filteredGenerator = createFilteredFactory();
        IdType idType = IdType.NODE;
        long highId = 1L;
        long maxId = 10L;
        LongSupplier highIdSupplier = () -> highId;
        IdGenerator idGenerator = filteredGenerator.open( file, idType, highIdSupplier, maxId );

        verify( idGeneratorFactory ).open( eq( file ), eq( idType ), any( LongSupplier.class ), eq( maxId ) );
        assertThat( idGenerator, instanceOf( FreeIdFilteredIdGenerator.class ) );
    }

    @Test
    public void openFilteredGeneratorWithGrabSize()
    {
        FreeIdFilteredIdGeneratorFactory filteredGenerator = createFilteredFactory();
        IdType idType = IdType.NODE;
        long highId = 1L;
        long maxId = 10L;
        int grabSize = 5;
        LongSupplier highIdSupplier = () -> highId;
        IdGenerator idGenerator = filteredGenerator.open( file, grabSize, idType, highIdSupplier, maxId );

        verify( idGeneratorFactory ).open( file, grabSize, idType, highIdSupplier, maxId );
        assertThat( idGenerator, instanceOf( FreeIdFilteredIdGenerator.class ) );
    }

    private FreeIdFilteredIdGeneratorFactory createFilteredFactory()
    {
        return new FreeIdFilteredIdGeneratorFactory( idGeneratorFactory, () -> true );
    }
}
