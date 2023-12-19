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
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Test;

import java.io.File;
import java.util.function.LongSupplier;

import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
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
