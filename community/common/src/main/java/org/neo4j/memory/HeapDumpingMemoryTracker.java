/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.memory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * A memory tracker that monitors the high water mark of estimated heap usage,
 * and allows to set a target value that when reached, either a heap dump will be taken,
 * or a callback fired, or both.
 */
public class HeapDumpingMemoryTracker implements MemoryTracker
{
    private final MemoryTracker delegate;
    private final HeapDumper heapDumper;
    private long targetHighWaterMark;
    private String heapDumpFileName;
    private boolean overwriteExisting;
    private boolean liveObjectsOnly;
    private Consumer<HeapDumpingMemoryTracker> onTargetReached;
    private long lastAllocatedBytes;

    public HeapDumpingMemoryTracker( MemoryTracker delegate )
    {
        this.delegate = delegate;
        this.heapDumper = new HeapDumper();
    }

    public void setHeapDumpAtHighWaterMark( long targetHighWaterMark, String heapDumpFileName )
    {
        this.targetHighWaterMark = targetHighWaterMark;
        this.heapDumpFileName = heapDumpFileName;
        this.overwriteExisting = true;
        this.liveObjectsOnly = true;
        this.onTargetReached = null;
    }

    public void setHeapDumpAtHighWaterMark( long targetHighWaterMark, String heapDumpFileName, boolean overwriteExisting, boolean liveObjectsOnly,
            Consumer<HeapDumpingMemoryTracker> onTargetReached )
    {
        this.targetHighWaterMark = targetHighWaterMark;
        this.heapDumpFileName = heapDumpFileName;
        this.overwriteExisting = overwriteExisting;
        this.liveObjectsOnly = liveObjectsOnly;
        this.onTargetReached = onTargetReached;
    }

    public void setCallbackAtHighWaterMark( long targetHighWaterMark, Consumer<HeapDumpingMemoryTracker> onTargetReached )
    {
        this.targetHighWaterMark = targetHighWaterMark;
        this.heapDumpFileName = null; // No heap dump will be done
        this.overwriteExisting = false;
        this.liveObjectsOnly = true;
        this.onTargetReached = onTargetReached;
    }

    @Override
    public long usedNativeMemory()
    {
        return delegate.usedNativeMemory();
    }

    @Override
    public long estimatedHeapMemory()
    {
        return delegate.estimatedHeapMemory();
    }

    @Override
    public void allocateNative( long bytes )
    {
        delegate.allocateNative( bytes );
    }

    @Override
    public void releaseNative( long bytes )
    {
        delegate.releaseNative( bytes );
    }

    @Override
    public void allocateHeap( long bytes )
    {
        delegate.allocateHeap( bytes );
        checkIfTargetHighWaterMarkReached( bytes );
    }

    @Override
    public void releaseHeap( long bytes )
    {
        delegate.releaseHeap( bytes );
    }

    @Override
    public long heapHighWaterMark()
    {
        return delegate.heapHighWaterMark();
    }

    @Override
    public void reset()
    {
        delegate.reset();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public MemoryTracker getScopedMemoryTracker()
    {
        return new ScopedMemoryTracker( this );
    }

    public long lastAllocatedBytes()
    {
        return lastAllocatedBytes;
    }

    private void checkIfTargetHighWaterMarkReached( long bytes )
    {
        var currentHighWaterMark = delegate.heapHighWaterMark();
        if ( targetHighWaterMark > 0 && currentHighWaterMark >= targetHighWaterMark )
        {
            lastAllocatedBytes = bytes;
            doHeapDump();
            if ( onTargetReached != null )
            {
                onTargetReached.accept( this );
            }
            targetHighWaterMark = 0; // Do not dump again until a new target is set
        }
    }

    private void doHeapDump()
    {
        if ( heapDumpFileName == null )
        {
            return;
        }

        if ( overwriteExisting && Files.exists( Path.of( heapDumpFileName ) ) )
        {
            try
            {
                Files.delete( Path.of( heapDumpFileName ) );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        heapDumper.createHeapDump( heapDumpFileName, liveObjectsOnly );
    }
}
