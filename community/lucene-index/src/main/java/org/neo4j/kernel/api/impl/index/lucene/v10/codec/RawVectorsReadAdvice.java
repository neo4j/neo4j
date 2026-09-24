/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index.lucene.v10.codec;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.ReadAdvice;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDirectory;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10Directory;
import org.neo4j.util.VisibleForTesting;

/// Applies [ReadAdvice#RANDOM] to the raw float32 vectors (`.vec`) and to nothing else.
///
/// `MADV_RANDOM` buys the loss of the kernel's fault-around readahead. That is a bad trade for the
/// HNSW graph (`.vex`) and the quantized codes (`.veq`) -- both are small enough to stay resident
/// and have to warm up quickly -- and a good one for `.vec`: binary quantization touches it only
/// while rescoring, one unrelated vector at a time, and on a large index it is far too big to hold
/// in the page cache, so readahead there reads 16-32 pages to use one and evicts the graph doing it.
///
/// Selecting only `.vec` is the whole difficulty. [MMapDirectory#setReadAdvice] is keyed on the file
/// name, and inside a compound segment -- the default layout, since
/// `internal.dbms.index.lucene.nocfs.ratio` is 1.0 -- the only name the directory is ever handed is
/// the container's `.cfs`, so a name predicate silently matches nothing. Lucene's own
/// [org.apache.lucene.store.DataAccessHint] does not discriminate either: `.vex`, `.veq` and `.vec`
/// are all opened with [org.apache.lucene.store.DataAccessHint#RANDOM], so honouring it advises all
/// three.
///
/// So the file is marked instead of matched. [#markRawVectors] wraps the directory a reader opens
/// its files through and tags `.vec` -- and only `.vec` -- with a private hint; [#READ_ADVICE], which
/// the vector index provider installs on its mmap directories when
/// [org.neo4j.kernel.api.impl.index.lucene.LuceneSettings#vector_rescore_read_advice] is enabled,
/// turns that hint into `RANDOM`. Both layouts then work through one
/// mechanism:
///
///   - plain segment: [MMapDirectory#openInput] evaluates [#READ_ADVICE] against the tagged context;
///   - compound segment: `Lucene90CompoundReader.openInput` forwards the context to
///     `IndexInput.slice(name, offset, length, context)`, which advises exactly that sub-range of the
///     shared `.cfs` mapping.
///
/// Merges keep their readahead: Lucene builds the context it reuses for
/// [org.apache.lucene.codecs.hnsw.FlatVectorsReader#getMergeInstance] before this wrapper tags
/// anything, so the merge instance re-advises without the hint and falls back to the default.
public final class RawVectorsReadAdvice {
    private static final String RAW_VECTORS_SUFFIX = ".vec";

    /// Private on purpose. A hint of our own is what makes `.vec` distinguishable from the other two
    /// vector files, all of which Lucene opens as randomly accessed.
    private enum Marker implements IOContext.FileOpenHint {
        RAW_VECTORS
    }

    private RawVectorsReadAdvice() {}

    /// Why the raw vectors stopped being advised after they started being -- a kernel that rejected
    /// the `posix_madvise` -- or null while they still are. Set once, never cleared, for the same
    /// reason as [NativeAccessRawVectorAdvisor]'s disabled latch: a call the kernel keeps rejecting
    /// should not be repeated, and advice that silently stopped applying is the failure mode this
    /// class exists to prevent. Reachable for tests and diagnosis.
    private static volatile String adviseRejected;

    static String adviseRejected() {
        return adviseRejected;
    }

    @VisibleForTesting
    static void clearRejectedForTesting() {
        adviseRejected = null;
    }

    /// For [MMapDirectory#setReadAdvice]. Everything unmarked falls through to
    /// [org.apache.lucene.util.Constants#DEFAULT_READADVICE].
    private static final Optional<ReadAdvice> RANDOM_ADVICE = Optional.of(ReadAdvice.RANDOM);

    public static final BiFunction<String, IOContext, Optional<ReadAdvice>> READ_ADVICE =
            (name, context) -> context.hints().contains(Marker.RAW_VECTORS) ? RANDOM_ADVICE : Optional.empty();

    /// Wraps `directory` so that opening a `.vec` file through it carries the marker.
    public static Directory markRawVectors(Directory directory) {
        return new MarkingDirectory(directory);
    }

    /// Installs [#READ_ADVICE] on the mmap directory under `directory`, if there is one, so that
    /// marked `.vec` opens resolve to [ReadAdvice#RANDOM]. Byte-buffer and NIO directories have no
    /// advice to give, and non-v10 Neo4j directories none to advise, and both are left alone.
    public static void adviseFor(LuceneDirectory directory) {
        if (!(directory instanceof Lucene10Directory lucene10Directory)) {
            return;
        }
        Directory delegate = lucene10Directory.directory();
        while (delegate instanceof FilterDirectory filterDirectory) {
            delegate = filterDirectory.getDelegate();
        }
        if (delegate instanceof MMapDirectory mmapDirectory) {
            mmapDirectory.setReadAdvice(READ_ADVICE);
        }
    }

    private static final class MarkingDirectory extends FilterDirectory {
        MarkingDirectory(Directory in) {
            super(in);
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (!isRawVectors(name) || adviseRejected != null) {
                return in.openInput(name, context);
            }
            try {
                return in.openInput(name, marked(context));
            } catch (IOException e) {
                adviseRejected = e.getMessage();
                return in.openInput(name, context);
            }
        }
    }

    /// [IOContext#withHints] *replaces* the hint set rather than adding to it, so the hints the
    /// reader has already put on the context -- `FileTypeHint.DATA`, `FileDataHint.KNN_VECTORS` --
    /// have to be carried over by hand, or opening a `.vec` would strip them.
    ///
    /// A hint may appear at most once per type, hence the check: `withHints` rejects duplicates.
    private static IOContext marked(IOContext context) {
        Set<IOContext.FileOpenHint> hints = context.hints();
        if (hints.contains(Marker.RAW_VECTORS)) {
            return context;
        }
        IOContext.FileOpenHint[] withMarker = hints.toArray(new IOContext.FileOpenHint[hints.size() + 1]);
        withMarker[hints.size()] = Marker.RAW_VECTORS;
        return context.withHints(withMarker);
    }

    private static boolean isRawVectors(String name) {
        return name.endsWith(RAW_VECTORS_SUFFIX);
    }
}
