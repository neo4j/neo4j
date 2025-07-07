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
package org.neo4j.bolt.testing.channel;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.lang.StackWalker.Option;
import java.lang.StackWalker.StackFrame;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.neo4j.bolt.testing.channel.StrictBufferContext.RootStrictBufferContext;

public class StrictBufferTestExtension implements ParameterResolver, AfterEachCallback {

    private static final Namespace NAMESPACE = Namespace.create(StrictBufferTestExtension.class);
    private static final String CHANNEL_PROVIDER_KEY = StrictBufferContext.class.getName();

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        var type = parameterContext.getParameter().getType();

        return type == StrictBufferContext.class || type == RootStrictBufferContext.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        var store = extensionContext.getStore(NAMESPACE);
        return store.getOrComputeIfAbsent(CHANNEL_PROVIDER_KEY, key -> new RegisteredStrictBufferContext());
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        var store = context.getStore(NAMESPACE);
        var provider = store.get(CHANNEL_PROVIDER_KEY, RegisteredStrictBufferContext.class);
        if (provider == null) {
            return;
        }

        provider.close();
    }

    private abstract class AbstractRegisteredStrictBufferContext implements StrictBufferContext {

        protected final Lock lock = new ReentrantLock();
        private boolean valid = true;
        private final List<EmbeddedChannel> channels = new ArrayList<>();
        private final List<ReferenceRegistration> references = new ArrayList<>();

        protected String getDescription() {
            return null;
        }

        protected void ensureValid(String resource) {
            if (!this.valid) {
                throw new UnsupportedOperationException(
                        "Attempted to allocate " + resource + " after test completion - Verify your test code");
            }
        }

        @Override
        public EmbeddedChannel channel(ChannelHandler... handlers) {
            this.lock.lock();
            try {
                this.ensureValid("Channel");

                var channel = new EmbeddedChannel(handlers);
                this.channels.add(channel);
                return channel;
            } finally {
                this.lock.unlock();
            }
        }

        private void track(ReferenceCounted ref, int expectedRefCount) {
            ref.touch("Tracked buffer: Expected refCnt is " + expectedRefCount + " Current count is " + ref.refCnt());

            var stack = new ArrayList<StackFrame>();
            StackWalker.getInstance(Option.RETAIN_CLASS_REFERENCE).forEach(frame -> {
                if (AbstractRegisteredStrictBufferContext.class.isAssignableFrom(frame.getDeclaringClass())
                        || frame.getDeclaringClass() == StrictBufferContext.class) {
                    return;
                }

                stack.add(frame);
            });

            this.references.add(new ReferenceRegistration(ref, stack, expectedRefCount));
        }

        private void scope(ReferenceCounted ref) {
            ref.touch("Scoped buffer");

            this.references.add(new ReferenceRegistration(ref, Collections.emptyList(), -1));
        }

        @Override
        public <R extends ReferenceCounted> R tracked(R ref, int expectedRefCount) {
            if (ref == null) {
                return null;
            }

            this.lock.lock();
            try {
                this.ensureValid(
                        ref instanceof ByteBuf ? "Buffer" : ref.getClass().getSimpleName());

                this.track(ref, expectedRefCount);
                return ref;
            } finally {
                this.lock.unlock();
            }
        }

        @Override
        public <R extends ReferenceCounted> R scoped(R ref) {
            if (ref == null) {
                return null;
            }

            this.lock.lock();
            try {
                this.ensureValid(
                        ref instanceof ByteBuf ? "Buffer" : ref.getClass().getSimpleName());

                this.scope(ref);
                return ref;
            } finally {
                this.lock.unlock();
            }
        }

        @Override
        public void close() {
            this.lock.lock();
            try {
                if (!this.valid) {
                    return;
                }

                this.valid = false;
                this.doClose();
            } finally {
                this.lock.unlock();
            }
        }

        protected void doClose() {
            this.finalizeChannels();
            this.finalizeBuffers();
        }

        private void finalizeChannels() {
            this.channels.forEach(ch -> {
                var inboundRefCount = ch.inboundMessages().stream()
                        .mapToInt(msg -> {
                            if (msg instanceof ReferenceCounted refCnt) {
                                return refCnt.refCnt();
                            }

                            return 0;
                        })
                        .sum();
                var outboundRefCount = ch.outboundMessages().stream()
                        .mapToInt(msg -> {
                            if (msg instanceof ReferenceCounted refCnt) {
                                return refCnt.refCnt();
                            }

                            return 0;
                        })
                        .sum();

                var inboundBuffersReadable = ch.inboundMessages().stream()
                        .mapToInt(msg -> {
                            if (msg instanceof ByteBuf buf) {
                                return buf.readableBytes();
                            }

                            return 0;
                        })
                        .sum();
                var outboundBuffersReadable = ch.outboundMessages().stream()
                        .mapToInt(msg -> {
                            if (msg instanceof ByteBuf buf) {
                                return buf.readableBytes();
                            }

                            return 0;
                        })
                        .sum();

                if (ch.finishAndReleaseAll()) {
                    var msg = new StringBuilder();

                    var desc = this.getDescription();
                    if (desc != null) {
                        msg.append("--- Context: ").append(desc).append("\n");
                    }

                    msg.append(
                                    "One or more buffers within test channels had remaining data at the end of the test:\n\n")
                            .append("    Inbound readable: ")
                            .append(inboundBuffersReadable)
                            .append(" (refCnt: ")
                            .append(inboundRefCount)
                            .append(")\n")
                            .append("    Outbound readable: ")
                            .append(outboundBuffersReadable)
                            .append(" (refCnt: ")
                            .append(outboundRefCount)
                            .append(")\n\n")
                            .append("Please ensure that all buffers are asserted on or discard them properly.");

                    throw new AssertionError(msg.toString());
                }
            });
        }

        private void finalizeBuffers() {
            var reportBuilder = new StringBuilder();

            var desc = this.getDescription();
            if (desc != null) {
                reportBuilder.append("--- Context: ").append(desc).append("\n");
            }

            reportBuilder.append("One or more test buffers have not been released at the end of the test:\n\n");

            var anyLeaks = this.references.stream().anyMatch(ReferenceRegistration::hasLeaked);
            if (anyLeaks) {
                var i = 0;
                for (var reg : this.references) {
                    if (!reg.hasLeaked()) {
                        continue;
                    }

                    reportBuilder.append(" #").append(++i).append(": ");

                    if (reg.ref instanceof ByteBuf buf) {
                        if (buf.refCnt() != 0) {
                            reportBuilder
                                    .append(buf.readableBytes())
                                    .append(" bytes remaining with a capacity of ")
                                    .append(buf.capacity())
                                    .append(" ");
                        } else {
                            reportBuilder.append("released early ");
                        }
                    }

                    reportBuilder.append("(refCnt: ").append(reg.ref.refCnt()).append(")");

                    if (!reg.allocationStack.isEmpty()) {
                        reportBuilder.append(" allocated at:\n");

                        reg.allocationStack.forEach(element -> reportBuilder
                                .append("     ")
                                .append(element.toString())
                                .append("\n"));
                    }

                    reportBuilder.append("\n");
                }

                reportBuilder.append("Please ensure that all buffers are discarded properly.");
            }

            this.references.stream().filter(reg -> reg.ref.refCnt() != 0).forEach(reg -> {
                ReferenceCountUtil.touch(
                        reg.ref, "Current refCnt: " + reg.ref.refCnt() + " about to release: " + reg.ref.refCnt());
                ReferenceCountUtil.safeRelease(reg.ref, reg.ref.refCnt());
            });

            if (anyLeaks) {
                throw new AssertionError(reportBuilder.toString());
            }
        }
    }

    private class RegisteredStrictBufferContext extends AbstractRegisteredStrictBufferContext
            implements StrictBufferContext.RootStrictBufferContext {

        private final List<InnerRegisteredStrictBufferContext> children = new ArrayList<>();

        @Override
        public StrictBufferContext child(String description) {
            this.lock.lock();
            try {
                this.ensureValid("Child");

                var child = new InnerRegisteredStrictBufferContext(description);
                this.children.add(child);
                return child;
            } finally {
                this.lock.unlock();
            }
        }

        @Override
        protected void doClose() {
            super.doClose();
            this.children.forEach(AbstractRegisteredStrictBufferContext::close);
        }
    }

    private class InnerRegisteredStrictBufferContext extends AbstractRegisteredStrictBufferContext {

        private final String description;

        public InnerRegisteredStrictBufferContext(String description) {
            this.description = description;
        }

        @Override
        protected String getDescription() {
            return this.description;
        }
    }

    private record ReferenceRegistration(ReferenceCounted ref, List<StackFrame> allocationStack, int expectedRefCount) {

        public boolean hasLeaked() {
            return this.expectedRefCount >= 0 && this.ref.refCnt() != this.expectedRefCount;
        }
    }
}
