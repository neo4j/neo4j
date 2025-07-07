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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ReferenceCounted;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.junit.jupiter.api.DynamicTest;
import org.neo4j.bolt.testing.annotation.StrictBufferExtension;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;

/**
 * Provides an injectable context which facilitates strict reference tracking mechanics within netty
 * handler tests.
 * <p/>
 * An implementation of this interface is automatically injected into tests when the
 * {@link StrictBufferExtension} annotation is present (or the {@link StrictBufferTestExtension}
 * extension is present within the scope of a test.
 */
public interface StrictBufferContext extends AutoCloseable {

    /**
     * Creates a new test scoped embedded channel with the given list of handlers pre-installed within
     * its pipeline.
     * <p/>
     * Channels created via this method will be closed automatically at the end of the test. Its
     * inbound and outbound queues will be checked for reference counted objects which have not been
     * freed correctly.
     * <p/>
     * As such, tests using this interface are expected to either assert and release all channel
     * outputs. For instance:
     * <p/>
     * <pre>
     * {@code
     * @Test
     * void myTest(TestChannelContext ctx) {
     *   var ch = ctx.channel(...);
     *
     *   var msg = ctx.output(ch.readOutbound());
     *   // or (for decoders)
     *   var msg = ctx.output(ch.readInbound());
     * }
     * }
     * </pre>
     * <p/>
     * Alternatively, inbound and outbound queues may be released manually (in cases where a message
     * is generated but not explicitly asserted on within a given test):
     * <p/>
     * <pre>
     * {@code
     * @Test
     * void myTest(TestChannelContext ctx) {
     *   var ch = ctx.channel(...);
     *
     *   ch.releaseOutbound();
     *   // or (for decoders)
     *   ch.releaseInbound();
     * }
     * }
     * </pre>
     * <p/>
     * Please note that the latter method should be avoided where possible as it risks potentially
     * missing actual buffer leaks as a result of the forced resource release.
     *
     * @param handlers an array of handlers to install.
     * @return a scoped embedded channel.
     */
    EmbeddedChannel channel(ChannelHandler... handlers);

    /**
     * Scopes a given reference counted object to the context of the test.
     * <p/>
     * References passed to this method will be released at the end of the test <b>without validation
     * of their final ref count</b>.
     * <p/>
     * For instance, this method should be invoked for objects created as part of a dynamic test
     * template where the resulting object is reused later within the test for validation.
     * <p/>
     * <pre>
     * {@code
     * @TestFactory
     * Stream<DynamicTest> myTest(TestChannelContext ctx) {
     *   return IntStream.range(0, 5)
     *     .mapToObject(i -> ctx.scoped(new MyObject(i)))
     *     .map(obj -> {
     *       DynamicTest.of(obj.toString(), () -> {
     *         var ch = ctx.channel(...);
     *         ch.writeInbound(ctx.tracked(obj.duplicate()));
     *       });
     *     });
     * }
     * }
     * </pre>
     *
     * @param ref a reference counted object or null.
     * @param <R> a reference counted object type.
     * @return the original reference counted object passed or null if null was given.
     */
    <R extends ReferenceCounted> R scoped(R ref);

    /**
     * Tracks a given reference counted object within the context of the test.
     * <p/>
     * Tracked reference counted object passed to this method will be released at the end of the test,
     * and their reference count will be validated to ensure that it equals the given expected number
     * of remaining references.
     * <p/>
     * For instance, this method should be invoked for objects received from a channel pipeline to
     * ensure that the handlers within the pipeline correctly retained the object when they are done
     * processing it:
     * <p/>
     * <pre>
     * {@code
     * @Test
     * void myTest(TestChannelContext ctx) {
     *   var ch = ctx.channel(...);
     *   var obj = ctx.tracked(ch.readOutbound(), 1);
     *   assertNotNull(obj);
     * }
     * }
     * </pre>
     * <p/>
     * Additionally to this method, this interface also provides the shorthand method
     * {@link #output(ReferenceCounted)} which is equivalent to {@code ctx.tracked(ref, 1)}.
     *
     * @param ref              a reference counted object or null.
     * @param expectedRefCount an expected reference count at the end of the test.
     * @param <R>              a reference counted object type.
     * @return the original reference counted object passed or null if null was given.
     */
    <R extends ReferenceCounted> R tracked(R ref, int expectedRefCount);

    /**
     * Tracks a given reference counted object within the context of the test.
     * <p/>
     * Tracked reference counted objects passed to this method will be released at the end of the
     * test, and their reference count will be validate to ensure that they have not been leaked.
     * <p/>
     * For instance, this method should be invoked for objects passed into the channel pipeline to
     * ensure that the handlers within the pipeline correctly release the object when they are done
     * processing it:
     * <p/>
     * <pre>
     * {@code
     * @Test
     * void myTest(TestChannelContext ctx) {
     *   var ch = ctx.channel(...);
     *   ch.writeInbound(ctx.tracked(obj));
     * }
     * }
     * }
     * </pre>
     *
     * @param ref a reference counted object.
     * @param <R> a reference counted object type.
     * @return the original reference counted object passed or null if null was given.
     */
    default <R extends ReferenceCounted> R tracked(R ref) {
        return this.tracked(ref, 0);
    }

    /**
     * Allocates a new buffer with a reasonable default capacity from the default ByteBuf allocator
     * and tracks it within the test scope.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.tracked(ByteBufAllocator.DEFAULT.buffer())}.
     *
     * @return a tracked buffer.
     * @see #tracked(ReferenceCounted) tracked(ReferenceCounted) for more detailed information.
     */
    default ByteBuf buffer() {
        return this.tracked(ByteBufAllocator.DEFAULT.buffer());
    }

    /**
     * Allocates a new buffer with a given initial capacity from the default ByteBuf allocator and
     * tracks it within the test scope.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.tracked(ByteBufAllocator.DEFAULT.buffer(initialCapacity))}.
     *
     * @param initialCapacity the initial capacity to allocate to this buffer.
     * @return a tracked buffer.
     * @see #tracked(ReferenceCounted) tracked(ReferenceCounted) for more detailed information.
     */
    default ByteBuf buffer(int initialCapacity) {
        return this.tracked(ByteBufAllocator.DEFAULT.buffer(initialCapacity));
    }

    /**
     * Allocates a new buffer with a given initial and maximum capacity from the default ByteBuf
     * allocator and tracks it within the test scope.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.tracked(ByteBufAllocator.DEFAULT.buffer(initialCapacity, maxCapacity))}.
     *
     * @param initialCapacity the initial capacity to allocate to this buffer.
     * @param maxCapacity     the maximum permitted capacity to which this buffer may grow.
     * @return a tracked buffer.
     * @see #tracked(ReferenceCounted) tracked(ReferenceCounted) for more detailed information.
     */
    default ByteBuf buffer(int initialCapacity, int maxCapacity) {
        return this.tracked(ByteBufAllocator.DEFAULT.buffer(initialCapacity, maxCapacity));
    }

    /**
     * Wraps a given byte array into a ByteBuf and tracks it within the test scope.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.tracked(Unpooled.wrappedBuffer(bytes))}.
     *
     * @param bytes a byte array.
     * @return a tracked buffer.
     * @see #tracked(ReferenceCounted) tracked(ReferenceCounted) for more detailed information.
     */
    default ByteBuf buffer(byte[] bytes) {
        return this.tracked(Unpooled.wrappedBuffer(bytes));
    }

    /**
     * Wraps a given byte array into a ByteBuf and tracks it within the test scope.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.tracked(Unpooled.wrappedBuffer(bytes))}.
     *
     * @param bytes          a byte array.
     * @param expectedRefCnt an expected reference count at the end of the test.
     * @return a tracked buffer.
     * @see #tracked(ReferenceCounted) tracked(ReferenceCounted) for more detailed information.
     */
    default ByteBuf buffer(byte[] bytes, int expectedRefCnt) {
        return this.tracked(Unpooled.wrappedBuffer(bytes), expectedRefCnt);
    }

    /**
     * Wraps a given UTF-8 string into a ByteBuf and tracks it within the test scope.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.buffer(plainText.getBytes(StandardCharsets.UTF_8))}.
     *
     * @param plainText a plain text string.
     * @return a tracked buffer.
     * @see #tracked(ReferenceCounted) tracked(ReferenceCounted) for more detailed information.
     */
    default ByteBuf buffer(String plainText) {
        return this.buffer(plainText.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Allocates a new buffer with a sensible default capacity via the default ByteBuf allocator and
     * scopes it to the current test.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.scoped(ByteBufAllocator.DEFAULT.buffer())}.
     *
     * @return a scoped buffer.
     * @see #scoped(ReferenceCounted) scoped(ReferenceCounted) for more detailed information.
     */
    default ByteBuf scopedBuffer() {
        return this.scoped(ByteBufAllocator.DEFAULT.buffer());
    }

    /**
     * Allocates a new buffer with a given initial capacity via the default ByteBuf allocator and
     * scopes it to the current test.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.scoped(ByteBufAllocator.DEFAULT.buffer(initialCapacity))}.
     *
     * @param initialCapacity the initial capacity of the allocated buffer.
     * @return a scoped buffer.
     * @see #scoped(ReferenceCounted) scoped(ReferenceCounted) for more detailed information.
     */
    default ByteBuf scopedBuffer(int initialCapacity) {
        return this.scoped(ByteBufAllocator.DEFAULT.buffer(initialCapacity));
    }

    /**
     * Allocates a new buffer with a given initial and maximum capacity via the default ByteBuf
     * allocator and scopes it to the current test.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.scoped(ByteBufAllocator.DEFAULT.buffer(initialCapacity, maxCapacity))}.
     *
     * @param initialCapacity the initial capacity of the allocated buffer.
     * @param maxCapacity     the maximum capacity of the allocated buffer.
     * @return a scoped buffer.
     * @see #scoped(ReferenceCounted) scoped(ReferenceCounted) for more detailed information.
     */
    default ByteBuf scopedBuffer(int initialCapacity, int maxCapacity) {
        return this.scoped(ByteBufAllocator.DEFAULT.buffer(initialCapacity, maxCapacity));
    }

    /**
     * Wraps a given byte array into a ByteBuf and scopes it to the test.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.scoped(Unpooled.scopedBuffer(bytes))}.
     *
     * @param bytes a byte array.
     * @return a scoped buffer.
     * @see #scoped(ReferenceCounted) scoped(ReferenceCounted) for more detailed information.
     */
    default ByteBuf scopedBuffer(byte[] bytes) {
        return this.scoped(Unpooled.wrappedBuffer(bytes));
    }

    /**
     * Wraps a given UTF-8 string into a ByteBuf and scopes it to the test.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.scopedBuffer(plainText.getBytes(StandardCharsets.UTF-8))}.
     *
     * @param plainText a plain text string.
     * @return a scoped buffer.
     * @see #scoped(ReferenceCounted) scoped(ReferenceCounted) for more detailed information.
     */
    default ByteBuf scopedBuffer(String plainText) {
        return this.scopedBuffer(plainText.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Tracks a given input buffer within the scope of this test.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.tracked(buffer.retainedDuplicate())}.
     * <p/>
     * For instance, this method is commonly used in cases where a buffer is constructed as part of a
     * dynamic test factory and later used as a source of truth for validation:
     * <p/>
     * <pre>
     * {@code
     * @TestFactory
     * Stream<DynamicTest> myTest(TestChannelContext ctx) {
     *   return IntStream.range(0, 3)
     *    .mapToObj(i -> ctx.scopedBuffer().writeInt(i))
     *    .map(buf -> DynamicTest.dynamicTest("Some Test", () -> {
     *      var ch = ctx.channel(...);
     *      ch.writeInbound(ctx.input(buf));
     *    });
     * }
     * }
     * </pre>
     *
     * @param buffer an input buffer.
     * @return a tracked buffer.
     * @see #scoped(ReferenceCounted) scoped(ReferenceCounted) for more detailed information.
     */
    default ByteBuf input(ByteBuf buffer) {
        return this.tracked(buffer.retainedDuplicate());
    }

    /**
     * Tracks a given output reference counted object within the scope of this test.
     * <p/>
     * This method is a shorthand that is equivalent to {@code ctx.tracked(ref, 1)}.
     * <p/>
     * For instance, this method is commonly used in cases where a reference counted object is
     * produced as a result of one or more decoder implementations within a channel pipeline:
     * <p/>
     * <pre>
     * {@code
     * @Test
     * void myTest(TestChannelContext ctx) {
     *   var ch = ctx.channel(...);
     *   var msg = ctx.output(ctx.readOutbound());
     * }
     * }
     * }
     * </pre>
     *
     * @param ref an output reference.
     * @param <R> a reference counted object type.
     * @return a tracked output reference.
     * @see #scoped(ReferenceCounted) scoped(ReferenceCounted) for more detailed information.
     */
    default <R extends ReferenceCounted> R output(R ref) {
        return this.tracked(ref, 1);
    }

    /**
     * Tracks a given output buffer within the scope of this test.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.tracked(ByteBufAllocator.DEFAULT.buffer(), 1)}.
     *
     * @return a tracked output buffer.
     * @see #output(ReferenceCounted) output(ReferenceCounted) for more detailed information.
     */
    default ByteBuf outputBuffer() {
        return this.output(ByteBufAllocator.DEFAULT.buffer());
    }

    /**
     * Tracks a given output buffer with a given initial capacity within the scope of this test.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.tracked(ByteBufAllocator.DEFAULT.buffer(initialCapacity), 1)}.
     *
     * @param initialCapacity the initial buffer capacity.
     * @return a tracked output buffer.
     * @see #output(ReferenceCounted) output(ReferenceCounted) for more detailed information.
     */
    default ByteBuf outputBuffer(int initialCapacity) {
        return this.output(ByteBufAllocator.DEFAULT.buffer(initialCapacity));
    }

    /**
     * Tracks a given output buffer with a given initial and maximum capacity within the scope of this
     * test.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ctx.tracked(ByteBufAllocator.DEFAULT.buffer(), 1)}.
     *
     * @param initialCapacity the initial buffer capacity.
     * @param maxCapacity     the maximum buffer capacity.
     * @return a tracked output buffer.
     * @see #output(ReferenceCounted) output(ReferenceCounted) for more detailed information.
     */
    default ByteBuf outputBuffer(int initialCapacity, int maxCapacity) {
        return this.output(ByteBufAllocator.DEFAULT.buffer(initialCapacity, maxCapacity));
    }

    /**
     * Creates a new embedded channel with a given set of handlers pre-installed within its pipeline.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ConnectionMockFactory.newFactory().createChannel(ctx::create, handlers)}.
     *
     * @param handlers an array of handlers to install on the channel pipeline.
     * @return a scoped embedded channel.
     * @see #channel(ChannelHandler...) create(ChannelHandler...) for additional information.
     */
    default EmbeddedChannel withConnection(ChannelHandler... handlers) {
        return ConnectionMockFactory.newFactory().createChannel(this::channel, handlers);
    }

    /**
     * Creates a new embedded channel witha given set of handlers pre-installed within its pipeline.
     * <p/>
     * This method is a shorthand that is equivalent to
     * {@code ConnectionMockFactory.newFactory().<configurer>.createChannel(ctx::create, handlers)}.
     *
     * @param configurer a configurer function which customizes the connection mock.
     * @param handlers   an array of handlers to install on the channel pipeline.
     * @return a scoped embedded channel.
     */
    default EmbeddedChannel withConnection(Consumer<ConnectionMockFactory> configurer, ChannelHandler... handlers) {
        var mock = ConnectionMockFactory.newFactory();
        configurer.accept(mock);
        return mock.createChannel(this::channel, handlers);
    }

    /**
     * Provides an extension to the strict buffer context interface which permits the creation of
     * sub-contexts.
     * <p/>
     * This functionality is primarily designed for test factories where the leak validation would
     * otherwise take place after <b>all constructed tests</b> have been invoked.
     */
    interface RootStrictBufferContext extends StrictBufferContext {

        /**
         * Constructs a child context which will be scoped to the lifetime of the main test.
         *
         * @param description a description which will appear within failure reports to provide
         *                    additional context.
         * @return a child context.
         */
        StrictBufferContext child(String description);

        /**
         * Constructs a dynamic test with a given description along with a dedicated child context.
         *
         * @param description a description for the test execution.
         * @param testFunc    a test function.
         * @return a dynamic test.
         */
        default DynamicTest test(String description, Consumer<StrictBufferContext> testFunc) {
            return DynamicTest.dynamicTest(description, () -> {
                try (var ctx = this.child(description)) {
                    testFunc.accept(ctx);
                }
            });
        }
    }
}
