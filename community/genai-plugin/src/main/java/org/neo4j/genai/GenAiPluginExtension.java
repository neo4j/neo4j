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
package org.neo4j.genai;

import java.util.function.Supplier;
import org.eclipse.collections.api.list.ImmutableList;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.genai.ai.image.embed.ImageVectorEmbedding;
import org.neo4j.genai.ai.text.chat.TextChat;
import org.neo4j.genai.ai.text.completion.TextCompletion;
import org.neo4j.genai.ai.text.embed.VectorEmbedding;
import org.neo4j.genai.ai.text.structuredCompletion.TextStructuredCompletion;
import org.neo4j.genai.ai.text.tokenCount.TextTokenCount;
import org.neo4j.genai.util.HttpService;
import org.neo4j.genai.util.provider.GlobalProviders;
import org.neo4j.genai.util.provider.NamedProvider;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.service.Services;
import org.neo4j.util.VisibleForTesting;

@ServiceProvider
public class GenAiPluginExtension extends ExtensionFactory<GenAiPluginExtension.Dependencies> {
    private final Supplier<GlobalProviders> providersSupplier;

    public GenAiPluginExtension() {
        this(new ServiceLoadedGlobalProviders());
    }

    @VisibleForTesting
    public GenAiPluginExtension(NamedProvider... providers) {
        this(() -> GlobalProviders.from(providers));
    }

    private GenAiPluginExtension(final Supplier<GlobalProviders> providersSupplier) {
        super(ExtensionType.GLOBAL, "gen-ai-plugin-extension");
        this.providersSupplier = providersSupplier;
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return new LifecycleAdapter() {
            @Override
            public void init() {
                final var providers = providersSupplier.get();
                final var httpService = new HttpServiceProvider();

                registerSafe(HttpService.class, httpService);
                registerSafe(TextCompletion.Providers.class, TxtCompProv.from(httpService, providers));
                registerSafe(TextChat.Providers.class, TxtChatProv.from(httpService, providers));
                registerSafe(TextTokenCount.Providers.class, TxtTokenCountProv.from(httpService, providers));
                registerSafe(TextStructuredCompletion.Providers.class, TxtCompStructProv.from(httpService, providers));
                registerSafe(VectorEmbedding.Providers.class, VectorEmbeddingProv.from(httpService, providers));
                registerSafe(ImageVectorEmbedding.Providers.class, ImgVectorEmbeddingProv.from(httpService, providers));

                // This component is used by metrics, can't use context.
                // It is registered without a context, so it can be accessed by the metrics module.
                dependencies.procedures().registerComponent(GlobalProviders.class, (ctx) -> providers, false);
            }

            private <T> void registerSafe(Class<T> cls, ProcedureProvider<T> provider) {
                dependencies.procedures().registerComponent(cls, provider, true);
            }
        };
    }

    public interface Dependencies {
        GlobalProcedures procedures();
    }
}

final class ServiceLoadedGlobalProviders implements Supplier<GlobalProviders> {
    private static final NamedProvider[] PROVIDERS =
            Services.loadAll(NamedProvider.class).toArray(NamedProvider[]::new);

    @Override
    public GlobalProviders get() {
        return GlobalProviders.from(PROVIDERS);
    }
}

interface ProcedureProvider<T> extends ThrowingFunction<Context, T, ProcedureException> {}

class HttpServiceProvider implements ProcedureProvider<HttpService> {
    public HttpService apply(Context ctx) throws ProcedureException {
        return new HttpService(ctx.urlAccessChecker());
    }
}

record TxtCompProv(HttpServiceProvider httpService, ImmutableList<TextCompletion.Provider> providers)
        implements ProcedureProvider<TextCompletion.Providers> {
    public TextCompletion.Providers apply(Context context) throws ProcedureException {
        return new TextCompletion.Providers.Impl(providers, httpService.apply(context));
    }

    public static TxtCompProv from(HttpServiceProvider httpService, GlobalProviders globalProviders) {
        return new TxtCompProv(httpService, globalProviders.providers(TextCompletion.Provider.class));
    }
}

record TxtCompStructProv(HttpServiceProvider httpService, ImmutableList<TextStructuredCompletion.Provider> providers)
        implements ProcedureProvider<TextStructuredCompletion.Providers> {
    public TextStructuredCompletion.Providers apply(Context context) throws ProcedureException {
        return new TextStructuredCompletion.Providers.Impl(providers, httpService.apply(context));
    }

    public static TxtCompStructProv from(HttpServiceProvider httpService, GlobalProviders globalProviders) {
        return new TxtCompStructProv(httpService, globalProviders.providers(TextStructuredCompletion.Provider.class));
    }
}

record TxtChatProv(HttpServiceProvider httpService, ImmutableList<TextChat.Provider> providers)
        implements ProcedureProvider<TextChat.Providers> {
    public TextChat.Providers apply(Context context) throws ProcedureException {
        return new TextChat.Providers.Impl(providers, httpService.apply(context));
    }

    public static TxtChatProv from(HttpServiceProvider httpService, GlobalProviders globalProviders) {
        return new TxtChatProv(httpService, globalProviders.providers(TextChat.Provider.class));
    }
}

record TxtTokenCountProv(HttpServiceProvider httpService, ImmutableList<TextTokenCount.Provider> providers)
        implements ProcedureProvider<TextTokenCount.Providers> {
    public TextTokenCount.Providers apply(Context context) throws ProcedureException {
        return new TextTokenCount.Providers.Impl(providers, httpService.apply(context));
    }

    public static TxtTokenCountProv from(HttpServiceProvider httpService, GlobalProviders globalProviders) {
        return new TxtTokenCountProv(httpService, globalProviders.providers(TextTokenCount.Provider.class));
    }
}

record VectorEmbeddingProv(HttpServiceProvider httpService, ImmutableList<VectorEmbedding.Provider> providers)
        implements ProcedureProvider<VectorEmbedding.Providers> {
    public VectorEmbedding.Providers apply(Context context) throws ProcedureException {
        return new VectorEmbedding.Providers.Impl(providers, httpService.apply(context));
    }

    public static VectorEmbeddingProv from(HttpServiceProvider httpService, GlobalProviders globalProviders) {
        return new VectorEmbeddingProv(httpService, globalProviders.providers(VectorEmbedding.Provider.class));
    }
}

record ImgVectorEmbeddingProv(HttpServiceProvider httpService, ImmutableList<ImageVectorEmbedding.Provider> providers)
        implements ProcedureProvider<ImageVectorEmbedding.Providers> {
    public ImageVectorEmbedding.Providers apply(Context context) throws ProcedureException {
        return new ImageVectorEmbedding.Providers.Impl(providers, httpService.apply(context));
    }

    public static ImgVectorEmbeddingProv from(HttpServiceProvider httpService, GlobalProviders globalProviders) {
        return new ImgVectorEmbeddingProv(httpService, globalProviders.providers(ImageVectorEmbedding.Provider.class));
    }
}
