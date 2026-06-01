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
package org.neo4j.annotations.service;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static javax.tools.Diagnostic.Kind.ERROR;
import static javax.tools.Diagnostic.Kind.NOTE;
import static javax.tools.StandardLocation.CLASS_OUTPUT;
import static org.neo4j.annotations.AnnotationConstants.DEFAULT_NEW_LINE;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.FileObject;

/**
 * Handles {@link Service} and {@link ServiceProvider} annotations. For each service type it collects associated service providers and creates
 * corresponding configuration file in {@code /META-INF/services/}.
 */
public class ServiceAnnotationProcessor extends AbstractProcessor {
    private static final boolean ENABLE_DEBUG = Boolean.getBoolean("enableAnnotationLogging");
    private final Map<TypeElement, List<TypeElement>> serviceProviders = new ConcurrentHashMap<>();
    private final String newLine;
    private Types typeUtils;
    private Elements elementUtils;

    public ServiceAnnotationProcessor() {
        this(DEFAULT_NEW_LINE);
    }

    public ServiceAnnotationProcessor(String newLine) {
        this.newLine = newLine;
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        typeUtils = processingEnv.getTypeUtils();
        elementUtils = processingEnv.getElementUtils();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Set.of(ServiceProvider.class.getName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        try {
            if (roundEnv.processingOver()) {
                if (!roundEnv.errorRaised()) {
                    generateConfigs();
                }
            } else {
                scan(roundEnv);
            }
        } catch (Exception e) {
            error("Service annotation processor failed", e);
        }
        return false;
    }

    private void scan(RoundEnvironment roundEnv) {
        final Set<TypeElement> elements = roundEnv.getElementsAnnotatedWith(ServiceProvider.class).stream()
                .map(TypeElement.class::cast)
                .collect(toSet());
        info("Processing service providers: "
                + elements.stream().map(Object::toString).sorted().toList());
        for (TypeElement serviceProvider : elements) {
            getImplementedService(serviceProvider).ifPresent(service -> {
                info(format("Service %s provided by %s", service, serviceProvider));
                serviceProviders
                        .computeIfAbsent(service, k -> new ArrayList<>())
                        .add(serviceProvider);
            });
        }
    }

    private Optional<TypeElement> getImplementedService(TypeElement serviceProvider) {
        final Set<TypeMirror> types = getTypeWithSupertypes(serviceProvider.asType());
        final List<TypeMirror> services = types.stream().filter(this::isService).toList();

        if (services.isEmpty()) {
            error(
                    format(
                            "Service provider %s neither has ascendants nor itself annotated with @Service)",
                            serviceProvider),
                    serviceProvider);
            return Optional.empty();
        }

        if (services.size() > 1) {
            error(
                    format(
                            "Service provider %s has multiple ascendants annotated with @Service: %s",
                            serviceProvider, services),
                    serviceProvider);
            return Optional.empty();
        }

        return Optional.of((TypeElement) typeUtils.asElement(services.get(0)));
    }

    private boolean isService(TypeMirror type) {
        return typeUtils.asElement(type).getAnnotation(Service.class) != null;
    }

    private Set<TypeMirror> getTypeWithSupertypes(TypeMirror type) {
        final Set<TypeMirror> allTypes = new HashSet<>();
        allTypes.add(type);
        final List<? extends TypeMirror> directSupertypes = typeUtils.directSupertypes(type);
        directSupertypes.forEach(directSupertype -> allTypes.addAll(getTypeWithSupertypes(directSupertype)));
        return allTypes;
    }

    private void generateConfigs() throws IOException {
        for (final Map.Entry<TypeElement, List<TypeElement>> entry : serviceProviders.entrySet()) {
            final TypeElement service = entry.getKey();
            final String path = "META-INF/services/" + elementUtils.getBinaryName(service);
            info("Generating service config file: " + path);

            final SortedSet<String> oldProviders = loadIfExists(path);
            final SortedSet<String> newProviders = new TreeSet<>();

            entry.getValue().forEach(providerType -> {
                final String providerName =
                        elementUtils.getBinaryName(providerType).toString();
                newProviders.add(providerName);
            });

            if (oldProviders.containsAll(newProviders)) {
                info("No new service providers found");
                return;
            }
            newProviders.addAll(oldProviders);

            final FileObject file = processingEnv.getFiler().createResource(CLASS_OUTPUT, "", path);
            try (BufferedWriter writer = new BufferedWriter(file.openWriter())) {
                info("Writing service providers: " + newProviders);
                for (final String provider : newProviders) {
                    writer.write(provider);
                    writer.write(newLine);
                }
            }
        }
    }

    private SortedSet<String> loadIfExists(String path) {
        final SortedSet<String> result = new TreeSet<>();
        try {
            final FileObject file = processingEnv.getFiler().getResource(CLASS_OUTPUT, "", path);
            final List<String> lines = new ArrayList<>();
            try (BufferedReader in =
                    new BufferedReader(new InputStreamReader(file.openInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    lines.add(line);
                }
            }
            lines.stream()
                    .map(ServiceAnnotationProcessor::substringBeforeHash)
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .forEach(result::add);
            info("Loaded existing providers: " + result);
        } catch (IOException ignore) {
            info("No existing providers loaded");
        }
        return result;
    }

    private void info(String msg) {
        if (ENABLE_DEBUG) {
            processingEnv.getMessager().printMessage(NOTE, msg);
        }
    }

    private void error(String msg, Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw, true));
        processingEnv.getMessager().printMessage(ERROR, msg + ": " + sw.toString());
    }

    private void error(String msg, Element element) {
        processingEnv.getMessager().printMessage(ERROR, msg, element);
    }

    private static String substringBeforeHash(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        final int pos = str.indexOf("#");
        if (pos == -1) {
            return str;
        }
        return str.substring(0, pos);
    }
}
