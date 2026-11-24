/*
 * Copyright the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.citrusframework.distribution.junit;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.ClasspathPackager;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.QuarkusWorkspace;
import org.citrusframework.distribution.model.ArtifactKind;
import org.citrusframework.distribution.model.ArtifactRef;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import tools.jackson.databind.JsonNode;

/** Packages Maven's Quarkus test model and the artifacts that its augmentation classloader needs. */
record QuarkusBootstrapInputs(QuarkusWorkspace layout, List<WorkspaceFileSet> inputs) {

    static Path findModel(Path moduleDir) {
        String configured = System.getProperty(QuarkusWorkspace.MODEL_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return null;
        }
        return moduleDir.resolve(configured).normalize();
    }

    static QuarkusBootstrapInputs prepare(Path model, Path moduleDir, Path buildDir, List<Path> classpath,
                                          Set<Path> mavenOutputs, Supplier<BlobCacheDirectory> cache,
                                          List<WorkspaceFileSet> existingInputs)
            throws IOException {
        if (!Files.isRegularFile(model) || Files.size(model) > ClasspathPackager.MAX_BLOB_BYTES) {
            throw new IOException("Missing or oversized Quarkus application model: " + model);
        }
        JsonNode json = readModel(model);
        if (json == null || !json.path("app-artifact").isObject() || !json.path("dependencies").isArray()) {
            throw new IOException("Invalid Quarkus application model: " + model);
        }

        Map<Path, Integer> classpathIndices = new LinkedHashMap<>();
        Map<Integer, String> classpathDirectories = new LinkedHashMap<>();
        for (int index = 0; index < classpath.size(); index++) {
            classpathIndices.putIfAbsent(classpath.get(index), index);
        }

        List<JsonNode> artifacts = new ArrayList<>();
        artifacts.add(json.path("app-artifact"));
        json.path("dependencies").forEach(artifacts::add);

        Set<Path> requiredPaths = new LinkedHashSet<>();
        Map<Path, String> moduleRoots = new LinkedHashMap<>();
        moduleRoots.put(moduleDir, "");
        if (!buildDir.startsWith(moduleDir)) {
            moduleRoots.put(buildDir, DistributedLauncher.WorkspaceSettings.externalBuildDirectory(moduleDir, buildDir));
        }
        for (JsonNode artifact : artifacts) {
            JsonNode module = artifact.path("module");
            Path root = absolutePath(module.path("module-dir").asString(""));
            if (root != null) {
                moduleRoots.computeIfAbsent(root, path -> WorkspacePackager.DEPENDENCY_STAGING_DIR
                        + "modules/" + pathHash(path));
            }
            addPaths(artifact.path("resolved-paths"), requiredPaths);
            addPaths(module.path("test-additional-cp-elements"), requiredPaths);
        }
        List<Path> roots = moduleRoots.keySet().stream()
                .sorted(Comparator.comparingInt(Path::getNameCount).reversed()).toList();

        for (int index = 0; index < classpath.size(); index++) {
            Path path = classpath.get(index);
            if (Files.isDirectory(path) && ((path.startsWith(buildDir) && !path.equals(buildDir))
                    || mavenOutputs.contains(path) || requiredPaths.contains(path))) {
                classpathDirectories.put(index, location(path, Map.of(), moduleRoots, roots).workspacePath());
            }
        }

        Map<String, QuarkusWorkspace.Location> locations = new LinkedHashMap<>();
        QuarkusWorkspace.rewriteStrings(json, value -> {
            Path path = absolutePath(value);
            if (path != null) {
                locations.putIfAbsent(value, location(path, classpathIndices, moduleRoots, roots));
            }
            return value;
        });

        List<WorkspaceFileSet> inputs = new ArrayList<>();
        // Stage the generated model alone so an external or relocated model does not pull in unrelated siblings.
        String modelPath = WorkspacePackager.DEPENDENCY_STAGING_DIR + "quarkus/application-model.json";
        byte[] modelBytes = DistributionJson.mapper().writeValueAsBytes(json);
        String digest = ClasspathPackager.sha256Hex(modelBytes);
        cache.get().put(digest, modelBytes);
        inputs.add(new WorkspaceFileSet(modelPath,
                new ArtifactRef("application-model.json", digest, ArtifactKind.REGULAR_FILE, modelBytes.length)));
        for (Path required : requiredPaths) {
            QuarkusWorkspace.Location location = location(required, classpathIndices, moduleRoots, roots);
            if (location.classpathIndex() != null || coveredByInput(location.workspacePath(), existingInputs)) {
                continue;
            }
            WorkspaceFileSet artifact = WorkspacePackager.packageInputToCache(cache.get(), required.getParent(),
                    required.getFileName().toString(), false).orElseThrow();
            inputs.add(new WorkspaceFileSet(location.workspacePath(), artifact.artifact()));
        }
        return new QuarkusBootstrapInputs(
                new QuarkusWorkspace(modelPath, classpathDirectories, locations), List.copyOf(inputs));
    }

    private static JsonNode readModel(Path model) throws IOException {
        try (var stream = Files.newInputStream(model)) {
            if (stream.read() != 0xac || stream.read() != 0xed) {
                return DistributionJson.mapper().readTree(model.toFile());
            }
        }

        // Quarkus can emit a Java-serialized model. Convert it using the build's Quarkus model API before transfer.
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> modelType = Class.forName("io.quarkus.bootstrap.model.ApplicationModel", false, loader);
            Class<?> factoryType = Class.forName("io.quarkus.bootstrap.model.MappableCollectionFactory", false, loader);
            var asMap = modelType.getMethod("asMap", factoryType);
            Object factory = factoryType.getMethod("defaultInstance").invoke(null);
            try (var input = new ObjectInputStream(Files.newInputStream(model)) {
                @Override
                protected Class<?> resolveClass(ObjectStreamClass descriptor) throws ClassNotFoundException, IOException {
                    try {
                        return Class.forName(descriptor.getName(), false, loader);
                    } catch (ClassNotFoundException e) {
                        return super.resolveClass(descriptor);
                    }
                }
            }) {
                Object applicationModel = modelType.cast(input.readObject());
                return DistributionJson.mapper().valueToTree(asMap.invoke(applicationModel, factory));
            }
        } catch (ReflectiveOperationException | LinkageError | ClassCastException e) {
            throw new IOException("Cannot convert the Quarkus Java-serialized model to JSON: " + model
                    + ". Use a Quarkus version with JSON model support.", e);
        }
    }

    private static QuarkusWorkspace.Location location(Path path, Map<Path, Integer> classpath,
                                                       Map<Path, String> moduleRoots, List<Path> roots) {
        Integer index = classpath.get(path);
        if (index != null) {
            return new QuarkusWorkspace.Location(index, null);
        }
        for (Path root : roots) {
            if (path.startsWith(root)) {
                String prefix = moduleRoots.get(root);
                String rel = relativePath(root, path);
                String destination = prefix.isEmpty() ? rel : rel.isEmpty() ? prefix : prefix + "/" + rel;
                return new QuarkusWorkspace.Location(null, destination);
            }
        }
        return new QuarkusWorkspace.Location(null,
                WorkspacePackager.DEPENDENCY_STAGING_DIR + "artifacts/"
                        + pathHash(path.getParent()) + "/" + path.getFileName());
    }

    private static void addPaths(JsonNode array, Set<Path> paths) {
        for (JsonNode entry : array) {
            Path path = absolutePath(entry.asString(""));
            if (path != null) {
                paths.add(path);
            }
        }
    }

    private static Path absolutePath(String value) {
        boolean windowsAbsolute = File.separatorChar == '\\'
                && (value.startsWith("\\\\") || (value.length() >= 3 && value.charAt(1) == ':'
                        && (value.charAt(2) == '\\' || value.charAt(2) == '/')));
        if (value.startsWith("/") || windowsAbsolute) {
            Path path = Path.of(value).normalize();
            if (path.isAbsolute() && path.getParent() != null) {
                return path;
            }
        }
        return null;
    }

    private static String pathHash(Path path) {
        return ClasspathPackager.sha256Hex(path.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static String relativePath(Path root, Path path) {
        return root.relativize(path).toString().replace(File.separatorChar, '/');
    }

    private static boolean coveredByInput(String path, List<WorkspaceFileSet> inputs) {
        return inputs.stream().anyMatch(input -> path.equals(input.relativePath())
                || path.startsWith(input.relativePath() + "/"));
    }
}
