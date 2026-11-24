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
import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.model.WorkspaceFileSet;
import tools.jackson.databind.JsonNode;

/** Packages Maven's Quarkus test model and the artifacts that its augmentation classloader needs. */
record QuarkusBootstrapInputs(QuarkusWorkspace layout, List<WorkspaceFileSet> inputs) {

    private static final Path BOOTSTRAP_DIR = Path.of("quarkus", "bootstrap");

    static Path findModel(Path moduleDir) {
        String configured = System.getProperty(QuarkusWorkspace.MODEL_PROPERTY);
        if (configured == null || configured.isBlank()) {
            return null;
        }
        Path model = moduleDir.resolve(configured).normalize();
        Path parent = model.getParent();
        return parent != null && parent.startsWith(moduleDir) && parent.endsWith(BOOTSTRAP_DIR) ? model : null;
    }

    static QuarkusBootstrapInputs prepare(Path model, Path moduleDir, List<Path> classpath,
                                          Supplier<BlobCacheDirectory> cache, List<WorkspaceFileSet> existingInputs)
            throws IOException {
        JsonNode json = DistributionJson.mapper().readTree(model.toFile());
        if (json == null || !json.path("app-artifact").isObject() || !json.path("dependencies").isArray()) {
            throw new IOException("Invalid Quarkus application model: " + model);
        }

        Map<Path, Integer> classpathIndices = new LinkedHashMap<>();
        Map<Integer, String> classpathDirectories = new LinkedHashMap<>();
        Path buildDir = model.getParent().getParent().getParent();
        for (int index = 0; index < classpath.size(); index++) {
            Path path = classpath.get(index);
            classpathIndices.putIfAbsent(path, index);
            if (path.startsWith(buildDir) && !path.equals(buildDir) && Files.isDirectory(path)) {
                classpathDirectories.put(index, relativePath(moduleDir, path));
            }
        }

        List<JsonNode> artifacts = new ArrayList<>();
        artifacts.add(json.path("app-artifact"));
        json.path("dependencies").forEach(artifacts::add);

        Set<Path> requiredPaths = new LinkedHashSet<>();
        Map<Path, String> moduleRoots = new LinkedHashMap<>();
        moduleRoots.put(moduleDir, "");
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

        Map<String, QuarkusWorkspace.Location> locations = new LinkedHashMap<>();
        QuarkusWorkspace.rewriteStrings(json, value -> {
            Path path = absolutePath(value);
            if (path != null) {
                locations.putIfAbsent(value, location(path, classpathIndices, moduleRoots, roots));
            }
            return value;
        });

        List<WorkspaceFileSet> inputs = new ArrayList<>();
        String bootstrapDir = relativePath(moduleDir, model.getParent());
        if (!coveredByInput(bootstrapDir, existingInputs)) {
            inputs.add(WorkspacePackager.packageInputToCache(cache.get(), moduleDir, bootstrapDir, false).orElseThrow());
        }
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
                new QuarkusWorkspace(relativePath(moduleDir, model), classpathDirectories, locations), List.copyOf(inputs));
    }

    static WorkUnitSpec requireCompatibleWorker(WorkUnitSpec unit) {
        Map<String, String> capabilities = new LinkedHashMap<>(unit.requiredCapabilities());
        capabilities.put(QuarkusWorkspace.CAPABILITY, "true");
        return new WorkUnitSpec(unit.key(), unit.className(), unit.methodName(), unit.dynamicSourceName(), capabilities,
                unit.estimatedDurationMs(), unit.enclosingClassNames());
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
