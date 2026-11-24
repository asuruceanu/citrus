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
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;

import org.citrusframework.distribution.config.ForkedJvmOptions;
import org.citrusframework.distribution.scheduler.CapabilityMatcher;

/** Resolves the file-based parts of the build JVM launch before packaging them for a worker. */
record TestJvmClasspath(List<Path> artifacts, List<Integer> classpath, List<ForkedJvmOptions.PathOption> pathOptions) {

    private static final Set<String> MODULE_PATH_OPTIONS = Set.of("--module-path", "--upgrade-module-path");

    static TestJvmClasspath capture(List<Path> extraClasspath) throws IOException {
        List<ModulePaths> options = modulePaths(ManagementFactory.getRuntimeMXBean().getInputArguments());
        Set<Path> moduleFiles = new LinkedHashSet<>();
        options.forEach(option -> moduleFiles.addAll(option.paths()));

        // Surefire exposes the full test classpath even when the physical classpath is a booter JAR.
        String cp = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path", ""));
        Set<Path> classpath = new LinkedHashSet<>(paths(cp));
        classpath.removeAll(moduleFiles);
        classpath.addAll(extraClasspath);
        List<Path> artifacts = new ArrayList<>(classpath);
        Map<Path, Integer> indices = new LinkedHashMap<>();
        for (int i = 0; i < artifacts.size(); i++) {
            indices.put(artifacts.get(i), i);
        }
        List<ForkedJvmOptions.PathOption> indexedOptions = new ArrayList<>();
        for (ModulePaths option : options) {
            List<Integer> locations = new ArrayList<>();
            for (Path path : option.paths()) {
                locations.add(indices.computeIfAbsent(path, key -> {
                    artifacts.add(key);
                    return artifacts.size() - 1;
                }));
            }
            indexedOptions.add(new ForkedJvmOptions.PathOption(option.name(), option.module(), List.copyOf(locations)));
        }
        return new TestJvmClasspath(List.copyOf(artifacts), IntStream.range(0, classpath.size()).boxed().toList(),
                List.copyOf(indexedOptions));
    }

    private static List<ModulePaths> modulePaths(List<String> args) throws IOException {
        List<ModulePaths> options = new ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            int separator = arg.indexOf('=');
            String name = separator < 0 ? arg : arg.substring(0, separator);
            name = "-p".equals(name) ? "--module-path" : name;
            if (!MODULE_PATH_OPTIONS.contains(name) && !"--patch-module".equals(name)) {
                continue;
            }
            String value = separator < 0 ? args.get(++i) : arg.substring(separator + 1);
            if ("--patch-module".equals(name)) {
                int moduleEnd = value.indexOf('=');
                options.add(new ModulePaths(name, value.substring(0, moduleEnd), paths(value.substring(moduleEnd + 1))));
            } else {
                List<Path> modules = new ArrayList<>();
                for (Path path : paths(value)) {
                    if (Files.isDirectory(path) && !Files.isRegularFile(path.resolve("module-info.class"))) {
                        try (var entries = Files.list(path)) {
                            modules.addAll(entries.filter(entry -> entry.toString().endsWith(".jar")
                                    || Files.isRegularFile(entry.resolve("module-info.class"))).sorted().toList());
                        }
                    } else {
                        modules.add(path);
                    }
                }
                options.add(new ModulePaths(name, null, List.copyOf(modules)));
            }
        }
        return options;
    }

    private static List<Path> paths(String value) {
        return Arrays.stream(value.split(File.pathSeparator)).filter(entry -> !entry.isBlank())
                .map(entry -> Path.of(entry).toAbsolutePath().normalize()).filter(Files::exists).toList();
    }

    JavaRelease javaRelease(boolean previewEnabled) throws IOException {
        // The distribution runtime requires Java 21. Multi-release entries are selected by the worker JVM.
        JavaRelease result = new JavaRelease(21, previewEnabled ? Runtime.version().feature() : 0);
        for (Path path : artifacts) {
            if (Files.isDirectory(path)) {
                try (var files = Files.walk(path)) {
                    for (Path file : (Iterable<Path>) files.filter(Files::isRegularFile)
                            .filter(file -> isBaseClass(path.relativize(file).toString().replace(File.separatorChar, '/')))::iterator) {
                        try (InputStream input = Files.newInputStream(file)) {
                            result = result.include(input);
                        }
                    }
                }
            } else if (path.toString().endsWith(".jar")) {
                try (JarFile jar = new JarFile(path.toFile())) {
                    var entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        ZipEntry entry = entries.nextElement();
                        if (!entry.isDirectory() && isBaseClass(entry.getName())) {
                            try (InputStream input = jar.getInputStream(entry)) {
                                result = result.include(input);
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    private static boolean isBaseClass(String name) {
        return name.endsWith(".class") && !name.startsWith("META-INF/versions/");
    }

    record JavaRelease(int minimum, int preview) {

        private JavaRelease include(InputStream input) throws IOException {
            byte[] header = input.readNBytes(8);
            if (header.length != 8 || (header[0] & 255) != 0xca || (header[1] & 255) != 0xfe
                    || (header[2] & 255) != 0xba || (header[3] & 255) != 0xbe) {
                return this;
            }
            int release = ((header[6] & 255) << 8 | (header[7] & 255)) - 44;
            if (header[4] == -1 && header[5] == -1 && preview != release && preview != 0) {
                throw new IOException("Test classpath contains preview bytecode for different Java releases");
            }
            int exact = header[4] == -1 && header[5] == -1 ? release : preview;
            return release <= minimum && exact == preview ? this : new JavaRelease(Math.max(release, minimum), exact);
        }

        void requireWorker(Map<String, String> capabilities) {
            String explicit = capabilities.get("jdk");
            if (preview > 0) {
                String exact = Integer.toString(preview);
                if (minimum > preview || (explicit != null
                        && !CapabilityMatcher.matches(Map.of("jdk", explicit), Map.of("jdk", exact)))) {
                    throw new IllegalArgumentException("Test bytecode requires Java " + preview
                            + " with preview features but has incompatible JDK requirements");
                }
                capabilities.put("jdk", exact);
            } else if (explicit == null) {
                capabilities.put("jdk", ">=" + minimum);
            } else if (explicit.trim().startsWith(">=")) {
                capabilities.put("jdk", ">=" + Math.max(minimum, Integer.parseInt(explicit.trim().substring(2).trim())));
            } else if (Integer.parseInt(explicit.trim()) < minimum) {
                throw new IllegalArgumentException("Test bytecode requires Java " + minimum + " or newer, but jdk=" + explicit);
            }
        }
    }

    private record ModulePaths(String name, String module, List<Path> paths) {
    }
}
