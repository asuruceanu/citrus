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

package org.citrusframework.distribution.config;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites host module base directory references in forwarded system properties to the
 * {@link DistributionSettings#WORKSPACE_TOKEN} placeholder on the host, and expands that placeholder
 * to the partition workspace directory on the worker.
 * <p>
 * A host prefix is only replaced at path-segment boundaries, so sibling directories sharing a name
 * prefix ({@code /work/rest} vs {@code /work/restdocs}) and paths that merely contain the base
 * directory further in ({@code /other/work/rest}) are left untouched. Matching is case-insensitive on
 * Windows and case-sensitive elsewhere. URL-encoded or otherwise escaped forms (other than
 * Java-properties style doubled backslashes on Windows) are not recognized. On expansion only a
 * single separator directly after the token is normalized, so a doubled (escaped) backslash after the
 * token keeps its second backslash.
 */
public final class PathRewriter {

    /** URI prefixes after which a host path may start ({@code file:/C:/..}, {@code file:///work/..}). */
    private static final List<String> FILE_URI_PREFIXES = List.of("file:///", "file://", "file:/");

    private static final boolean WINDOWS = File.separatorChar == '\\';

    private PathRewriter() {
    }

    /**
     * Returns a copy of {@code props} where every boundary-delimited occurrence of {@code hostBaseDir}
     * (in its absolute, real, forward-slash and, on Windows, escaped-backslash forms) is replaced with
     * {@link DistributionSettings#WORKSPACE_TOKEN}. Iteration order and {@code null} values are preserved.
     * Nothing is rewritten when {@code hostBaseDir} is {@code null} or the filesystem root.
     */
    public static Map<String, String> rewriteHostProperties(Map<String, String> props, Path hostBaseDir) {
        Map<String, String> result = new LinkedHashMap<>();
        if (props == null) {
            return result;
        }
        List<String> candidates = hostPrefixCandidates(hostBaseDir);
        props.forEach((key, value) ->
                result.put(key, candidates.isEmpty() ? value : rewriteValue(value, candidates, WINDOWS)));
        return result;
    }

    /**
     * Returns a copy of {@code props} with every {@link DistributionSettings#WORKSPACE_TOKEN} replaced by
     * the absolute, normalized {@code workspaceDir}. A {@code /} or {@code \} directly following the token is
     * normalized to {@link File#separatorChar}. Iteration order and {@code null} values are preserved.
     */
    public static Map<String, String> expandWorkspaceProperties(Map<String, String> props, Path workspaceDir) {
        Map<String, String> result = new LinkedHashMap<>();
        if (props == null) {
            return result;
        }
        String workspace = workspaceDir.toAbsolutePath().normalize().toString();
        props.forEach((key, value) -> result.put(key, expand(value, workspace)));
        return result;
    }

    static List<String> hostPrefixCandidates(Path hostBaseDir) {
        if (hostBaseDir == null) {
            return List.of();
        }
        Path absolute = hostBaseDir.toAbsolutePath().normalize();
        if (absolute.getNameCount() == 0) {
            return List.of();
        }

        Set<String> forms = new LinkedHashSet<>(separatorForms(absolute.toString(), WINDOWS));
        try {
            Path real = absolute.toRealPath();
            if (real.getNameCount() > 0) {
                forms.addAll(separatorForms(real.toString(), WINDOWS));
            }
        } catch (IOException | SecurityException ignored) {
            // Base directory not resolvable on this host: the absolute form is the only candidate.
        }

        return forms.stream()
                .sorted(Comparator.comparingInt(String::length).reversed())
                .toList();
    }

    /**
     * Spellings of a native host path to look for: the native form and, on Windows, its forward-slash and
     * Java-properties escaped (doubled backslash) forms.
     */
    static List<String> separatorForms(String nativeForm, boolean windows) {
        if (!windows) {
            return List.of(nativeForm);
        }
        return new LinkedHashSet<>(List.of(
                nativeForm,
                nativeForm.replace('\\', '/'),
                nativeForm.replace("\\", "\\\\"))).stream().toList();
    }

    /**
     * Replaces boundary-delimited occurrences of {@code candidates} (ordered longest first) in {@code value}.
     */
    static String rewriteValue(String value, List<String> candidates, boolean ignoreCase) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        StringBuilder out = null;
        int copiedUpTo = 0;
        int i = 0;
        while (i < value.length()) {
            String match = null;
            if (isPrecedingBoundary(value, i)) {
                for (String candidate : candidates) {
                    int end = i + candidate.length();
                    if (end <= value.length()
                            && value.regionMatches(ignoreCase, i, candidate, 0, candidate.length())
                            && isFollowingBoundary(value, end)) {
                        match = candidate;
                        break;
                    }
                }
            }
            if (match == null) {
                i++;
                continue;
            }
            if (out == null) {
                out = new StringBuilder(value.length());
            }
            out.append(value, copiedUpTo, i).append(DistributionSettings.WORKSPACE_TOKEN);
            i += match.length();
            copiedUpTo = i;
        }
        if (out == null) {
            return value;
        }
        return out.append(value, copiedUpTo, value.length()).toString();
    }

    private static boolean isPrecedingBoundary(String value, int index) {
        if (index == 0) {
            return true;
        }
        for (String prefix : FILE_URI_PREFIXES) {
            int start = index - prefix.length();
            if (start >= 0 && value.regionMatches(true, start, prefix, 0, prefix.length())) {
                return true;
            }
        }
        char c = value.charAt(index - 1);
        return Character.isWhitespace(c)
                || c == '=' || c == ':' || c == ';' || c == ','
                || c == '"' || c == '\'' || c == '(' || c == '[' || c == '@';
    }

    private static boolean isFollowingBoundary(String value, int index) {
        if (index == value.length()) {
            return true;
        }
        char c = value.charAt(index);
        return Character.isWhitespace(c)
                || c == '/' || c == '\\' || c == ':' || c == ';' || c == ','
                || c == '"' || c == '\'' || c == ')' || c == ']';
    }

    private static String expand(String value, String workspace) {
        if (value == null) {
            return null;
        }
        String token = DistributionSettings.WORKSPACE_TOKEN;
        int at = value.indexOf(token);
        if (at < 0) {
            return value;
        }
        StringBuilder out = new StringBuilder(value.length() + workspace.length());
        int copiedUpTo = 0;
        while (at >= 0) {
            out.append(value, copiedUpTo, at).append(workspace);
            int next = at + token.length();
            if (next < value.length() && (value.charAt(next) == '/' || value.charAt(next) == '\\')) {
                out.append(File.separatorChar);
                next++;
            }
            copiedUpTo = next;
            at = value.indexOf(token, next);
        }
        return out.append(value, copiedUpTo, value.length()).toString();
    }
}
