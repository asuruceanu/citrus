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
 * token keeps its second backslash. New Windows submissions canonicalize path separators on the build host.
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
        return rewriteHostProperties(props, hostBaseDir, null, "");
    }

    /** Also maps a build directory that is staged separately from the module root. */
    public static Map<String, String> rewriteHostProperties(Map<String, String> props, Path hostBaseDir,
                                                            Path hostBuildDir, String workspaceBuildDir) {
        Map<String, String> result = new LinkedHashMap<>();
        if (props == null) {
            return result;
        }
        Map<String, String> replacements = new LinkedHashMap<>();
        if (!workspaceBuildDir.isEmpty()) {
            hostPrefixCandidates(hostBuildDir).forEach(root -> replacements.put(root,
                    DistributionSettings.WORKSPACE_TOKEN + "/" + workspaceBuildDir));
        }
        hostPrefixCandidates(hostBaseDir).forEach(root -> replacements.put(root, DistributionSettings.WORKSPACE_TOKEN));
        List<String> candidates = replacements.keySet().stream().sorted(Comparator.comparingInt(String::length).reversed()).toList();
        props.forEach((key, value) ->
                result.put(key, rewriteValue(value, candidates, WINDOWS, WINDOWS, replacements)));
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
        return rewriteValue(value, candidates, ignoreCase, false);
    }

    static String rewriteValue(String value, List<String> candidates, boolean ignoreCase, boolean canonicalize) {
        return rewriteValue(value, candidates, ignoreCase, canonicalize, Map.of());
    }

    private static String rewriteValue(String value, List<String> candidates, boolean ignoreCase, boolean canonicalize,
                                       Map<String, String> replacements) {
        if (value == null || value.isEmpty() || candidates.isEmpty()) {
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
                            && matchesPrefix(value, i, candidate, ignoreCase, canonicalize)
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
            out.append(value, copiedUpTo, i).append(replacements.getOrDefault(match, DistributionSettings.WORKSPACE_TOKEN));
            boolean singleQuoted = i > 0 && value.charAt(i - 1) == '\'';
            boolean embedded = i > (singleQuoted ? 1 : 0);
            i += match.length();
            if (canonicalize && i < value.length() && (value.charAt(i) == '/' || value.charAt(i) == '\\')) {
                // Normalize only the matched Windows path. Leave later list entries and other values untouched.
                boolean escaped = match.contains("\\\\");
                while (i < value.length() && !endsWindowsPath(value, i, singleQuoted, embedded)) {
                    char c = value.charAt(i++);
                    if (c == '\\') {
                        if (escaped && i < value.length() && value.charAt(i) == '\\') {
                            i++;
                        }
                        c = '/';
                    }
                    out.append(c);
                }
            }
            copiedUpTo = i;
        }
        if (out == null) {
            return value;
        }
        return out.append(value, copiedUpTo, value.length()).toString();
    }

    private static boolean matchesPrefix(String value, int offset, String root, boolean ignoreCase, boolean canonicalize) {
        if (value.regionMatches(ignoreCase, offset, root, 0, root.length())) {
            return true;
        }
        if (!canonicalize) {
            return false;
        }
        for (int i = 0; i < root.length(); i++) {
            char candidate = root.charAt(i);
            char current = value.charAt(offset + i);
            if (!(isSeparator(candidate) && isSeparator(current))
                    && !value.regionMatches(ignoreCase, offset + i, root, i, 1)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isSeparator(char c) {
        return c == '/' || c == '\\';
    }

    private static boolean endsWindowsPath(String value, int offset, boolean singleQuoted, boolean embedded) {
        char c = value.charAt(offset);
        if (":\"<>|?*".indexOf(c) >= 0) {
            return true;
        }
        if (c != ',' && c != ';' && c != '\'') {
            return false;
        }
        // These characters are legal inside a filename. Treat them as separators only at another value or a closing quote.
        if (c == '\'' && singleQuoted && (offset + 1 == value.length() || ";,) ]".indexOf(value.charAt(offset + 1)) >= 0)) {
            return true;
        }
        int next = offset + 1;
        while (next < value.length() && Character.isWhitespace(value.charAt(next))) {
            next++;
        }
        if (next + 2 < value.length() && (value.startsWith("file:", next)
                || (Character.isLetter(value.charAt(next)) && value.charAt(next + 1) == ':' && isSeparator(value.charAt(next + 2)))
                || (isSeparator(value.charAt(next)) && isSeparator(value.charAt(next + 1))))) {
            return true;
        }
        return c == ';' && embedded && startsPropertyAssignment(value, next);
    }

    private static boolean startsPropertyAssignment(String value, int next) {
        int end = next;
        while (end < value.length()) {
            char c = value.charAt(end);
            if (!Character.isLetterOrDigit(c) && c != '_' && c != '-' && c != '.') {
                break;
            }
            end++;
        }
        return end > next && end < value.length() && value.charAt(end) == '=';
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
