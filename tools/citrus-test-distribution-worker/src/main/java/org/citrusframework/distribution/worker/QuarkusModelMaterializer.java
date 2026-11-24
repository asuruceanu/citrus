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

package org.citrusframework.distribution.worker;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.citrusframework.distribution.cache.BlobCacheDirectory;
import org.citrusframework.distribution.cache.WorkspacePackager;
import org.citrusframework.distribution.config.DistributionJson;
import org.citrusframework.distribution.config.QuarkusWorkspace;
import tools.jackson.databind.JsonNode;

/** Rewrites the copy of the Maven application model owned by this partition. */
final class QuarkusModelMaterializer {

    private QuarkusModelMaterializer() {
    }

    static Path rewrite(QuarkusWorkspace layout, Path workspace, List<Path> classpath) throws IOException {
        Path model = workspace.resolve(WorkspacePackager.validateRelativePath(layout.modelPath()));
        if (BlobCacheDirectory.findSymlinkComponent(workspace, model) != null) {
            throw new IOException("Symbolic link on Quarkus model path: " + model);
        }
        JsonNode json = DistributionJson.mapper().readTree(model.toFile());
        QuarkusWorkspace.rewriteStrings(json, value -> {
            QuarkusWorkspace.Location location = layout.modelLocations().get(value);
            if (location == null) {
                return value;
            }
            if (location.classpathIndex() != null) {
                return classpath.get(location.classpathIndex()).toString();
            }
            String relative = location.workspacePath();
            return relative.isEmpty() ? workspace.toString()
                    : workspace.resolve(WorkspacePackager.validateRelativePath(relative)).toString();
        });
        DistributionJson.mapper().writeValue(model.toFile(), json);
        return model;
    }
}
