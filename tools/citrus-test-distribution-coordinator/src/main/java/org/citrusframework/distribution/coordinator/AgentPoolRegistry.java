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

package org.citrusframework.distribution.coordinator;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.citrusframework.distribution.model.AgentPoolRecord;
import org.citrusframework.distribution.model.GeneratedRegistrationKey;
import org.citrusframework.distribution.model.RegistrationKeyRecord;
import org.citrusframework.distribution.scheduler.CapabilityMatcher;
import org.citrusframework.distribution.store.DistributionStore;
import org.citrusframework.distribution.store.StoredRegistrationKey;

/** Pool configuration used by the scheduler. All accesses are serialized by the owning coordinator. */
final class AgentPoolRegistry {
    private final DistributionStore store;
    private final Map<String, AgentPoolRecord> pools = new HashMap<>();

    record IssuedKey(StoredRegistrationKey stored, GeneratedRegistrationKey generated) {
    }

    AgentPoolRegistry(DistributionStore store) {
        this.store = store;
        reload();
    }

    List<AgentPoolRecord> ordered() {
        return pools.values().stream().sorted(Comparator.comparingInt(AgentPoolRecord::priorityOrder)
                .thenComparing(AgentPoolRecord::poolId)).toList();
    }

    AgentPoolRecord get(String id) {
        return pools.get(id);
    }

    AgentPoolRecord newPool(String name, Map<String, String> capabilities, long now) {
        validate(name, capabilities);
        String id;
        do {
            id = RegistrationKeyCrypto.generatePoolId();
        } while (pools.containsKey(id));
        int priority = pools.values().stream().mapToInt(AgentPoolRecord::priorityOrder).max().orElse(-1) + 1;
        return new AgentPoolRecord(id, name.trim(), capabilities, priority, now, now);
    }

    void insert(AgentPoolRecord pool, IssuedKey key) {
        store.insertAgentPool(pool, key == null ? null : key.stored());
        pools.put(pool.poolId(), pool);
    }

    AgentPoolRecord update(AgentPoolRecord previous, String name, Map<String, String> capabilities, long now) {
        validate(name, capabilities);
        var updated = new AgentPoolRecord(previous.poolId(), name.trim(), capabilities, previous.priorityOrder(),
                previous.createdAt(), now);
        store.upsertAgentPool(updated);
        pools.put(updated.poolId(), updated);
        return updated;
    }

    boolean delete(String id) {
        if (!store.deleteAgentPool(id)) {
            return false;
        }
        pools.remove(id);
        return true;
    }

    void reorder(List<String> ids, long now) {
        if (ids == null) {
            throw new IllegalArgumentException("Priority order must include every agent pool");
        }
        store.updateAgentPoolPriorities(ids, now);
        reload();
    }

    private void reload() {
        List<AgentPoolRecord> ordered = store.listAgentPools();
        pools.clear();
        ordered.forEach(p -> pools.put(p.poolId(), p));
    }

    static IssuedKey issueKey(String poolId, String description, long now) {
        if (description != null && description.length() > 512) {
            throw new IllegalArgumentException("Key description must contain at most 512 characters");
        }
        var secret = RegistrationKeyCrypto.generateSecret();
        var metadata = new RegistrationKeyRecord(UUID.randomUUID().toString(), poolId, secret.keyPrefix(), description,
                now, null, null, null);
        return new IssuedKey(new StoredRegistrationKey(metadata, secret.sha256Hex()),
                new GeneratedRegistrationKey(metadata, secret.secretKey()));
    }

    private static void validate(String name, Map<String, String> capabilities) {
        if (name == null || name.isBlank() || name.length() > 255) {
            throw new IllegalArgumentException("Pool display name must contain between 1 and 255 characters");
        }
        CapabilityMatcher.validateCapabilities(capabilities);
    }
}
