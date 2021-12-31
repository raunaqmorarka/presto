/*
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
package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.airlift.log.Logger;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ConsistentHashingNodeProvider
        implements NodeProvider
{
    private static final Logger LOG = Logger.get(ConsistentHashingNodeProvider.class);

    private final NodeManager nodeManager;
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("FileLocationProvider"));
    private final AtomicReference<List<HostAddress>> workerNodes = new AtomicReference<>(ImmutableList.of());
    private final HashFunction hashFunction = Hashing.goodFastHash(32);

    @Inject
    public ConsistentHashingNodeProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @PostConstruct
    public void start()
    {
        executor.scheduleWithFixedDelay(this::refreshWorkerNodes, 0, 10, SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }

    public Optional<HostAddress> getHost(String identifier)
    {
        List<HostAddress> currentNodes = this.workerNodes.get();
        if (currentNodes.isEmpty()) {
            return Optional.empty();
        }
        int index = Hashing.consistentHash(hashFunction.hashUnencodedChars(identifier), currentNodes.size());
        return Optional.of(currentNodes.get(index));
    }

    private void refreshWorkerNodes()
    {
        try {
            List<HostAddress> workerNodes = nodeManager.getWorkerNodes().stream()
                    .map(Node::getHostAndPort)
                    .sorted(Comparator.comparing(HostAddress::getHostText))
                    .collect(toImmutableList());
            this.workerNodes.set(workerNodes);
        }
        catch (Throwable e) {
            // Catch all exceptions here since throwing an exception from executor#scheduleWithFixedDelay method
            // suppresses all future scheduled invocations
            LOG.error(e, "Error refreshing nodes");
        }
    }
}
