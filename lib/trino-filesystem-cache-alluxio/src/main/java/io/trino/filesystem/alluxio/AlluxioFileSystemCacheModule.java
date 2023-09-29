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
package io.trino.filesystem.alluxio;

import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.cache.TrinoFileSystemCache;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_DIRS;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_ENABLED;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_SIZE;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_TTL_ENABLED;
import static alluxio.conf.PropertyKey.USER_CLIENT_CACHE_TTL_THRESHOLD_SECONDS;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.filesystem.alluxio.AlluxioFileSystemCacheConfig.CACHE_DIRECTORIES;
import static io.trino.filesystem.alluxio.AlluxioFileSystemCacheConfig.CACHE_MAX_PERCENTAGES;
import static io.trino.filesystem.alluxio.AlluxioFileSystemCacheConfig.CACHE_MAX_SIZES;
import static java.lang.String.join;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class AlluxioFileSystemCacheModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(AlluxioFileSystemCacheConfig.class);
        binder.bind(CacheStats.class).to(AlluxioCacheStats.class).in(SINGLETON);
        newExporter(binder).export(CacheStats.class).as(generator -> generator.generatedNameOf(AlluxioCacheStats.class));

        binder.bind(TrinoFileSystemCache.class).to(AlluxioFileSystemCache.class).in(SINGLETON);

        Properties metricProps = new Properties();
        metricProps.put("sink.jmx.class", "alluxio.metrics.sink.JmxSink");
        metricProps.put("sink.jmx.domain", "org.alluxio");
        MetricsSystem.startSinksFromConfig(new MetricsConfig(metricProps));
    }

    @Provides
    @Singleton
    public static AlluxioConfiguration getAlluxioConfiguration(AlluxioFileSystemCacheConfig config)
    {
        checkArgument(config.getMaxCacheSizes().isEmpty() ^ config.getMaxCacheDiskUsagePercentages().isEmpty(),
                "Either %s or %s must be specified", CACHE_MAX_SIZES, CACHE_MAX_PERCENTAGES);
        int size = config.getMaxCacheSizes().isEmpty() ? config.getMaxCacheDiskUsagePercentages().size() : config.getMaxCacheSizes().size();
        checkArgument(config.getCacheDirectories().size() == size,
                "%s and %s must have the same size", CACHE_DIRECTORIES, config.getMaxCacheSizes().isEmpty() ? CACHE_MAX_PERCENTAGES : CACHE_MAX_SIZES);
        List<DataSize> maxCacheSizes = config.getMaxCacheSizes().isEmpty() ?
                calculateMaxCacheSizes(config.getMaxCacheDiskUsagePercentages(), config.getCacheDirectories().stream()
                        .map(directory -> new File(directory).getTotalSpace()).collect(toImmutableList()))
                : config.getMaxCacheSizes();

        AlluxioProperties alluxioProperties = new AlluxioProperties();
        alluxioProperties.set(USER_CLIENT_CACHE_ENABLED, true);
        alluxioProperties.set(USER_CLIENT_CACHE_DIRS, join(",", config.getCacheDirectories()));
        alluxioProperties.set(USER_CLIENT_CACHE_SIZE, join(",", maxCacheSizes.stream().map(DataSize::toBytesValueString).toList()));
        alluxioProperties.set(USER_CLIENT_CACHE_PAGE_SIZE, config.getCachePageSize().toBytesValueString());
        Duration ttl = config.getCacheTTL();
        if (ttl != null) {
            alluxioProperties.set(USER_CLIENT_CACHE_TTL_THRESHOLD_SECONDS, ttl.roundTo(TimeUnit.SECONDS));
            alluxioProperties.set(USER_CLIENT_CACHE_TTL_ENABLED, true);
        }
        return new InstancedConfiguration(alluxioProperties);
    }

    @Provides
    @Singleton
    public static CacheManager getCacheManager(AlluxioConfiguration alluxioConfiguration)
    {
        try {
            return CacheManager.Factory.create(alluxioConfiguration);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<DataSize> calculateMaxCacheSizes(List<Integer> cachePercentages, List<Long> cacheDiskSizes)
    {
        ImmutableList.Builder<DataSize> maxCacheSizes = ImmutableList.builderWithExpectedSize(cacheDiskSizes.size());
        for (int i = 0; i < cacheDiskSizes.size(); i++) {
            maxCacheSizes.add(DataSize.of(Math.round(cachePercentages.get(i) / 100.0 * cacheDiskSizes.get(i)), DataSize.Unit.BYTE));
        }
        return maxCacheSizes.build();
    }
}
