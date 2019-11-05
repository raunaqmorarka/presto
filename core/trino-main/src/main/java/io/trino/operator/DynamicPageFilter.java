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
package io.trino.operator;

import io.trino.operator.project.DictionaryAwarePageFilter;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class DynamicPageFilter
        implements PageFilter
{
    private PageFilter currentFilter;
    private Optional<Supplier<PageFilter>> currentPageFilterSupplier;
    private final DynamicPageFilterCollector collector;

    public DynamicPageFilter(Optional<PageFilter> filter, DynamicPageFilterCollector collector)
    {
        this.collector = requireNonNull(collector, "collector is null");
        currentPageFilterSupplier = collector.getFilterFunctionSupplier();
        if (currentPageFilterSupplier.isPresent()) {
            filter = Optional.of(currentPageFilterSupplier.get().get());
        }
        this.currentFilter = requireNonNull(filter, "filter is null")
                .map(pageFilter -> {
                    if (pageFilter.getInputChannels().size() == 1 && pageFilter.isDeterministic()) {
                        return new DictionaryAwarePageFilter(pageFilter);
                    }
                    else {
                        return pageFilter;
                    }
                }).get();
    }

    @Override
    public boolean isDeterministic()
    {
        return currentFilter.isDeterministic();
    }

    @Override
    public InputChannels getInputChannels()
    {
        // DynamicPageFilterCollector can introduce updated PageFilter between getInputChannels and filter calls in PageProcessor
        // To avoid ArrayIndexOutOfBoundsException from PageProcessor.process we update current filter here
        Optional<Supplier<PageFilter>> filterFunctionSupplier = collector.getFilterFunctionSupplier();
        if (!filterFunctionSupplier.equals(currentPageFilterSupplier)) {
            filterFunctionSupplier.ifPresent(this::setUpdatedFilter);
            currentPageFilterSupplier = filterFunctionSupplier;
        }
        return currentFilter.getInputChannels();
    }

    @Override
    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        return currentFilter.filter(session, page);
    }

    private void setUpdatedFilter(Supplier<PageFilter> filterFunctionSupplier)
    {
        PageFilter pageFilter = filterFunctionSupplier.get();
        if (pageFilter.getInputChannels().size() == 1 && pageFilter.isDeterministic()) {
            currentFilter = new DictionaryAwarePageFilter(pageFilter);
        }
        else {
            currentFilter = pageFilter;
        }
    }
}
