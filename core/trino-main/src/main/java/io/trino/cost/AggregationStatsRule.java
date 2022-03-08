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
package io.trino.cost;

import io.trino.Session;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static io.trino.operator.scalar.MathFunctions.isNaN;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class AggregationStatsRule
        extends SimpleStatsRule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();

    public AggregationStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(AggregationNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        if (node.getGroupingSetCount() != 1) {
            return Optional.empty();
        }

        if (node.getStep() != SINGLE) {
            return Optional.empty();
        }

        return Optional.of(groupBy(
                statsProvider.getStats(node.getSource()),
                node.getGroupingKeys(),
                node.getAggregations()));
    }

    public static PlanNodeStatsEstimate groupBy(PlanNodeStatsEstimate sourceStats, Collection<Symbol> groupBySymbols, Map<Symbol, Aggregation> aggregations)
    {
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        for (Symbol groupBySymbol : groupBySymbols) {
            SymbolStatsEstimate symbolStatistics = sourceStats.getSymbolStatistics(groupBySymbol);
            result.addSymbolStatistics(groupBySymbol, symbolStatistics.mapNullsFraction(nullsFraction -> {
                if (nullsFraction == 0.0) {
                    return 0.0;
                }
                return 1.0 / (symbolStatistics.getDistinctValuesCount() + 1);
            }));
        }

        double rowsCount = min(getRowsCount(sourceStats, groupBySymbols), sourceStats.getOutputRowCount());
        result.setOutputRowCount(rowsCount);

        for (Map.Entry<Symbol, Aggregation> aggregationEntry : aggregations.entrySet()) {
            // Calculate aggregate stats only when the number of grouping symbols is zero or one.
            // Chances of over-estimation are high with multiple grouping columns due to multiplication of NDVs without
            // taking into account the correlation in the data of the columns.
            if (groupBySymbols.size() <= 1) {
                result.addSymbolStatistics(aggregationEntry.getKey(), estimateAggregationStats(aggregationEntry.getValue(), sourceStats, rowsCount));
            }
        }

        return result.build();
    }

    public static double getRowsCount(PlanNodeStatsEstimate sourceStats, Collection<Symbol> groupBySymbols)
    {
        double rowsCount = 1;
        for (Symbol groupBySymbol : groupBySymbols) {
            SymbolStatsEstimate symbolStatistics = sourceStats.getSymbolStatistics(groupBySymbol);
            int nullRow = (symbolStatistics.getNullsFraction() == 0.0) ? 0 : 1;
            rowsCount *= symbolStatistics.getDistinctValuesCount() + nullRow;
        }
        return rowsCount;
    }

    private static SymbolStatsEstimate estimateAggregationStats(Aggregation aggregation, PlanNodeStatsEstimate sourceStats, double aggregatedRowCount)
    {
        requireNonNull(aggregation, "aggregation is null");
        requireNonNull(sourceStats, "sourceStats is null");

        if (aggregation.getArguments().size() != 1
                || !(aggregation.getArguments().get(0) instanceof SymbolReference)
                || aggregation.isDistinct()
                || aggregation.getFilter().isPresent()
                || aggregation.getMask().isPresent()
                || isNaN(aggregatedRowCount)) {
            // Attempt to estimate only simple aggregations
            return SymbolStatsEstimate.unknown();
        }
        if (aggregatedRowCount == 0) {
            return SymbolStatsEstimate.zero();
        }
        Symbol aggregateSymbol = Symbol.from(aggregation.getArguments().get(0));
        SymbolStatsEstimate aggregatedSymbolSourceStats = sourceStats.getSymbolStatistics(aggregateSymbol);
        if (aggregatedSymbolSourceStats.isUnknown()) {
            return SymbolStatsEstimate.unknown();
        }
        SymbolStatsEstimate.Builder builder = SymbolStatsEstimate.buildFrom(aggregatedSymbolSourceStats)
                .setDistinctValuesCount(aggregatedRowCount);
        switch (aggregation.getResolvedFunction().getSignature().getName()) {
            case "min":
                if (aggregatedRowCount == 1) {
                    builder.setLowValue(aggregatedSymbolSourceStats.getLowValue())
                            .setHighValue(aggregatedSymbolSourceStats.getLowValue());
                }
                break;
            case "max":
                if (aggregatedRowCount == 1) {
                    builder.setLowValue(aggregatedSymbolSourceStats.getHighValue())
                            .setHighValue(aggregatedSymbolSourceStats.getHighValue());
                }
                break;
            case "sum":
                if (aggregatedRowCount == 1) {
                    return SymbolStatsEstimate.unknown();
                }
                else {
                    double avgRowsPerGroup = sourceStats.getOutputRowCount() / aggregatedRowCount;
                    // We don't know how skewed the data distribution is over the groupBy symbols.
                    // We take the avgRowsPerGroup and multiply it by 10 to come up with some upper bound for group size.
                    double approxMaxRowsPerGroup = min(10 * avgRowsPerGroup, aggregatedRowCount);
                    double sourceLowValue = aggregatedSymbolSourceStats.getLowValue();
                    double sourceHighValue = aggregatedSymbolSourceStats.getHighValue();
                    // Extrapolate a lower/upper bound for low/high values in the skewed group
                    builder.setLowValue(sourceLowValue < 0 ? sourceLowValue * approxMaxRowsPerGroup : sourceLowValue)
                            .setHighValue(sourceHighValue < 0 ? sourceHighValue : sourceHighValue * approxMaxRowsPerGroup);
                }
                break;
            case "count":
                if (aggregatedRowCount == 1) {
                    builder.setLowValue(sourceStats.getOutputRowCount())
                            .setHighValue(sourceStats.getOutputRowCount());
                }
                else if (sourceStats.getOutputRowCount() == aggregatedRowCount) {
                    builder.setLowValue(1)
                            .setHighValue(1)
                            .setDistinctValuesCount(1);
                }
                else {
                    // Skew in data distribution over the groupBy symbols is unknown
                    return SymbolStatsEstimate.unknown();
                }
                break;
            default:
                // Estimating aggregations other than min, max, count, sum is not supported
                return SymbolStatsEstimate.unknown();
        }
        return builder.build();
    }
}
