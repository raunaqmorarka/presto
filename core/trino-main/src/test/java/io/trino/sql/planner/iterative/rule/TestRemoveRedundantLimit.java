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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expressions;

public class TestRemoveRedundantLimit
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new RemoveRedundantLimit())
                .on(p ->
                        p.limit(
                                10,
                                p.aggregation(builder -> builder
                                        .addAggregation(p.symbol("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .globalGrouping()
                                        .source(p.values(p.symbol("foo"))))))
                .matches(
                        node(AggregationNode.class,
                                node(ValuesNode.class)));
    }

    @Test
    public void testRemoveLimitWithTies()
    {
        tester().assertThat(new RemoveRedundantLimit())
                .on(p -> {
                    Symbol c = p.symbol("c");
                    return p.limit(
                            10,
                            ImmutableList.of(c),
                            p.values(5, c));
                })
                .matches(values("c"));
    }

    @Test
    public void testForZeroLimit()
    {
        tester().assertThat(new RemoveRedundantLimit())
                .on(p ->
                        p.limit(
                                0,
                                p.filter(
                                        expression("b > 5"),
                                        p.values(
                                                ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                                ImmutableList.of(
                                                        expressions("1", "10"),
                                                        expressions("2", "11"))))))
                // TODO: verify contents
                .matches(values(ImmutableMap.of()));
    }

    @Test
    public void testOrderSensitiveLimit()
    {
        tester().assertThat(new RemoveRedundantLimit())
                .on(p -> {
                    List<Symbol> orderBy = ImmutableList.of(p.symbol("c"));
                    return p.limit(
                            10,
                            ImmutableList.of(),
                            true,
                            Optional.of(
                                    new OrderingScheme(orderBy, ImmutableMap.of(p.symbol("c"), ASC_NULLS_FIRST))),
                            p.aggregation(builder -> builder
                                    .addAggregation(p.symbol("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                    .globalGrouping()
                                    .source(p.values(p.symbol("foo")))));
                })
                .matches(
                        node(AggregationNode.class,
                                node(ValuesNode.class)));

        tester().assertThat(new RemoveRedundantLimit())
                .on(p -> {
                    List<Symbol> orderBy = ImmutableList.of(p.symbol("a"));
                    return p.limit(
                            10,
                            ImmutableList.of(),
                            true,
                            Optional.of(
                                    new OrderingScheme(orderBy, ImmutableMap.of(p.symbol("a"), ASC_NULLS_FIRST))),
                            p.filter(
                                    expression("b > 5"),
                                    p.values(
                                            ImmutableList.of(p.symbol("a"), p.symbol("b")),
                                            ImmutableList.of(
                                                    expressions("1", "10"),
                                                    expressions("2", "11")))));
                })
                // TODO: verify contents
                .matches(
                        node(SortNode.class,
                                node(FilterNode.class,
                                        node(ValuesNode.class))));
    }

    @Test
    public void doesNotFire()
    {
        tester().assertThat(new RemoveRedundantLimit())
                .on(p ->
                        p.limit(
                                10,
                                p.aggregation(builder -> builder
                                        .addAggregation(p.symbol("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .singleGroupingSet(p.symbol("foo"))
                                        .source(p.values(20, p.symbol("foo"))))))
                .doesNotFire();
    }
}
