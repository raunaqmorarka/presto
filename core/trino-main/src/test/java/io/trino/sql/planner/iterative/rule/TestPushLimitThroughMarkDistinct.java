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
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestPushLimitThroughMarkDistinct
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new PushLimitThroughMarkDistinct())
                .on(p ->
                        p.limit(
                                1,
                                p.markDistinct(
                                        p.symbol("foo"), ImmutableList.of(p.symbol("bar")), p.values())))
                .matches(
                        node(MarkDistinctNode.class,
                                limit(1, values())));
    }

    @Test
    public void testPushLimitWithTies()
    {
        tester().assertThat(new PushLimitThroughMarkDistinct())
                .on(p ->
                        p.limit(
                                1,
                                ImmutableList.of(p.symbol("foo")),
                                p.markDistinct(
                                        p.symbol("foo"), ImmutableList.of(p.symbol("bar")), p.values())))
                .matches(
                        node(MarkDistinctNode.class,
                                node(LimitNode.class,
                                        node(ValuesNode.class))));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new PushLimitThroughMarkDistinct())
                .on(p ->
                        p.markDistinct(
                                p.symbol("foo"),
                                ImmutableList.of(p.symbol("bar")),
                                p.limit(
                                        1,
                                        p.values())))
                .doesNotFire();

        tester().assertThat(new PushLimitThroughMarkDistinct())
                .on(p -> {
                    List<Symbol> orderBy = ImmutableList.of(p.symbol("foo"));
                    return p.limit(
                            1,
                            Optional.of(new OrderingScheme(orderBy, ImmutableMap.of(p.symbol("foo"), ASC_NULLS_FIRST))),
                            p.markDistinct(
                                    p.symbol("foo"), ImmutableList.of(p.symbol("bar")), p.values()));
                })
                .doesNotFire();
    }

    @Test
    public void testPushdownOrderSensitiveLimit()
    {
        tester().assertThat(new PushLimitThroughMarkDistinct())
                .on(p -> {
                    List<Symbol> orderBy = ImmutableList.of(p.symbol("bar"));
                    return p.limit(
                            2,
                            Optional.of(new OrderingScheme(orderBy, ImmutableMap.of(p.symbol("bar"), ASC_NULLS_FIRST))),
                            p.markDistinct(
                                    p.symbol("foo"), ImmutableList.of(p.symbol("bar")), p.values()));
                })
                .matches(
                        node(
                                MarkDistinctNode.class,
                                limit(2, ImmutableList.of(), false, ImmutableList.of(sort("bar", ASCENDING, FIRST)), values())));
    }
}
