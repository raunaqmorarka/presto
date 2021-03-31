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
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.sort;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.SortItem.NullOrdering.FIRST;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;

public class TestMergeLimits
        extends BaseRuleTest
{
    @Test
    public void testMergeLimitsNoTies()
    {
        tester().assertThat(new MergeLimits())
                .on(p -> p.limit(
                        5,
                        p.limit(
                                3,
                                p.values())))
                .matches(
                        limit(
                                3,
                                values()));
    }

    @Test
    public void testMergeLimitsNoTiesTies()
    {
        tester().assertThat(new MergeLimits())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(
                            3,
                            p.limit(
                                    5,
                                    ImmutableList.of(a),
                                    p.values(a)));
                })
                .matches(
                        limit(
                                3,
                                values("a")));
    }

    @Test
    public void testRearrangeLimitsNoTiesTies()
    {
        tester().assertThat(new MergeLimits())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(
                            5,
                            p.limit(
                                    3,
                                    ImmutableList.of(a),
                                    p.values(a)));
                })
                .matches(
                        limit(
                                3,
                                ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                limit(
                                        5,
                                        values("a"))));
    }

    @Test
    public void testDoNotFireParentWithTies()
    {
        tester().assertThat(new MergeLimits())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    return p.limit(
                            5,
                            ImmutableList.of(a),
                            p.limit(
                                    3,
                                    p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testOrderSensitiveChild()
    {
        tester().assertThat(new MergeLimits())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    List<Symbol> orderBy = ImmutableList.of(a);
                    return p.limit(
                            5,
                            p.limit(
                                    3,
                                    Optional.of(new OrderingScheme(orderBy, ImmutableMap.of(a, ASC_NULLS_FIRST))),
                                    p.values()));
                })
                .matches(
                        limit(
                                3,
                                ImmutableList.of(),
                                false,
                                ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                values()));
    }

    @Test
    public void testOrderSensitiveParent()
    {
        tester().assertThat(new MergeLimits())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    List<Symbol> orderBy = ImmutableList.of(a);
                    return p.limit(
                            3,
                            Optional.of(new OrderingScheme(orderBy, ImmutableMap.of(a, ASC_NULLS_FIRST))),
                            p.limit(
                                    5,
                                    p.values()));
                })
                .matches(
                        limit(
                                3,
                                ImmutableList.of(),
                                false,
                                ImmutableList.of(sort("a", ASCENDING, FIRST)),
                                values()));
    }

    @Test
    public void testOrderSensitiveParentAndChild()
    {
        tester().assertThat(new MergeLimits())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    List<Symbol> orderBy = ImmutableList.of(a);
                    return p.limit(
                            3,
                            Optional.of(new OrderingScheme(orderBy, ImmutableMap.of(a, ASC_NULLS_FIRST))),
                            p.limit(
                                    5,
                                    Optional.of(new OrderingScheme(
                                            ImmutableList.of(a, b),
                                            ImmutableMap.of(a, ASC_NULLS_FIRST, b, ASC_NULLS_FIRST))),
                                    p.values()));
                })
                .matches(
                        limit(
                                3,
                                ImmutableList.of(),
                                false,
                                ImmutableList.of(sort("a", ASCENDING, FIRST), sort("b", ASCENDING, FIRST)),
                                values()));
    }
}
