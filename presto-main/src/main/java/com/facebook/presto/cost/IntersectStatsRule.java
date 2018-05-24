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

package com.facebook.presto.cost;

import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.IntersectNode;

import java.util.Set;
import java.util.stream.Stream;

import static com.facebook.presto.cost.AggregationStatsRule.groupBy;
import static com.facebook.presto.util.MoreMath.min;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.emptyMap;

public class IntersectStatsRule
        extends AbstractSetOperationStatsRule
{
    private static final Pattern<IntersectNode> PATTERN = Pattern.typeOf(IntersectNode.class);

    public IntersectStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<IntersectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected PlanNodeStatsEstimate operate(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second)
    {
        PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.builder();

        Set<Symbol> allSymbols = allSymbols(first);
        for (Symbol symbol : allSymbols) {
            SymbolStatsEstimate leftSymbolStats = first.getSymbolStatistics(symbol);
            SymbolStatsEstimate rightSymbolStats = second.getSymbolStatistics(symbol);
            StatisticRange leftRange = StatisticRange.from(leftSymbolStats);
            StatisticRange rightRange = StatisticRange.from(rightSymbolStats);
            StatisticRange intersection = leftRange.intersect(rightRange);

            statsBuilder.addSymbolStatistics(
                    symbol,
                    SymbolStatsEstimate.builder()
                            .setStatisticsRange(intersection)
                            // It doesn't matter how many nulls are preserved, only whether there are nulls in the intersection or not.
                            // The nullsFraction will be normalized below by groupBy.
                            .setNullsFraction(min(leftSymbolStats.getNullsFraction(), rightSymbolStats.getNullsFraction()))
                            .build());
        }

        statsBuilder.setOutputRowCount(first.getOutputRowCount() + second.getOutputRowCount());  // this is the maximum row count;
        PlanNodeStatsEstimate intermediateResult = statsBuilder.build();
        return groupBy(intermediateResult, allSymbols, emptyMap());
    }

    private static Set<Symbol> allSymbols(PlanNodeStatsEstimate... estimates)
    {
        return Stream.of(estimates)
                .flatMap(estimate -> estimate.getSymbolsWithKnownStatistics().stream())
                .collect(toImmutableSet());
    }
}
