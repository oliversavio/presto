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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Patterns;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;

public class DetermineSemiJoinDistributionType
        implements Rule<SemiJoinNode>
{
    // TODO this rule has not been reviewed at TD

    private boolean isDeleteQuery;

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return Patterns.semiJoin();
    }

    @Override
    public Result apply(SemiJoinNode node, Captures captures, Context context)
    {
        // This rule is BROKEN. See the WAT below.
        return Result.empty();

        /*
        if (node instanceof DeleteNode) {
            // TODO WAT???????????????????????????????????????????????????????????????????????????????????
            isDeleteQuery = true;
            return Optional.empty();
        }
        if (!(node instanceof SemiJoinNode)) {
            return Optional.empty();
        }

        SemiJoinNode semiJoinNode = (SemiJoinNode) node;

        if (semiJoinNode.getDistributionType().isPresent()) {
            return Optional.empty();
        }

        JoinDistributionType joinDistributionType = getJoinDistributionType(context.getSession());

        if (joinDistributionType.canRepartition() && !isDeleteQuery) {
            return Optional.of(semiJoinNode.withDistributionType(PARTITIONED));
        }
        return Optional.of(semiJoinNode.withDistributionType(REPLICATED));
        */
    }
}
