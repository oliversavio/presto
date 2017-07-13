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

import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestDetermineSemiJoinDistributionType
{
    /*
    TODO DetermineSemiJoinDistributionType is broken, disabled

    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("join_distribution_type", "automatic")
                .build();
        LocalQueryRunner queryRunner = queryRunnerWithFakeNodeCountForStats(session, 4);
        tester = new RuleTester(queryRunner);
    }

    @Test
    public void testRepartitionsWhenRequiredBySession()
    {
        tester.assertThat(new DetermineSemiJoinDistributionType())
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                p.symbol("A1", BIGINT),
                                p.symbol("B1", BIGINT),
                                p.symbol("A1", BIGINT),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .setSystemProperty("join_distribution_type", "REPARTITIONED")
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "A1",
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0)),
                        Optional.of(PARTITIONED)));
    }

    @Test
    public void testReplicatesWhenRequiredBySession()
    {
        tester.assertThat(new DetermineSemiJoinDistributionType())
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                p.symbol("A1", BIGINT),
                                p.symbol("B1", BIGINT),
                                p.symbol("A1", BIGINT),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .setSystemProperty("join_distribution_type", "REPLICATED")
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "A1",
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0)),
                        Optional.of(REPLICATED)));
    }
    */
}
