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
package com.facebook.presto.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestSubqueries
{
    private static final String UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG = "line .*: Given correlated subquery is not supported";

    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testCorrelatedExistsSubqueriesWithOrPredicateAndNull()
    {
        assertions.assertQuery(
                "SELECT EXISTS(SELECT 1 FROM (VALUES null, 10) t(x) WHERE y > x OR y + 10 > x) FROM (values (11)) t2(y)",
                "VALUES true");
        assertions.assertQuery(
                "SELECT EXISTS(SELECT 1 FROM (VALUES null) t(x) WHERE y > x OR y + 10 > x) FROM (values (11)) t2(y)",
                "VALUES false");
    }

    @Test
    public void testUnsupportedSubqueries()
    {
        // coercion from subquery symbol type to correlation type
        assertions.assertFails(
                "select (select count(*) from (values 1) t(a) where t.a=t2.b limit 1) from (values 1.0) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertFails(
                "select EXISTS(select 1 from (values (null, null)) t(a, b) where t.a=t2.b GROUP BY t.b) from (values 1, 2) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Test
    public void testCorrelatedSubqueryWithLimit()
    {
        assertions.assertFails(
                "select (select count(*) from (values 1) t(a) where t.a=t2.b limit 1) from (values 1.0) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertQuery(
                "select (select count(*) from (select t.a from (values 1, 1, null) t(a) limit 1) t where t.a=t2.b) from (values 1, 2) t2(b)",
                "VALUES BIGINT '1', BIGINT '0'");
        assertions.assertFails(
                "select (select count(*) from (values 1, 2, 3, null) t(a) where t.a<t2.b GROUP BY t.a) from (values 1, 2, 3) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertFails(
                "select (select count(*) from (values 1, 1) t(a) where t.a=t2.b limit 1) from (values 1) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertQuery(
                "select EXISTS(select 1 from (values 1, 1) t(a) where t.a=t2.b limit 1) from (values 1, 2) t2(b)",
                "VALUES true, false");
        assertions.assertQuery(
                "select EXISTS(select 1 from (values 1, 1) t(a) where t.a=t2.b GROUP BY t.a) from (values 1, 2) t2(b)",
                "VALUES true, false");
        assertions.assertQuery(
                "select EXISTS(select 1 from (values (1, 2), (1, 2), (null, null)) t(a, b) where t.a=t2.b GROUP BY t.a, t.b) from (values 1, 2) t2(b)",
                "VALUES true, false");
        assertions.assertQuery(
                "select EXISTS(select 1 from (values (1, 2), (1, 2), (null, null)) t(a, b) where t.a<t2.b GROUP BY t.a, t.b) from (values 1, 2) t2(b)",
                "VALUES false, true");
        assertions.assertFails(
                "select EXISTS(select 1 from (values (1, 1), (1, 1), (null, null)) t(a, b) where t.a+t.b<t2.b GROUP BY t.a) from (values 1, 2) t2(b)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertions.assertQuery(
                "select EXISTS(select 1 from (values (1, 1), (1, 1), (null, null)) t(a, b) where t.a+t.b<t2.b GROUP BY t.a, t.b) from (values 1, 4) t2(b)",
                "VALUES false, true");
        assertions.assertQuery(
                "select EXISTS(select 1 from (values (1, 2), (1, 2), (null, null)) t(a, b) where t.a=t2.b GROUP BY t.b) from (values 1, 2) t2(b)",
                "VALUES true, false");
    }
}
