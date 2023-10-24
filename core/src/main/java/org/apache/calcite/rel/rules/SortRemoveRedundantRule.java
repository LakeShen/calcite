/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;

import org.immutables.value.Value;

import java.util.Optional;

/**
 * Rule that removes redundant {@code Order By} or {@code Limit}
 * when its input RelNode's threshold is less than or equal to specified row count.
 * All of them are represented by {@link Sort}
 *
 * <p> If a {@code Sort} is order by,and its offset is null,when its input RelNode's
 * threshold is less than or equal to 1,then we could remove the redundant sort.
 *
 * <p> For example:
 * <blockquote><pre>{@code
 *  select max(totalprice) from orders order by 1}
 *  </pre></blockquote>
 *
 * <p> could be converted to
 * <blockquote><pre>{@code
 *  select max(totalprice) from orders}
 *  </pre></blockquote>
 *
 * <p> For example:
 * <blockquote><pre>{@code
 *  SELECT count(*) FROM orders ORDER BY 1 LIMIT 10 }
 *  </pre></blockquote>
 *
 * <p> could be converted to
 * <blockquote><pre>{@code
 *  SELECT count(*) FROM orders}
 *  </pre></blockquote>
 *
 * <p> If a {@code Sort} is pure limit,and its offset is null, when its input
 * RelNode's threshold is less than or equal to the limit's fetch,then we could
 * remove the redundant {@code Limit}.
 *
 * <p> For example:
 * <blockquote><pre>{@code
 * SELECT * FROM (VALUES 1,2,3,4,5,6) AS t1 LIMIT 10}
 * </pre></blockquote>
 *
 * <p> The above values threshold is 6 rows, and the limit's fetch is 10,
 * so we could remove the redundant sort.
 *
 * <p> It could be converted to:
 * <blockquote><pre>{@code
 * SELECT * FROM (VALUES 1,2,3,4,5,6) AS t1}
 * </pre></blockquote>
 *
 * @see CoreRules#SORT_REMOVE_REDUNDANT
 */
@Value.Enclosing
public class SortRemoveRedundantRule
    extends RelRule<SortRemoveRedundantRule.Config>
    implements TransformationRule {
  protected SortRemoveRedundantRule(final SortRemoveRedundantRule.Config config) {
    super(config);
  }

  @Override public void onMatch(final RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    if (RelOptUtil.isOffset(sort)) {
      // Don't remove sort if it has explicit OFFSET
      return;
    }

    // Get the threshold for sort's input RelNode.
    final Double inputMaxRowCount = call.getMetadataQuery().getMaxRowCount(sort.getInput());

    // Get the target threshold with sort's semantics.
    // If sort is 'order by x' or 'order by x limit n', the target threshold is 1.
    // If sort is pure limit, the target threshold is the limit's fetch.
    // If the limit's fetch is 0, we could use CoreRules.SORT_FETCH_ZERO_INSTANCE to deal with it,
    // so we don't need to deal with it in this rule.
    final Optional<Integer> targetMaxRowCount = getSortInputSpecificMaxRowCount(sort);

    if (!targetMaxRowCount.isPresent()) {
      return;
    }

    // If the threshold is not null and less than or equal to targetMaxRowCount,
    // then we could remove the redundant sort.
    if (inputMaxRowCount != null && inputMaxRowCount <= targetMaxRowCount.get()) {
      call.transformTo(sort.getInput());
    }
  }

  private Optional<Integer> getSortInputSpecificMaxRowCount(Sort sort) {
    if (RelOptUtil.isLimit(sort)) {
      final int fetch =
          sort.fetch instanceof RexLiteral ? RexLiteral.intValue(sort.fetch) : 0;

      // We don't need to deal with fetch is 0.
      if (fetch == 0) {
        return Optional.empty();
      }

      // If sort is 'order by x limit n', the target threshold is 1.
      if (RelOptUtil.isOrder(sort)) {
        return Optional.of(1);
      }

      // If sort is 'limit n', the target threshold is the limit's fetch.
      return Optional.of(fetch);
    } else if (RelOptUtil.isPureOrder(sort)) {
      return Optional.of(1);
    }
    return Optional.empty();
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSortRemoveRedundantRule.Config.of()
        .withOperandSupplier(b ->
            b.operand(Sort.class).anyInputs());

    @Override default SortRemoveRedundantRule toRule() {
      return new SortRemoveRedundantRule(this);
    }
  }
}
