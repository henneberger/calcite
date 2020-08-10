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
package org.apache.calcite.test.schemata.orderstream;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/** Mock table that returns a stream of orders from a fixed array. */
@SuppressWarnings("UnusedDeclaration")
public class OrdersStreamTableFactory implements TableFactory<Table> {
  // public constructor, per factory contract
  public OrdersStreamTableFactory() {
  }

  public Table create(SchemaPlus schema, String name,
      Map<String, Object> operand, @Nullable RelDataType rowType) {
    return new OrdersTable(getRowList());
  }

  public static ImmutableList<Object[]> getRowList() {
    final Object[][] rows = {
        {ts(10, 15, 0), 1, "paint", 10},
        {ts(10, 24, 15), 2, "paper", 5},
        {ts(10, 24, 45), 3, "brush", 12},
        {ts(10, 58, 0), 4, "paint", 3},
        {ts(11, 10, 0), 5, "paint", 3}
    };
    return ImmutableList.copyOf(rows);
  }

  private static Object ts(int h, int m, int s) {
    return DateTimeUtils.unixTimestamp(2015, 2, 15, h, m, s);
  }
}
