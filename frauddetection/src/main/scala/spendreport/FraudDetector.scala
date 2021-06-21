/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

object FraudDetector {
  val SMALL_AMOUNT_THRESHOLD: Double = 1.00
  val LARGE_AMOUNT_THRESHOLD: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @transient private var lastTransactionSmallFlagState: ValueState[java.lang.Boolean] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("lastTransactionSmallFlag", Types.BOOLEAN)
    lastTransactionSmallFlagState = getRuntimeContext.getState(flagDescriptor)
  }

  @throws[Exception]
  def processElement(
      transaction: Transaction,
      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
      collector: Collector[Alert]): Unit = {
    val lastTransactionSmall: java.lang.Boolean = Option(lastTransactionSmallFlagState.value).getOrElse(false)
    if (lastTransactionSmall && transaction.getAmount > FraudDetector.LARGE_AMOUNT_THRESHOLD) {
      val alert = new Alert
      alert.setId(transaction.getAccountId)
      collector.collect(alert)
    }
    lastTransactionSmallFlagState.update(transaction.getAmount < FraudDetector.SMALL_AMOUNT_THRESHOLD)
  }
}
