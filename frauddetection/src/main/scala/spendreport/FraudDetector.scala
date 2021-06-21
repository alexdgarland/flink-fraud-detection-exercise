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
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

object FraudDetector {
  private val SMALL_AMOUNT_THRESHOLD: Double = 1.00
  private val LARGE_AMOUNT_THRESHOLD: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L

  implicit class ExtendedTransaction(t: Transaction) {
    def isSmall: Boolean = t.getAmount < SMALL_AMOUNT_THRESHOLD
    def isLarge: Boolean = t.getAmount > LARGE_AMOUNT_THRESHOLD
    def createAlert: Alert = {
      val alert = new Alert
      alert.setId(t.getAccountId)
      alert
    }
  }

}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  import FraudDetector._

  @transient private var lastTransactionSmallFlagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  private def valueState[V](name: String, clazz: Class[V]) =
    getRuntimeContext.getState(new ValueStateDescriptor[V](name, clazz))

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    lastTransactionSmallFlagState = valueState("lastTransactionSmallFlag", classOf[java.lang.Boolean])
    timerState = valueState("timerState", classOf[java.lang.Long])
  }

  @throws[Exception]
  def processElement(
      transaction: Transaction,
      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
      collector: Collector[Alert]): Unit = {
    if(lastTransactionSmallFlagState.value != null) {
      if (transaction.isLarge) { collector.collect(transaction.createAlert) }
      cleanUp(context)
    }
    if (transaction.isSmall) {
      val timer = context.timerService.currentProcessingTime + ONE_MINUTE
      context.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)
      lastTransactionSmallFlagState.update(true)
    }
  }

  override def onTimer
  (
    timestamp: Long,
    ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
    out: Collector[Alert]
  ): Unit = {
    timerState.clear()
    lastTransactionSmallFlagState.clear()
  }

  private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    ctx.timerService.deleteProcessingTimeTimer(timerState.value)
    timerState.clear()
    lastTransactionSmallFlagState.clear()
  }

}
