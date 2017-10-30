package com.idyllic.spark.streaming

import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.QueryExecution

import com.saikocat.util.TryUtils

object IteratorWriter extends LazyLogging {
  override def toString: String = "IteratorWriter"

  def write[T, W <: AbstractWriteTask[T]](
      parameters: Map[String, String],
      queryExecution: QueryExecution,
      expressionEncoder: ExpressionEncoder[T],
      writerTaskFactory: (Map[String, String], ExpressionEncoder[T]) => W): Unit = {
    queryExecution.toRdd.foreachPartition { iter =>
      val writeTask: W = writerTaskFactory(parameters, expressionEncoder)
      TryUtils.tryWithSafeFinally(block = writeTask.execute(iter))(
        finallyBlock = writeTask.close())
    }
  }
}
