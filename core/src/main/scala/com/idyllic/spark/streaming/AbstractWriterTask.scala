package com.saikocat.spark.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

abstract class AbstractWriteTask[T](
  def parameters: Map[String, String]
  def expressionEncoder: ExpressionEncoder[T]

  def setup: () => Unit
  def writeRecordFunc: (T) => Unit

  def execute(iterator: Iterator[InternalRow]): Unit = {
    setup()
    while (iterator.hasNext) {
      val currentRow: InternalRow = iterator.next()
      val record: T = expressionEncoder.fromRow(currentRow)
      writeRecordFunc(pipeline, record)
    }
  }

  def close(): Unit
}
