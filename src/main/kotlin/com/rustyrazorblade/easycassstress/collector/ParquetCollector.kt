package com.rustyrazorblade.easycassstress.collector

import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.google.common.base.Throwables
import com.rustyrazorblade.easycassstress.Either
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.schema.MessageTypeParser
import java.io.File

class ParquetCollector(fileOrDirectory: File) : AsyncCollector(fileOrDirectory) {

    override fun createWriter(fileOrDirectory: File): Writer = ParquetTableWriter(if (fileOrDirectory.isDirectory) File(fileOrDirectory, "rawlog.parquet") else fileOrDirectory)

    class ParquetTableWriter(file: File) : Writer {
        private val writer: ParquetWriter<Group>
        private val groupFactory: SimpleGroupFactory

        init {
            file.delete()
            val path = Path("file://${file.absolutePath}")
            println("Parquet Log path $path")
            val schema = MessageTypeParser.parseMessageType(
                "message example {\n" +
                        "  required binary operation (UTF8);\n" +
                        "  required boolean success;\n" +
                        "  required binary failure_reason (UTF8);\n" +
                        "  required binary failure_stacktrace (UTF8);\n" +
                        "  required int64 start_time_ms;\n" +
                        "  required int64 duration_ns;\n" +
                        "}"
            )
            groupFactory = SimpleGroupFactory(schema)
            writer = ExampleParquetWriter.builder(path).withType(schema).build()
        }

        override fun write(event: Event) {
            val group = groupFactory.newGroup()
                .append("operation", op(event))
                .append("success", event.result is Either.Left)
                .append("failure_reason", reasonName(event.result))
                .append("failure_stacktrace", reasonStackTrace(event.result))
                .append("start_time_ms", event.startTimeMs)
                .append("duration_ns", event.durationNs)
            writer.write(group)
        }

        override fun close() {
            writer.close()
        }
    }

    companion object
    {
        private fun op(event: Event) = event.op.javaClass.simpleName.replace("Statement", "")
        private fun reasonName(result: Either<AsyncResultSet, Throwable>) = if (result is Either.Right) result.value.javaClass.simpleName
        else ""

        private fun reasonStackTrace(result: Either<AsyncResultSet, Throwable>) = if (result is Either.Right) Throwables.getStackTraceAsString(result.value)
        else ""
    }
}