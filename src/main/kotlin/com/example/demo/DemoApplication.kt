package com.example.demo

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Predicate
import org.apache.kafka.streams.kstream.TimeWindows
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration
import java.util.*
import java.util.function.Function

@SpringBootApplication
class DemoApplication

fun main(args: Array<String>) {
	runApplication<DemoApplication>(*args)
}

@Configuration
class KafkaStreamsTopologies {

	val isEnglish = Predicate<Any, WordCount> { _, v -> v.word == "english" }
	val isFrench = Predicate<Any, WordCount> { _, v -> v.word == "french" }
	val isSpanish = Predicate<Any, WordCount> { _, v -> v.word == "spanish" }

	@Bean
//    fun process() = Function<KStream<String?, String?>, Array<KStream<String?, WordCount?>>> { input ->
	fun process(): Function<KStream<String?, String?>, Array<KStream<String?, WordCount?>>> = Function { input ->
		input
				.flatMapValues { value -> value?.toLowerCase()?.split("\\W+")?.toList() }
				.groupBy { _, value -> value }
				.windowedBy(TimeWindows.of(Duration.ofSeconds(6)))
				.count(Materialized.`as`("WordCounts-1"))
				.toStream()
				.map { key, value -> KeyValue.pair<String?, WordCount?>(null,
						WordCount(key.key(), value, Date(key.window().start()), Date(key.window().end()))) }
				.branch(isEnglish, isFrench, isSpanish)
	}

}

data class WordCount(var word: String, var count: Long, var start: Date, var end: Date)
