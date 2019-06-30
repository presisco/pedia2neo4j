package com.presisco.pedia2neo4j

import com.presisco.gsonhelper.MapHelper
import org.neo4j.driver.v1.Session
import org.neo4j.driver.v1.Values
import org.neo4j.driver.v1.Values.parameters
import java.time.Instant
import java.util.*

fun String.json2Map() = MapHelper().fromJson(this)

fun Set<String>.toLabels() = if (this.isEmpty()) {
    ""
} else {
    this.joinToString(prefix = ":", separator = ":") { "`$it`" }
}

fun Session.mergeEntity(entity: String, labels: Set<String> = setOf()) = this.writeTransaction {
    it.run(
        "merge (new${labels.toLabels()}{name: \$name}) RETURN id(new)",
        Values.parameters("name", entity)
    ).single()
        .get(0)
        .asInt()
}


fun Session.createRelationBetweenIds(from: Int, to: Int, relation: String) {
    this.writeTransaction {
        it.run(
            "match (from), (to)" +
                    " where id(from) = $from and id(to) = $to" +
                    " merge (from) - [rel:`$relation`] -> (to)"
        )
    }
}

fun <T> Map<*, T>.mergeKeySets(): Map<String, T> {
    val merged = hashMapOf<String, T>()
    this.forEach { fuzzyKey, value ->
        when (fuzzyKey) {
            is Collection<*> -> fuzzyKey.forEach { key ->
                merged[key as String] = value
            }
            else -> merged[fuzzyKey.toString()] = value
        }
    }
    return merged
}

fun Session.createRelationFromIdToNameLabel(
    fromId: Int,
    toLabels: Set<String>, toName: String,
    relation: String
) {
    this.writeTransaction {
        it.run(
            "match (from), (to${toLabels.toLabels()})" +
                    " where id(from) = $fromId and to.name =~ '.*name.*'" +
                    " merge (from) - [rel:$relation] -> (to)",
            parameters("name", toName)
        )
    }
}

fun Map<String, String>.toProperties(): Properties {
    val prop = Properties()
    prop.putAll(this)
    return prop
}

fun nowMs() = Instant.now().toEpochMilli()