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

fun <T> Map<String, *>.byType(key: String): T =
    if (this.containsKey(key)) this[key] as T else throw IllegalStateException("$key not defined in map")

fun Map<String, *>.getInt(key: String) = this.byType<Number>(key).toInt()

fun Map<String, *>.getLong(key: String) = this.byType<Number>(key).toLong()

fun Map<String, *>.getString(key: String) = this.byType<String>(key)

fun Map<String, *>.getBoolean(key: String) = this.byType<Boolean>(key)

fun <K, V> Map<String, *>.getMap(key: String) = this.byType<Map<K, V>>(key)

fun Map<String, *>.getHashMap(key: String) = this.byType<HashMap<String, Any?>>(key)

fun <E> Map<String, *>.getList(key: String) = this.byType<List<E>>(key)

fun <E> Map<String, *>.getArrayList(key: String) = this.byType<ArrayList<E>>(key)

fun Map<String, *>.getListOfMap(key: String) = this[key] as List<Map<String, *>>

fun Map<String, *>.getAsDouble(key: String) = this.byType<Double>(key)

fun Map<String, *>.addFieldToNewMap(pair: Pair<String, Any?>): HashMap<String, Any?> {
    val newMap = hashMapOf(pair)
    newMap.putAll(this)
    return newMap
}

inline fun <R, T> List<T>.mapToArrayList(mapFunc: (original: T) -> R): ArrayList<R> {
    val arrayList = ArrayList<R>(this.size)
    this.mapTo(arrayList, mapFunc)
    return arrayList
}

inline fun <R, T> List<T>.mapIndexedToArrayList(mapFunc: (original: T) -> R): ArrayList<R> {
    val arrayList = ArrayList<R>(this.size)
    this.forEach { arrayList.add(mapFunc(it)) }
    return arrayList
}

fun <K, V> Map<String, V>.mapKeyToHashMap(keyMap: (key: String) -> K): HashMap<K, V> {
    val hashMap = hashMapOf<K, V>()
    this.forEach { key, value -> hashMap[keyMap(key)] = value }
    return hashMap
}

fun <Old, New> Map<String, Old>.mapValueToHashMap(valueMap: (value: Old) -> New): HashMap<String, New> {
    val hashMap = hashMapOf<String, New>()
    this.forEach { key, value -> hashMap[key] = valueMap(value) }
    return hashMap
}

fun <T> collectionToArrayList(collection: Collection<T>): ArrayList<T> {
    val arrayList = ArrayList<T>(collection.size)
    arrayList.addAll(collection)
    return arrayList
}

fun String.firstMatch(regex: Regex): String? {
    val match = regex.matchEntire(this)
    return if (match == null) {
        null
    } else {
        match.groupValues.first()
    }
}