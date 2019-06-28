package com.presisco.pedia2neo4j

import com.github.kittinunf.fuel.httpGet
import com.presisco.gsonhelper.MapHelper
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Values.parameters

const val tripleUrl = "http://shuyantech.com/api/cndbpedia/avpair?q="
const val mentionUrl = "http://shuyantech.com/api/cndbpedia/ment2ent?q="

const val entityLabelRelation = "CATEGORY_ZH"
const val companyLabel = "公司"
const val companyCreatorRelation = "创办人"
const val companyProduceRelation = "开发商"
const val companyPublisherRelation = "发行商"

val neo4jConf = mapOf(
    "uri" to "bolt://10.144.48.95:7687",
    "username" to "neo4j",
    "password" to "experimental"
)
val neo4jDriver = GraphDatabase.driver(
    neo4jConf["uri"],
    AuthTokens.basic(neo4jConf["username"], neo4jConf["password"]),
    Config.defaultConfig()
)!!
val session = neo4jDriver.session()!!
val seedMentions = setOf("搜狗", "搜狗输入法", "搜狐", "搜狗浏览器")
val entityIdMap = hashMapOf<String, Int>()
val entityMentionMap = hashMapOf<String, String>()

fun String.json2Map() = MapHelper().fromJson(this)

fun idFromCache(entity: String, name: String = entity): Int {
    return if (!entityIdMap.containsKey(entity)) {
        val id = session.writeTransaction {
            it.run(
                "merge (new:Entity{name: \$entity, shortName: \$name}) RETURN id(new)",
                parameters("entity", entity, "name", name)
            ).single()
                .get(0)
                .asInt()
        }
        entityIdMap[entity] = id
        id
    } else {
        entityIdMap[entity]!!
    }
}

fun createRelationBetweenIds(from: Int, to: Int, relation: String) {
    session.writeTransaction {
        it.run(
            "match (from), (to)" +
                    " where id(from) = $from and id(to) = $to" +
                    " merge (from) - [rel:$relation] -> (to)"
        )
    }
}

fun Set<String>.toLabels() = if (this.isEmpty()) {
    ""
} else {
    this.joinToString(prefix = ":", separator = ":")
}

fun createRelationsBetweenIdsWithLabel(
    from: Int, fromLabels: Set<String>,
    to: Int, toLabels: Set<String>,
    relation: String
) {
    session.writeTransaction {
        it.run(
            "match (from${fromLabels.toLabels()}), (to${toLabels.toLabels()})" +
                    " where id(from) = $from and id(to) = $to" +
                    " merge (from) - [rel:$relation] -> (to)"
        )
    }
}

fun createMentionNode(entityName: String, mention: String) {
    val entityId = idFromCache(entityName)
    val mentionId = idFromCache(mention)
    createRelationBetweenIds(entityId, mentionId, "简称 ")
}

fun setLabelsForId(id: Int, labels: Set<String>) {
    session.writeTransaction {
        it.run(
            "match (node)" +
                    " where id(node) = $id" +
                    " set node:${labels.joinToString(separator = ":")}"
        )
    }
}

fun getEntitiesForMention(mention: String): List<String> {
    val entities = (mentionUrl + mention)
        .httpGet().responseString().third.component1()!!
        .json2Map()["ret"] as List<String>
    entities.forEach { entityMentionMap[it] = mention }
    return entities
}

fun main() {
    val mapHelper = MapHelper()

    val seedSet = hashSetOf<String>()
    seedMentions.forEach { mention ->
        seedSet.addAll(getEntitiesForMention(mention))
    }

    while (seedSet.isNotEmpty()) {
        val seedWord = seedSet.first()
        val json = (tripleUrl + seedWord).httpGet().responseString().third.component1()
        val triples = mapHelper.fromJson(json!!)["ret"] as List<List<String>>
        val fromId = idFromCache(seedWord, entityMentionMap[seedWord]!!)

        val labels = hashSetOf<String>()

        triples.forEach { triple ->
            println("creating '$seedWord' --'${triple[0]}'--> '${triple[1]}'")
            val toEntity = triple[1]
            val toId = idFromCache(toEntity)
            val relation = triple[0]
            when (relation) {
                entityLabelRelation -> labels.add(toEntity)
                companyCreatorRelation -> seedSet.addAll(getEntitiesForMention(toEntity))
                companyProduceRelation, companyPublisherRelation -> createRelationsBetweenIdsWithLabel(
                    fromId, setOf(companyLabel),
                    toId, setOf(),
                    relation
                )
                else -> createRelationBetweenIds(fromId, toId, relation)
            }
        }

        if (labels.isNotEmpty()) {
            setLabelsForId(fromId, labels)
        }
        seedSet.remove(seedWord)
    }

    println("finished!")
}