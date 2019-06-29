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
        "uri" to "bolt://localhost:7687",
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

val finishedSet = hashSetOf<String>()

fun String.json2Map() = MapHelper().fromJson(this)

fun Set<String>.toLabels() = if (this.isEmpty()) {
    ""
} else {
    this.joinToString(prefix = ":", separator = ":")
}

fun mergeEntity(entity: String, labels: Set<String> = setOf()) = session.writeTransaction {
    it.run(
            "merge (new${labels.toLabels()}{name: \$name}) RETURN id(new)",
            parameters("name", entity)
    ).single()
            .get(0)
            .asInt()
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

fun createRelationFromIdToNameLabel(
        fromId: Int,
        toLabels: Set<String>, toName: String,
        relation: String
) {
    session.writeTransaction {
        it.run(
                "match (from), (to${toLabels.toLabels()})" +
                        " where id(from) = $fromId and to.name =~ '.*name.*'" +
                        " merge (from) - [rel:$relation] -> (to)",
                parameters("name", toName)
        )
    }
}

fun getEntitiesForMention(mention: String): Set<String> {
    val entities = (mentionUrl + mention)
            .httpGet().responseString().third.component1()!!
            .json2Map()["ret"] as List<String>
    println("entities for mention $mention: $entities")
    return entities.toSet()
}

fun mergeEntityWithCache(name: String, labels: Set<String>): Int {
    var id = Neo4jIdCache.idFor(name, labels)
    if (id == -1) {
        id = mergeEntity(name, labels)
        Neo4jIdCache.addIdFor(name, labels, id)
    }
    return id
}

fun buildGraph(mention: String) {
    val seedSet = getEntitiesForMention(mention)
    val mapHelper = MapHelper()

    val queueSet = seedSet.minus(finishedSet)
    queueSet.forEach { seedWord ->
        finishedSet.add(seedWord)
        val json = (tripleUrl + seedWord).httpGet().responseString().third.component1()
        val triples = mapHelper.fromJson(json!!)["ret"] as List<List<String>>
        val labels = triples.filter { it[0] == entityLabelRelation }.map { it[1] }.toSet()
        println("labels for $seedWord: $labels")

        val fromId = mergeEntityWithCache(seedWord, labels)

        triples.forEach { triple ->
            val relation = triple[0]
            val toEntity = triple[1]
            println("creating '$seedWord' --'$relation'--> '$toEntity'")
            val toLabels = when (relation) {
                companyCreatorRelation -> {
                    buildGraph(toEntity)
                    setOf("经济人物")
                }
                companyProduceRelation, companyPublisherRelation -> {
                    buildGraph(toEntity)
                    setOf(companyLabel)
                }
                else -> setOf(relation)
            }
            val toId = mergeEntityWithCache(toEntity, toLabels)
            createRelationBetweenIds(fromId, toId, relation)
        }
    }
}

fun main() {
    session.writeTransaction {
        it.run("match (n) detach delete n")
    }

    seedMentions.forEach { mention ->
        buildGraph(mention)
    }

    println("finished!")
}