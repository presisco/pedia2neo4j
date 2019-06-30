package com.presisco.pedia2neo4j.cndbpedia

import com.github.kittinunf.fuel.httpGet
import com.presisco.pedia2neo4j.*
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Values

abstract class AbstractSubjectGraph(
    private val maxRequestPerSec: Int = 1
) {
    companion object {
        const val tripleUrl = "http://shuyantech.com/api/cndbpedia/avpair?q="
        const val mentionUrl = "http://shuyantech.com/api/cndbpedia/ment2ent?q="
        const val entityLabelRelation = "CATEGORY_ZH"
    }

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

    val finishedSet = hashSetOf<String>()

    var lastRequestMs = nowMs()

    val safeInterval = 1000L / maxRequestPerSec

    abstract fun targetLabelsForRelation(labels: Set<String>, relation: String): Set<String>

    abstract fun isRecursive(labels: Set<String>, relation: String): Boolean

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
                Values.parameters("name", toName)
            )
        }
    }

    fun <ITEM> CNDBPediaRequest(url: String, keyword: String): List<ITEM> {
        if (nowMs() - lastRequestMs < safeInterval) {
            println("sleep for ${nowMs() - lastRequestMs} ms")
            Thread.sleep(nowMs() - lastRequestMs)
        }
        val result = (url + keyword)
            .httpGet().responseString().third.component1()!!
            .json2Map()
        if (!result.containsKey("ret")) {
            throw IllegalStateException("request for $url$keyword failed! response: $result")
        }
        lastRequestMs = nowMs()
        return result["ret"] as List<ITEM>
    }

    fun getEntitiesForMention(mention: String) = CNDBPediaRequest<String>(mentionUrl, mention).toSet()

    fun getTriplesForEntity(entity: String) = CNDBPediaRequest<List<String>>(tripleUrl, entity)

    fun mergeEntityWithCache(name: String, labels: Set<String>): Int {
        var id = Neo4jIdCache.idFor(name, labels)
        if (id == -1) {
            id = session.mergeEntity(name, labels)
            Neo4jIdCache.addIdFor(name, labels, id)
        }
        return id
    }

    fun startWithEntity(entity: String) {
        val triples = getTriplesForEntity(entity)
        val labels = triples.filter { it[0] == entityLabelRelation }.map { it[1] }.toSet()
        println("labels for $entity: $labels")

        val fromId = mergeEntityWithCache(entity, labels)

        triples.forEach { triple ->
            val relation = triple[0]
            val toMention = triple[1]
            println("creating '$entity' --'$relation'--> '$toMention'")
            if (isRecursive(labels, relation)) {
                startWithMention(toMention)
            }
            val toLabels = targetLabelsForRelation(labels, relation)
            val toId = mergeEntityWithCache(toMention, toLabels)
            session.createRelationBetweenIds(fromId, toId, relation)
        }
    }

    fun startWithMention(mention: String) {
        val seedSet = getEntitiesForMention(mention)

        val queueSet = seedSet.minus(finishedSet)
        queueSet.forEach { seedWord ->
            finishedSet.add(seedWord)
            startWithEntity(seedWord)
        }

    }

    fun clearGraph() {
        ReligionGraph.session.writeTransaction {
            it.run("match (n) detach delete n")
        }
    }

    fun closeGraph() {
        neo4jDriver.close()
    }

}