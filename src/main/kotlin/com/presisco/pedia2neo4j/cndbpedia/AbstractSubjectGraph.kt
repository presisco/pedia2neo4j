package com.presisco.pedia2neo4j.cndbpedia

import com.presisco.pedia2neo4j.Neo4jIdCache
import com.presisco.pedia2neo4j.createRelationBetweenIds
import com.presisco.pedia2neo4j.mergeEntity
import com.presisco.pedia2neo4j.toLabels
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Values

abstract class AbstractSubjectGraph {
    companion object {
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

    fun mergeEntityWithCache(name: String, labels: Set<String>): Int {
        var id = Neo4jIdCache.idFor(name, labels)
        if (id == -1) {
            id = session.mergeEntity(name, labels)
            Neo4jIdCache.addIdFor(name, labels, id)
        }
        return id
    }

    fun startWithEntity(entity: String) {
        val triples = APIRequestCache.getAVPair(entity)
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
        val seedSet = APIRequestCache.getMent2Ent(mention)

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