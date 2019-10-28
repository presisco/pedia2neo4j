package com.presisco.pedia2neo4j

import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Config
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Values
import org.slf4j.LoggerFactory

open class Neo4jGraph {
    private val logger = LoggerFactory.getLogger(Neo4jGraph::class.java)

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

    fun createRelationBetweenIds(fromId: Int, toId: Int, relation: String) =
        session.createRelationBetweenIds(fromId, toId, relation)

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

    fun createRelationFromIdToNodes(
        fromId: Int,
        toIds: List<Int>,
        relation: String
    ) {
        session.writeTransaction {
            it.run(
                "match (from), (to)" +
                        " where id(from) = $fromId and id(to) in [${toIds.joinToString(separator = ",")}]" +
                        " merge (from) - [rel:`$relation`] -> (to)"
            )
        }
    }

    fun createNodes(
        names: List<String>,
        labels: Set<String>
    ): List<Int> {
        val ids = arrayListOf<Int>()
        session.writeTransaction { trans ->
            names.forEach {
                ids.add(
                    trans.run(
                        "create (new${labels.toLabels()}{name: \$name}) RETURN id(new) ",
                        Values.parameters("name", it)
                    )
                        .single().get(0).asInt()
                )
            }
        }
        return ids
    }

    fun mergeEntityWithCache(name: String, labels: Set<String>): Int {
        var id = Neo4jIdCache.idFor(name, labels)
        if (id == -1) {
            id = session.mergeEntity(name, labels)
            Neo4jIdCache.addIdFor(name, labels, id)
        }
        return id
    }

    fun clearGraph() {
        logger.info("deleting graph")
        session.writeTransaction {
            it.run("match (n) detach delete n")
        }
        logger.info("graph deleted")
    }

    fun closeGraph() {
        session.close()
        neo4jDriver.close()
    }
}