package com.presisco.pedia2neo4j.weibo

object MicroBlog {
    val validTimeRegex = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}".toRegex()

    val blogUrlRegex = "//weibo\\.com/(.+?)/([A-Za-z0-9]{9}).*".toRegex()
    val userUrlIdRegex = "//weibo\\.com/([A-Za-z0-9/]*)".toRegex()
    val decimalMidRange = listOf(0..1, 2..8, 9..15)
    val codedMidRanges = listOf(0..0, 1..4, 5..8)
    val customRadixTable = listOf(
        '0', '1', '2', '3', '4', '5', '6',
        '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
        'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w',
        'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
        'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
        'X', 'Y', 'Z'
    )
    val customRadix = customRadixTable.size

    fun String.fromCustomBase(): String {
        var value = 0L
        this.forEach {
            value *= customRadix
            value += customRadixTable.indexOf(it)
        }
        return value.toString(10)
    }

    fun String.toCustomBase(): String {
        var value = this.toLong()

        val bits = arrayListOf<Char>()
        while (value > 0) {
            val bitValue = (value % customRadix).toInt()
            bits.add(0, customRadixTable[bitValue])
            value /= customRadix
        }

        return bits.joinToString(separator = "")
    }

    fun url2codedMid(url: String): String {
        val matchResult = blogUrlRegex.findAll(url).first()
        val codedMid = matchResult.groupValues[2]
        return codedMid
    }

    fun url2mid(url: String): Long {
        val codedMid = url2codedMid(url)
        val midBuilder = StringBuilder()
        codedMidRanges.forEach { midBuilder.append(codedMid.substring(it).fromCustomBase()) }
        return midBuilder.toString().toLong()
    }

    fun uidFromBlogUrl(url: String): String {
        val matchResult = blogUrlRegex.findAll(url).first()
        val uid = matchResult.groupValues[1]
        return uid
    }

    fun encodeMid(value: String): String {
        val codeBuilder = StringBuilder()
        decimalMidRange.forEach { codeBuilder.append(value.substring(it).toCustomBase()) }
        return codeBuilder.toString()
    }

    fun decodeMid(value: String) = value.fromCustomBase()

    fun uidFromUserUrl(url: String): String {
        val matchResult = userUrlIdRegex.findAll(url).first()
        val uid = matchResult.groupValues[1]
        return uid.replace("u/", "")
    }

    fun isValidTime(timeString: String) = validTimeRegex.matches(timeString)

}