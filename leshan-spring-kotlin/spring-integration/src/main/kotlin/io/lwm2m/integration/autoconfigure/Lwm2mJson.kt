package io.lwm2m.integration.autoconfigure

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

/** Private wire mapper; does not register or alter the host's ObjectMapper bean. */
class Lwm2mJson(val mapper: ObjectMapper = jacksonObjectMapper().registerModule(JavaTimeModule())
    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS))
