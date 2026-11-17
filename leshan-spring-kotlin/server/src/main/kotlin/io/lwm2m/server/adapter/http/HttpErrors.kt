package io.lwm2m.server.adapter.http

import com.fasterxml.jackson.core.JsonProcessingException
import io.lwm2m.core.*
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler

@RestControllerAdvice
class HttpErrors : ResponseEntityExceptionHandler() {
    private val log = LoggerFactory.getLogger(javaClass)
    @ExceptionHandler(DeviceException::class)
    fun device(e: DeviceException) = ResponseEntity.status(httpStatus(e.code)).body(CommandError(e.code, e.message ?: e.code.name))

    @ExceptionHandler(IllegalArgumentException::class, JsonProcessingException::class)
    fun invalid(e: Exception) = ResponseEntity.badRequest().body(CommandError(ErrorCode.INVALID_REQUEST,
        if (e is JsonProcessingException) "Invalid JSON request body" else e.message ?: "Invalid request"))

    @ExceptionHandler(Exception::class)
    fun unexpected(e: Exception): ResponseEntity<CommandError> {
        log.error("HTTP request failed", e)
        return ResponseEntity.internalServerError().body(CommandError(ErrorCode.INTERNAL_ERROR, "Operation failed"))
    }
}
